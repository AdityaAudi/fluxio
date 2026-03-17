"""
fluxio/engine.py

The execution engine. Orchestrates step dispatch based on completed deps,
handles fan-in barrier coordination, and drives the workflow to completion.

Two entry points:
  1. start_workflow()  — called by your application to kick off a workflow
  2. execute_step()    — called by the Lambda worker for each SQS message
"""

from __future__ import annotations
import os
import traceback
import uuid
from typing import Any, Optional

from fluxio.workflow import WorkflowDefinition, StepDefinition, get_workflow
from fluxio.state import StateStore, StepStatus, WorkflowStatus
from fluxio.dispatcher import Dispatcher


class FluxioEngine:

    def __init__(
        self,
        table_name: str = "fluxio_workflows",
        queue_url: str = None,
        region: str = "us-east-1",
    ):
        self.state   = StateStore(table_name=table_name, region=region)
        self.dispatcher = Dispatcher(
            queue_url=queue_url or os.environ["FLUXIO_QUEUE_URL"],
            region=region,
        )
        self.region = region

    # ──────────────────────────────────────────────
    # 1. Start a workflow
    # ──────────────────────────────────────────────

    def start_workflow(self, workflow_name: str, input_data: dict) -> str:
        """
        Initialize workflow state in DynamoDB, then dispatch entry steps to SQS.

        Returns workflow_id (a fresh uuid each call).
        Note: not idempotent — calling twice creates two separate workflow runs.
        If you need idempotency, check for an existing workflow_id in your
        application layer before calling this.
        """
        wf_def = get_workflow(workflow_name)

        # Pre-initialize barriers for all fan-in steps
        fan_in_steps = {
            name: step
            for name, step in wf_def.steps.items()
            if len(step.depends_on) > 1
        }

        # Create workflow record + all step records atomically
        workflow_id = self.state.create_workflow(
            workflow_name=workflow_name,
            input_data=input_data,
            step_names=list(wf_def.steps.keys()),
        )

        # Initialize barriers for fan-in steps
        for step_name, step_def in fan_in_steps.items():
            self.state.init_barrier(
                workflow_id=workflow_id,
                step_name=step_name,
                total_deps=len(step_def.depends_on),
            )

        self.state.mark_workflow_running(workflow_id)

        # Dispatch entry steps (no dependencies) to SQS in a single batch call
        entry_messages = [
            {
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "step_name": step_name,
                "input_data": input_data,
                "visibility_timeout": wf_def.steps[step_name].timeout_seconds,
            }
            for step_name in wf_def.entry_steps
        ]
        if len(entry_messages) == 1:
            self.dispatcher.dispatch_step(**entry_messages[0])
        else:
            self.dispatcher.dispatch_batch(entry_messages)

        return workflow_id

    # ──────────────────────────────────────────────
    # 2. Execute a single step (called by Lambda worker)
    # ──────────────────────────────────────────────

    def execute_step(
        self,
        workflow_id: str,
        workflow_name: str,
        step_name: str,
        input_data: dict,
    ) -> None:
        """
        Core execution logic. Called once per SQS message delivery.

        Flow:
          1. Claim the step (exactly-once mutex)
          2. Resolve input args from completed dependency results
          3. Execute user code
          4. Store result, dispatch next ready steps
          5. On failure: retry or fail workflow
        """
        wf_def = get_workflow(workflow_name)
        step_def = wf_def.steps[step_name]
        executor_id = f"lambda-{uuid.uuid4().hex[:8]}"

        # ── Step 1: Claim (distributed mutex) ──────────────
        claimed = self.state.claim_step(workflow_id, step_name, executor_id)
        if not claimed:
            # Another Lambda instance already claimed this step.
            # Discard this SQS delivery — nothing to do.
            print(f"[fluxio] Step {step_name} already claimed — discarding duplicate delivery")
            return

        # ── Step 2: Resolve inputs ──────────────────────────
        self.state.mark_step_running(workflow_id, step_name)

        try:
            step_input = self._resolve_inputs(workflow_id, step_def, input_data)
        except Exception as exc:
            self.state.fail_step(workflow_id, step_name, str(exc), 1, step_def.retry_max)
            raise

        # ── Step 3: Execute user code ───────────────────────
        try:
            instance = wf_def.cls()
            method = getattr(instance, step_name)
            result = method(**step_input)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            step_item = self.state.get_step(workflow_id, step_name)
            attempts = int(step_item.get("attempts", 1)) if step_item else 1

            self.state.fail_step(
                workflow_id, step_name, error_msg,
                attempts=attempts,
                max_attempts=step_def.retry_max,
            )

            if attempts >= step_def.retry_max:
                self.state.mark_workflow_failed(workflow_id, error_msg)
                raise RuntimeError(
                    f"Workflow {workflow_id} FAILED: step '{step_name}' "
                    f"exhausted {step_def.retry_max} attempts"
                ) from exc
            raise   # let SQS redeliver after visibility timeout

        # ── Step 4: Store result + dispatch next steps ──────
        self.state.complete_step(workflow_id, step_name, result)
        self._advance_workflow(workflow_id, wf_def, step_name, result, input_data)

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    def _resolve_inputs(
        self,
        workflow_id: str,
        step_def: StepDefinition,
        initial_input: dict,
    ) -> dict:
        """
        Build the kwargs dict for a step's function call.

        - Entry steps (no deps) receive the original workflow input
        - Steps with one dependency receive that dep's result directly
        - Fan-in steps receive each dep's result as a named kwarg
        """
        if not step_def.depends_on:
            return initial_input

        if len(step_def.depends_on) == 1:
            dep_name = step_def.depends_on[0]
            result = self.state.get_step_result(workflow_id, dep_name)
            # If the result is a dict, spread it as kwargs; else wrap it
            if isinstance(result, dict):
                return result
            return {"result": result}

        # Fan-in: retrieve from barrier
        barrier_results = self.state.get_barrier_results(workflow_id, step_def.name)
        kwargs = {}
        for dep_name in step_def.depends_on:
            r = barrier_results.get(dep_name)
            if isinstance(r, dict):
                kwargs.update(r)
            else:
                kwargs[dep_name] = r
        return kwargs

    def _advance_workflow(
        self,
        workflow_id: str,
        wf_def: WorkflowDefinition,
        completed_step: str,
        result: Any,
        initial_input: dict,
    ):
        """
        After a step completes:
          - For fan-in dependencies: arrive at barrier; dispatch gated step only
            when ALL deps have arrived
          - For regular downstream steps: dispatch immediately if all deps complete
          - If no more steps pending: mark workflow COMPLETED
        """
        # Find all steps that directly depend on the just-completed step
        downstream = [
            name for name, step in wf_def.steps.items()
            if completed_step in step.depends_on
        ]

        dispatched_any = False

        for next_step in downstream:
            next_def = wf_def.steps[next_step]

            if len(next_def.depends_on) == 1:
                # Simple dependency — dispatch immediately
                self.dispatcher.dispatch_step(
                    workflow_id=workflow_id,
                    workflow_name=wf_def.name,
                    step_name=next_step,
                    input_data=initial_input,
                    visibility_timeout=next_def.timeout_seconds,
                )
                dispatched_any = True

            else:
                # Fan-in — arrive at barrier
                all_arrived = self.state.arrive_at_barrier(
                    workflow_id=workflow_id,
                    barrier_step=next_step,
                    arriving_step=completed_step,
                    result=result,
                )
                if all_arrived:
                    # We are the last branch — dispatch the fan-in step
                    self.dispatcher.dispatch_step(
                        workflow_id=workflow_id,
                        workflow_name=wf_def.name,
                        step_name=next_step,
                        input_data=initial_input,
                        visibility_timeout=next_def.timeout_seconds,
                    )
                    dispatched_any = True

        # Check if the entire workflow is complete
        if not dispatched_any and not downstream:
            all_steps = self.state.get_all_steps(workflow_id)
            all_done = all(
                s["status"] in (StepStatus.COMPLETED.value, StepStatus.SKIPPED.value)
                for s in all_steps
            )
            if all_done:
                self.state.mark_workflow_completed(workflow_id)
                print(f"[fluxio] Workflow {workflow_id} COMPLETED")
