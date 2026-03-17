"""
fluxio/workflow.py

User-facing API: @workflow and @step decorators.

Usage:
    @workflow
    class OrderFulfillment:

        @step(retry=3, timeout=30)
        def validate_payment(self, order_id: str) -> dict:
            ...

        @step(depends_on=["validate_payment"], retry=2)
        def reserve_inventory(self, payment: dict) -> dict:
            ...

        @step(depends_on=["validate_payment"], retry=1)
        def send_confirmation(self, payment: dict) -> dict:
            ...

        # Fan-in: only runs when ALL listed deps complete
        @step(depends_on=["reserve_inventory", "send_confirmation"])
        def complete_order(self, inventory: dict, confirmation: dict) -> dict:
            ...
"""

from __future__ import annotations
import functools
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ──────────────────────────────────────────────
# Step metadata (attached to each decorated method)
# ──────────────────────────────────────────────

@dataclass
class StepDefinition:
    name: str
    fn: Callable
    depends_on: list[str]        # step names this step waits for
    retry_max: int                # max attempts before marking FAILED
    timeout_seconds: int          # per-attempt timeout hint (for SQS visibility)
    is_parallel: bool             # hint: can run concurrently with siblings


@dataclass
class WorkflowDefinition:
    name: str
    cls: type
    steps: dict[str, StepDefinition]    # step_name -> StepDefinition
    entry_steps: list[str]              # steps with no dependencies (start here)

    def execution_order(self) -> list[list[str]]:
        """
        Returns steps grouped by wave: each wave can run in parallel.
        Uses Kahn's algorithm (BFS topological sort).

        Example for OrderFulfillment:
          Wave 0: [validate_payment]
          Wave 1: [reserve_inventory, send_confirmation]   ← parallel
          Wave 2: [complete_order]
        """
        in_degree: dict[str, int] = {name: 0 for name in self.steps}
        dependents: dict[str, list[str]] = {name: [] for name in self.steps}

        for name, step in self.steps.items():
            in_degree[name] = len(step.depends_on)
            for dep in step.depends_on:
                if dep not in dependents:
                    raise ValueError(
                        f"Step '{name}' depends on '{dep}' which does not exist in workflow '{self.name}'"
                    )
                dependents[dep].append(name)

        waves: list[list[str]] = []
        ready = [n for n, d in in_degree.items() if d == 0]

        while ready:
            waves.append(sorted(ready))  # sorted for determinism
            next_ready = []
            for n in ready:
                for dependent in dependents[n]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_ready.append(dependent)
            ready = next_ready

        if sum(len(w) for w in waves) != len(self.steps):
            raise ValueError(f"Workflow '{self.name}' has a cycle — check depends_on chains")

        return waves

    def ready_steps(self, completed: set[str]) -> list[str]:
        """
        Given a set of completed step names, return all steps whose
        dependencies are fully satisfied and that haven't started yet.
        Used by the engine after each step completes.
        """
        return [
            name for name, step in self.steps.items()
            if name not in completed
            and all(dep in completed for dep in step.depends_on)
        ]


# ──────────────────────────────────────────────
# Registry (in-process lookup, keyed by workflow class name)
# ──────────────────────────────────────────────

_REGISTRY: dict[str, WorkflowDefinition] = {}


def get_workflow(name: str) -> WorkflowDefinition:
    if name not in _REGISTRY:
        raise KeyError(f"No workflow registered with name '{name}'. "
                       f"Known workflows: {list(_REGISTRY.keys())}")
    return _REGISTRY[name]


def list_workflows() -> list[str]:
    return list(_REGISTRY.keys())


# ──────────────────────────────────────────────
# Decorators
# ──────────────────────────────────────────────

def step(
    depends_on: Optional[list[str]] = None,
    retry: int = 3,
    timeout: int = 60,
    parallel: bool = False,
) -> Callable:
    """
    Mark a workflow method as a step.

    Args:
        depends_on: Step names that must complete before this step runs.
                    Multiple deps = fan-in (all must complete).
        retry:      Max execution attempts before marking step as FAILED.
        timeout:    Per-attempt timeout in seconds. Used to set SQS
                    VisibilityTimeout so the message reappears if Lambda dies.
        parallel:   Hint that this step is safe to run concurrently with
                    sibling steps (same wave). Default True when depends_on
                    is populated.
    """
    def decorator(fn: Callable) -> Callable:
        fn._fluxio_step = StepDefinition(
            name=fn.__name__,
            fn=fn,
            depends_on=depends_on or [],
            retry_max=retry,
            timeout_seconds=timeout,
            is_parallel=parallel or bool(depends_on),
        )
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapper._fluxio_step = fn._fluxio_step
        return wrapper
    return decorator


def workflow(cls: type) -> type:
    """
    Register a class as a fluxio workflow.
    Scans all methods for @step decorators and builds a WorkflowDefinition.
    """
    steps: dict[str, StepDefinition] = {}

    for attr_name in dir(cls):
        method = getattr(cls, attr_name, None)
        step_def = getattr(method, "_fluxio_step", None)
        if step_def is not None:
            step_def.name = attr_name   # ensure name matches method name
            steps[attr_name] = step_def

    if not steps:
        raise ValueError(f"@workflow class '{cls.__name__}' has no @step methods")

    entry_steps = [name for name, s in steps.items() if not s.depends_on]
    if not entry_steps:
        raise ValueError(
            f"Workflow '{cls.__name__}' has no entry steps "
            f"(steps with no depends_on). Check for a dependency cycle."
        )

    wf_def = WorkflowDefinition(
        name=cls.__name__,
        cls=cls,
        steps=steps,
        entry_steps=entry_steps,
    )

    # Validate the graph (raises on cycles or unknown deps)
    wf_def.execution_order()

    _REGISTRY[cls.__name__] = wf_def
    cls._fluxio_definition = wf_def
    return cls
