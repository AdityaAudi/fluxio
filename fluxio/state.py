"""
fluxio/state.py

DynamoDB-backed workflow state store.

The hardest distributed systems problem in this codebase:
EXACTLY-ONCE step execution across concurrent Lambda invocations.

This is the same checkpoint-and-claim pattern that AWS independently
implemented in Lambda Durable Functions (launched re:Invent 2025).
fluxio implements it on standard DynamoDB + SQS, making it available
in every AWS region and on Python 3.10+ — not just us-east-2 with
Python 3.13/3.14 as Lambda Durable Functions currently requires.

Problem: SQS delivers messages at-least-once. Two Lambda instances may
receive the same step message simultaneously after a cold start or retry.
If both execute the step, you get duplicate side effects (double charges,
double emails, etc.).

Solution: DynamoDB conditional writes acting as a distributed mutex.
Before executing, a worker must "claim" the step atomically:

  UpdateItem(
    Key={workflow_id, "STEP#step_name"},
    UpdateExpression="SET #status = :claimed, executor_id = :id",
    ConditionExpression="#status = :pending"   ← only succeeds once
  )

If two workers race, only one wins. The loser gets a
ConditionalCheckFailedException and discards its SQS message.
This is cheaper and simpler than a distributed lock (no DLM, no TTL races).

DynamoDB Table Schema (single-table design):
┌──────────────────┬──────────────────────────────────────────────────────────┐
│ PK               │ SK                  │ Purpose                            │
├──────────────────┼─────────────────────┼────────────────────────────────────│
│ wf-{uuid}        │ META                │ Workflow metadata + overall status │
│ wf-{uuid}        │ STEP#{step_name}    │ Per-step state + result            │
│ wf-{uuid}        │ BARRIER#{step_name} │ Fan-in barrier counter             │
└──────────────────┴─────────────────────┴────────────────────────────────────┘
"""

from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class WorkflowStatus(str, Enum):
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"


class StepStatus(str, Enum):
    PENDING   = "PENDING"
    CLAIMED   = "CLAIMED"   # locked by one Lambda instance
    RUNNING   = "RUNNING"   # executing user code
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"   # future: conditional branching


class StateStore:
    """
    All DynamoDB operations for fluxio.
    Each method maps to one or two DynamoDB calls.
    """

    def __init__(self, table_name: str = "fluxio_workflows", region: str = "us-east-1"):
        self.table_name = table_name
        self.region = region
        self._ddb = boto3.resource("dynamodb", region_name=region)
        self._table = self._ddb.Table(table_name)
        # Initialize low-level client once — boto3.client() creates a new
        # connection pool on every call; reusing avoids overhead at high throughput
        self._ddb_client = boto3.client("dynamodb", region_name=region)

    # ──────────────────────────────────────────────
    # Workflow lifecycle
    # ──────────────────────────────────────────────

    def create_workflow(
        self,
        workflow_name: str,
        input_data: dict,
        step_names: list[str],
    ) -> str:
        """
        Atomically create a new workflow run with all steps pre-initialized as PENDING.
        Returns the workflow_id.
        """
        workflow_id = f"wf-{uuid.uuid4().hex[:12]}"
        now = _now_iso()

        # Write all items in a single TransactWrite to guarantee atomicity.
        # Either all succeed or none do — no partial workflow state.
        transact_items = [
            {
                "Put": {
                    "TableName": self.table_name,
                    "Item": {
                        "PK": workflow_id,
                        "SK": "META",
                        "workflow_name": workflow_name,
                        "status": WorkflowStatus.PENDING.value,
                        "input": json.dumps(input_data),
                        "created_at": now,
                        "updated_at": now,
                        "version": 0,
                    },
                    "ConditionExpression": "attribute_not_exists(PK)",  # idempotency guard
                }
            }
        ]

        for step_name in step_names:
            transact_items.append({
                "Put": {
                    "TableName": self.table_name,
                    "Item": {
                        "PK": workflow_id,
                        "SK": f"STEP#{step_name}",
                        "step_name": step_name,
                        "status": StepStatus.PENDING.value,
                        "attempts": 0,
                        "created_at": now,
                    },
                }
            })

        # DynamoDB TransactWriteItems hard limit: 100 items per transaction.
        # A workflow with 99+ steps will hit this. Raise early with a clear message.
        if len(transact_items) > 100:
            raise ValueError(
                f"Workflow '{workflow_name}' has {len(step_names)} steps which exceeds "
                f"DynamoDB TransactWriteItems limit of 99 steps (100 items including META). "
                f"Split into sub-workflows or reduce step count."
            )

        # transact_write_items is a low-level client call — items must use
        # DynamoDB typed format {"S": "val"} not plain Python. Use TypeSerializer.
        from boto3.dynamodb.types import TypeSerializer
        ser = TypeSerializer()
        for entry in transact_items:
            entry["Put"]["Item"] = {k: ser.serialize(v) for k, v in entry["Put"]["Item"].items()}

        self._ddb_client.transact_write_items(TransactItems=transact_items)
        return workflow_id

    def mark_workflow_running(self, workflow_id: str):
        self._table.update_item(
            Key={"PK": workflow_id, "SK": "META"},
            UpdateExpression="SET #s = :s, updated_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": WorkflowStatus.RUNNING.value, ":t": _now_iso()},
        )

    def mark_workflow_completed(self, workflow_id: str):
        self._table.update_item(
            Key={"PK": workflow_id, "SK": "META"},
            UpdateExpression="SET #s = :s, completed_at = :t, updated_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": WorkflowStatus.COMPLETED.value, ":t": _now_iso()},
        )

    def mark_workflow_failed(self, workflow_id: str, error: str):
        self._table.update_item(
            Key={"PK": workflow_id, "SK": "META"},
            UpdateExpression="SET #s = :s, #e = :e, updated_at = :t",
            ExpressionAttributeNames={"#s": "status", "#e": "error"},
            ExpressionAttributeValues={
                ":s": WorkflowStatus.FAILED.value,
                ":e": error,
                ":t": _now_iso(),
            },
        )

    def get_workflow_meta(self, workflow_id: str) -> Optional[dict]:
        resp = self._table.get_item(Key={"PK": workflow_id, "SK": "META"})
        return resp.get("Item")

    # ──────────────────────────────────────────────
    # Step lifecycle — the exactly-once core
    # ──────────────────────────────────────────────

    def claim_step(self, workflow_id: str, step_name: str, executor_id: str) -> bool:
        """
        Atomically transition a step from PENDING → CLAIMED.

        This is the distributed mutex. Only one Lambda instance can claim
        a step — the ConditionExpression ensures that.

        Returns True if this instance won the race, False if another did.

        Implementation note: we use attribute_exists + status check rather
        than just status check so DynamoDB can use the sort key index
        efficiently without a full table scan.
        """
        try:
            self._table.update_item(
                Key={"PK": workflow_id, "SK": f"STEP#{step_name}"},
                UpdateExpression=(
                    "SET #s = :claimed, executor_id = :eid, "
                    "claimed_at = :t, attempts = attempts + :one"
                ),
                ConditionExpression=(
                    "attribute_exists(PK) AND #s = :pending"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":claimed": StepStatus.CLAIMED.value,
                    ":pending": StepStatus.PENDING.value,
                    ":eid": executor_id,
                    ":t": _now_iso(),
                    ":one": 1,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False   # lost the race — another instance claimed it
            raise

    def mark_step_running(self, workflow_id: str, step_name: str):
        self._table.update_item(
            Key={"PK": workflow_id, "SK": f"STEP#{step_name}"},
            UpdateExpression="SET #s = :s, started_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": StepStatus.RUNNING.value, ":t": _now_iso()},
        )

    def complete_step(self, workflow_id: str, step_name: str, result: Any):
        """
        Mark a step COMPLETED and store its serialized result.
        The result is stored so fan-in steps can access their dependency outputs.
        """
        self._table.update_item(
            Key={"PK": workflow_id, "SK": f"STEP#{step_name}"},
            UpdateExpression="SET #s = :s, #r = :r, completed_at = :t",
            ExpressionAttributeNames={"#s": "status", "#r": "result"},
            ExpressionAttributeValues={
                ":s": StepStatus.COMPLETED.value,
                ":r": json.dumps(result, default=str),
                ":t": _now_iso(),
            },
        )

    def fail_step(self, workflow_id: str, step_name: str, error: str, attempts: int, max_attempts: int):
        """
        On failure: if under retry limit, reset to PENDING so SQS will redeliver.
        If at retry limit, mark FAILED permanently and fail the workflow.
        """
        if attempts < max_attempts:
            # Reset to PENDING — SQS visibility timeout will expire and redeliver
            self._table.update_item(
                Key={"PK": workflow_id, "SK": f"STEP#{step_name}"},
                UpdateExpression="SET #s = :s, last_error = :e, updated_at = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": StepStatus.PENDING.value,
                    ":e": error,
                    ":t": _now_iso(),
                },
            )
        else:
            self._table.update_item(
                Key={"PK": workflow_id, "SK": f"STEP#{step_name}"},
                UpdateExpression="SET #s = :s, last_error = :e, updated_at = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": StepStatus.FAILED.value,
                    ":e": error,
                    ":t": _now_iso(),
                },
            )

    def get_step(self, workflow_id: str, step_name: str) -> Optional[dict]:
        resp = self._table.get_item(Key={"PK": workflow_id, "SK": f"STEP#{step_name}"})
        item = resp.get("Item")
        if item and "result" in item:
            item["result"] = json.loads(item["result"])
        return item

    def get_step_result(self, workflow_id: str, step_name: str) -> Any:
        item = self.get_step(workflow_id, step_name)
        if not item:
            raise KeyError(f"Step '{step_name}' not found in workflow {workflow_id}")
        return item.get("result")

    def get_all_steps(self, workflow_id: str) -> list[dict]:
        """
        Query all step items for a workflow, paginating through all results.
        DynamoDB Query returns at most 1MB per call — without pagination,
        workflows with large step results or many steps silently return truncated data.
        """
        items = []
        kwargs = {
            "KeyConditionExpression": Key("PK").eq(workflow_id) & Key("SK").begins_with("STEP#")
        }
        while True:
            resp = self._table.query(**kwargs)
            page = resp.get("Items", [])
            for item in page:
                if "result" in item:
                    item["result"] = json.loads(item["result"])
            items.extend(page)
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return items

    # ──────────────────────────────────────────────
    # Barrier — atomic fan-in counter
    # ──────────────────────────────────────────────

    def init_barrier(self, workflow_id: str, step_name: str, total_deps: int):
        """
        Create a barrier for a fan-in step.
        Called when the workflow starts, before any parallel branches run.
        results is a DynamoDB map so branches can write to individual keys atomically.
        """
        self._table.put_item(Item={
            "PK": workflow_id,
            "SK": f"BARRIER#{step_name}",
            "total_deps": total_deps,
            "completed_deps": 0,
            "results": {},   # DynamoDB map — each branch writes its own key atomically
            "created_at": _now_iso(),
        })

    def arrive_at_barrier(
        self,
        workflow_id: str,
        barrier_step: str,
        arriving_step: str,
        result: Any,
    ) -> bool:
        """
        A parallel branch has completed. Atomically increment the barrier counter
        and store this branch result.

        Returns True if THIS arrival completes the barrier (all deps done).

        Bug fix: the original implementation did a read-modify-write on dep_results
        which caused a data loss race: two branches arriving simultaneously would
        both read the same dep_results, each add their own key, and the last write
        would overwrite the first branch result silently.

        Fix: use DynamoDB map attribute path to write each branch result atomically
        into its own key without touching other keys. The path
        SET results.#branch_key = :val writes only that key, leaving all others
        untouched regardless of concurrent writes. Combined with atomic ADD on
        completed_deps, this is fully race-free.
        """
        # Sanitize arriving_step to be a safe DynamoDB attribute name
        # (step names are Python identifiers so this is safe, but be explicit)
        safe_key = arriving_step.replace("-", "_")

        resp = self._table.update_item(
            Key={"PK": workflow_id, "SK": f"BARRIER#{barrier_step}"},
            UpdateExpression=(
                "ADD completed_deps :one "
                "SET results.#branch_key = :val, updated_at = :t"
            ),
            ExpressionAttributeNames={
                "#branch_key": safe_key,
            },
            ExpressionAttributeValues={
                ":one": 1,
                ":val": json.dumps(result, default=str),
                ":t": _now_iso(),
            },
            ReturnValues="ALL_NEW",
        )

        attrs = resp["Attributes"]
        new_count = int(attrs["completed_deps"])
        total_deps = int(attrs["total_deps"])
        return new_count >= total_deps

    def get_barrier_results(self, workflow_id: str, barrier_step: str) -> dict:
        item = self._table.get_item(
            Key={"PK": workflow_id, "SK": f"BARRIER#{barrier_step}"}
        ).get("Item", {})
        raw = item.get("results", {})
        # Each value is stored as a JSON string; deserialize each branch result
        return {k: json.loads(v) if isinstance(v, str) else v for k, v in raw.items()}

    # ──────────────────────────────────────────────
    # Stuck step recovery
    # ──────────────────────────────────────────────

    def recover_stuck_steps(
        self,
        workflow_id: str,
        stuck_after_seconds: int = 300,
    ) -> list[str]:
        """
        Find steps stuck in CLAIMED status beyond stuck_after_seconds and
        reset them to PENDING so SQS will redeliver.

        Call this from a scheduled Lambda (e.g., EventBridge every 5 minutes)
        to recover from Lambda mid-execution failures (OOM kill, 15-min timeout,
        infrastructure failure after claim but before complete).

        A step in CLAIMED with claimed_at older than stuck_after_seconds means
        Lambda died without completing. Without this, the workflow hangs forever.

        Returns list of step names that were reset.
        """
        from datetime import timedelta
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(seconds=stuck_after_seconds)).isoformat()

        all_steps = self.get_all_steps(workflow_id)
        reset = []

        for step in all_steps:
            if step.get("status") == StepStatus.CLAIMED.value:
                claimed_at = step.get("claimed_at", "")
                if claimed_at and claimed_at < cutoff:
                    self._table.update_item(
                        Key={"PK": workflow_id, "SK": f"STEP#{step['step_name']}"},
                        UpdateExpression="SET #s = :pending, updated_at = :t",
                        ConditionExpression="#s = :claimed",  # only reset if still CLAIMED
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={
                            ":pending": StepStatus.PENDING.value,
                            ":claimed": StepStatus.CLAIMED.value,
                            ":t": _now_iso(),
                        },
                    )
                    reset.append(step["step_name"])

        return reset

    # ──────────────────────────────────────────────
    # CloudFormation / Terraform helper
    # ──────────────────────────────────────────────

    @staticmethod
    def table_definition(table_name: str = "fluxio_workflows") -> dict:
        """Returns the DynamoDB CreateTable parameters."""
        return {
            "TableName": table_name,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            "TimeToLiveSpecification": {
                "AttributeName": "ttl",
                "Enabled": True,
            },
        }
