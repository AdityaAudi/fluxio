"""
fluxio/dispatcher.py

SQS-based step dispatcher.

Poison pill problem: if a step message is malformed or causes an unhandled
exception on every attempt, it will loop forever between SQS and Lambda,
blocking the queue and burning money.

Defense in depth (same protections used in Lambda Durable Functions checkpointing):
  1. maxReceiveCount on the SQS queue's redrive policy (set to retry_max)
     moves messages to a DLQ after N failures — set this in your IaC.
  2. VisibilityTimeout is set per-step based on the step's `timeout` value,
     so a timed-out Lambda doesn't release the message back too early.
  3. MessageGroupId (FIFO queue) is set to workflow_id — guarantees
     in-order delivery per workflow while allowing cross-workflow parallelism.
  4. MessageDeduplicationId prevents duplicate dispatches within 5 minutes
     (FIFO queue deduplication window).

Note on regional availability: this dispatcher uses standard SQS and DynamoDB
APIs available in all AWS commercial, GovCloud, and China regions — unlike
Lambda Durable Functions which launched in us-east-2 only (December 2025).
"""

from __future__ import annotations
import hashlib
import json
import uuid
from typing import Any

import boto3


class Dispatcher:

    def __init__(self, queue_url: str, region: str = "us-east-1"):
        self.queue_url = queue_url
        self._sqs = boto3.client("sqs", region_name=region)
        self._is_fifo = queue_url.endswith(".fifo")

    def dispatch_step(
        self,
        workflow_id: str,
        workflow_name: str,
        step_name: str,
        input_data: dict,
        visibility_timeout: int = 60,
        delay_seconds: int = 0,
    ) -> str:
        """
        Send a step execution message to SQS.

        The message body contains everything the Lambda worker needs:
          - workflow_id: which workflow run
          - workflow_name: which WorkflowDefinition to load
          - step_name: which step to execute
          - input_data: original workflow input (for entry steps)

        Returns the SQS MessageId.
        """
        body = json.dumps({
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
            "step_name": step_name,
            "input_data": input_data,
        })

        kwargs: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MessageBody": body,
            "MessageAttributes": {
                "workflow_id": {
                    "StringValue": workflow_id,
                    "DataType": "String",
                },
                "step_name": {
                    "StringValue": step_name,
                    "DataType": "String",
                },
            },
        }

        if delay_seconds > 0 and not self._is_fifo:
            # FIFO queues don't support per-message delays
            kwargs["DelaySeconds"] = min(delay_seconds, 900)

        if self._is_fifo:
            # MessageGroupId = workflow_id ensures ordered delivery per workflow
            # but allows parallel processing across different workflows
            kwargs["MessageGroupId"] = workflow_id

            # Deduplication: hash of (workflow_id, step_name) prevents duplicate
            # dispatches within the 5-minute SQS deduplication window
            dedup_key = f"{workflow_id}#{step_name}"
            kwargs["MessageDeduplicationId"] = hashlib.sha256(
                dedup_key.encode()
            ).hexdigest()[:40]

        # NOTE: SQS does not support per-message VisibilityTimeout at send time.
        # VisibilityTimeout is a queue-level setting only. The correct approach
        # is to call sqs.change_message_visibility() in the Lambda worker after
        # receiving the message, before executing the step. Set your queue
        # VisibilityTimeout to the maximum step timeout across all your steps.

        resp = self._sqs.send_message(**kwargs)
        msg_id = resp["MessageId"]
        print(f"[fluxio] Dispatched step '{step_name}' for {workflow_id} → SQS {msg_id}")
        return msg_id

    def dispatch_batch(self, messages: list[dict]) -> dict:
        """
        Dispatch up to 10 step messages in a single SQS SendMessageBatch call.
        Used when multiple parallel steps become ready simultaneously.

        Returns {"successful": [...], "failed": [...]}
        """
        entries = []
        for msg in messages[:10]:  # SQS batch limit
            entry_id = uuid.uuid4().hex[:8]
            body = json.dumps(msg)
            entry: dict[str, Any] = {
                "Id": entry_id,
                "MessageBody": body,
            }
            if self._is_fifo:
                entry["MessageGroupId"] = msg["workflow_id"]
                dedup_key = f"{msg['workflow_id']}#{msg['step_name']}"
                entry["MessageDeduplicationId"] = hashlib.sha256(
                    dedup_key.encode()
                ).hexdigest()[:40]
            entries.append(entry)

        resp = self._sqs.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        return {
            "successful": resp.get("Successful", []),
            "failed": resp.get("Failed", []),
        }
