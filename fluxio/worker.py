"""
fluxio/worker.py

Lambda handler that processes fluxio step messages from SQS.

Deploy this as your Lambda function with an SQS event source mapping.
Set batch_size=1 for simplest exactly-once guarantees.

Why fluxio over Lambda Durable Functions (re:Invent 2025)?
  - Lambda Durable Functions is currently limited to us-east-2 only.
    fluxio works in ap-southeast-1, eu-west-1, us-gov-west-1 — any region
    where DynamoDB and SQS are available.
  - Lambda Durable Functions requires Python 3.13 or 3.14.
    fluxio supports Python 3.10, 3.11, 3.12, and 3.13.
  - fluxio state lives in your own DynamoDB table — fully inspectable,
    queryable, and auditable with standard AWS tooling.

Important SQS settings:
  - VisibilityTimeout: set >= max step timeout
  - RedrivePolicy.maxReceiveCount: set to your max retry_max across all steps
  - RedrivePolicy.deadLetterTargetArn: point to a DLQ for inspection

Environment variables required:
  FLUXIO_TABLE_NAME   DynamoDB table name (default: fluxio_workflows)
  FLUXIO_QUEUE_URL    SQS queue URL
  AWS_REGION          (set automatically by Lambda runtime)
"""

from __future__ import annotations
import json
import os
import boto3

from fluxio.engine import FluxioEngine


def build_engine() -> FluxioEngine:
    return FluxioEngine(
        table_name=os.environ.get("FLUXIO_TABLE_NAME", "fluxio_workflows"),
        queue_url=os.environ["FLUXIO_QUEUE_URL"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
    )


# Module-level engine — reused across warm Lambda invocations (no re-init cost)
_engine: FluxioEngine | None = None


def lambda_handler(event: dict, context) -> dict:
    """
    SQS event source mapping handler.

    With batch_size=1, each invocation processes exactly one step message.
    With batch_size>1, steps are processed sequentially within the batch
    (Lambda SQS partial batch failure is supported — failed items are retried).

    Returns: {"batchItemFailures": [...]} for partial batch failure reporting.
    """
    global _engine
    if _engine is None:
        _engine = build_engine()

    failures = []

    sqs_client = boto3.client("sqs")
    queue_url = os.environ["FLUXIO_QUEUE_URL"]

    for record in event.get("Records", []):
        message_id = record["messageId"]
        receipt_handle = record["receiptHandle"]
        try:
            body = json.loads(record["body"])
            wf_def_name = body["workflow_name"]
            step_name = body["step_name"]

            # Set per-step visibility timeout BEFORE executing.
            # This is the correct place — SQS does not allow VisibilityTimeout
            # at send time, only after receipt via change_message_visibility.
            from fluxio.workflow import get_workflow
            try:
                wf_def = get_workflow(wf_def_name)
                step_timeout = wf_def.steps[step_name].timeout_seconds
                # Add 30s buffer so the message doesn't reappear right as the step finishes
                visibility = min(step_timeout + 30, 43200)
                sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=visibility,
                )
            except Exception:
                pass  # best-effort; don't block execution if this fails

            _engine.execute_step(
                workflow_id=body["workflow_id"],
                workflow_name=wf_def_name,
                step_name=step_name,
                input_data=body.get("input_data", {}),
            )
        except Exception as exc:
            print(f"[fluxio] Step execution failed for message {message_id}: {exc}")
            # Report this message as failed — SQS will redeliver it
            # (up to maxReceiveCount, then routes to DLQ)
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
