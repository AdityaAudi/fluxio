# fluxio

**Durable, exactly-once serverless workflow orchestration for AWS Lambda — available in every AWS region, on any Python version.**

No servers. No Step Functions costs. Just DynamoDB + SQS + Lambda — and hard distributed systems guarantees.

[![PyPI](https://img.shields.io/pypi/v/fluxio)](https://pypi.org/project/fluxio/)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## The problem

You need a multi-step async workflow on AWS. Your options:

| Option | Problem |
|---|---|
| **AWS Step Functions** | $25 per million state transitions. A 6-step workflow at 1M/day = $54K/year |
| **AWS Lambda Durable Functions** | Currently limited to `us-east-2` only. Requires Python 3.13/3.14 or Node.js 22/24 |
| **Temporal / Conductor** | Requires running a coordination server — not serverless |
| **DIY with SQS + Lambda** | You will reimplement exactly-once delivery and fan-in barriers incorrectly |

fluxio is the production-ready alternative: a Python library that gives you durable workflow orchestration using only DynamoDB + SQS + Lambda — in **every AWS region**, on **Python 3.10+**, today.

> AWS Lambda Durable Functions (launched re:Invent 2025) validated this exact architectural pattern. fluxio implements the same primitives — checkpoint-and-claim via DynamoDB conditional writes, atomic fan-in barriers, topological dependency resolution — without the regional or runtime restrictions.

---

## The hard problems fluxio solves

These are the same core problems AWS solved in Lambda Durable Functions (re:Invent 2025). fluxio implements equivalent guarantees using primitives available in every region today.

### 1. Exactly-once step execution

SQS delivers messages *at-least-once*. In a normal setup, two Lambda instances can receive the same step message after a cold start, timeout, or retry — both execute, causing double charges, duplicate emails, or corrupted state.

fluxio uses **DynamoDB conditional writes as a distributed mutex** — the same checkpoint-and-claim pattern that underpins Lambda Durable Functions:

```python
# Only one Lambda instance wins this race
UpdateItem(
    ConditionExpression="status = :pending"  # fails for all but the first caller
)
```

The losing instance gets `ConditionalCheckFailedException` and discards its SQS message. Zero coordination server required.

### 2. Atomic fan-in barrier

Parallel branches (fan-out) are easy. Waiting for *all* of them before continuing (fan-in) requires a distributed barrier. fluxio uses DynamoDB's atomic `ADD` operation:

```python
# Each completing branch atomically increments a counter
# The branch that pushes completed_deps == total_deps dispatches the fan-in step
ADD completed_deps :one
```

This is safe without locks because `ADD` is atomic in DynamoDB — no two arrivals can both observe the completion condition simultaneously.

### 3. Poison pill protection

A malformed step message could loop forever between SQS and Lambda. fluxio defends in depth:
- Per-step `timeout` sets SQS `VisibilityTimeout` correctly
- Per-step `retry` count is enforced in state before rescheduling
- SQS redrive policy (set in IaC) moves exhausted messages to a DLQ

---

## Define a workflow

```python
from fluxio import workflow, step

@workflow
class OrderFulfillment:

    @step(retry=3, timeout=30)
    def validate_payment(self, order_id: str, amount: float) -> dict:
        # validate with payment API
        return {"order_id": order_id, "token": "tok_abc", "amount": amount}

    # These three run in PARALLEL after validate_payment
    @step(depends_on=["validate_payment"], retry=3)
    def charge_card(self, order_id: str, token: str, amount: float, **kwargs) -> dict:
        return {"charge_id": "ch_abc", "charged": amount}

    @step(depends_on=["validate_payment"], retry=2)
    def reserve_inventory(self, order_id: str, **kwargs) -> dict:
        return {"reservation_id": "res_abc"}

    @step(depends_on=["validate_payment"], retry=1)
    def send_confirmation(self, order_id: str, **kwargs) -> dict:
        return {"email_sent": True}

    # Fan-in: only runs when ALL three above complete
    @step(depends_on=["charge_card", "reserve_inventory", "send_confirmation"])
    def fulfill_order(self, order_id: str, charge_id: str, reservation_id: str, **kwargs) -> dict:
        return {"fulfillment_id": "ful_abc", "status": "fulfilled"}
```

fluxio automatically resolves the execution graph:

```
Wave 0:  validate_payment
Wave 1:  charge_card | reserve_inventory | send_confirmation  (parallel)
Wave 2:  fulfill_order  (fan-in, waits for all of wave 1)
```

---

## Start a workflow

```python
from fluxio import FluxioEngine

engine = FluxioEngine(
    table_name="fluxio_workflows",
    queue_url="https://sqs.us-east-1.amazonaws.com/123456789/fluxio.fifo",
)

workflow_id = engine.start_workflow(
    workflow_name="OrderFulfillment",
    input_data={"order_id": "ord-001", "amount": 149.99},
)
# "wf-a3f9c12b8e41"
```

---

## Deploy the worker Lambda

```python
# lambda_handler.py
from fluxio.worker import lambda_handler  # that's it
```

```hcl
# terraform
resource "aws_lambda_function" "fluxio_worker" {
  function_name = "fluxio-worker"
  handler       = "lambda_handler.lambda_handler"
  
  environment {
    variables = {
      FLUXIO_TABLE_NAME = aws_dynamodb_table.fluxio.name
      FLUXIO_QUEUE_URL  = aws_sqs_queue.fluxio.url
    }
  }
}

resource "aws_lambda_event_source_mapping" "fluxio_sqs" {
  event_source_arn                   = aws_sqs_queue.fluxio.arn
  function_name                      = aws_lambda_function.fluxio_worker.arn
  batch_size                         = 1
  function_response_types            = ["ReportBatchItemFailures"]
}
```

---

## Cost comparison

For 1M workflow executions/day with a 6-step workflow:

| | Step Functions | Lambda Durable Functions | fluxio |
|---|---|---|---|
| Orchestration cost | $150/day ($54K/yr) | Pay-per-checkpoint (lower) | ~$0.10/day ($36/yr) |
| Region availability | Global | `us-east-2` only (as of 2026) | **Every AWS region** |
| Python version required | Any | 3.13 / 3.14 only | **3.10+** |
| Exactly-once guarantee | Yes | Yes | Yes (DynamoDB conditional writes) |
| Fan-out / fan-in | Yes | Yes | Yes (atomic barrier) |
| Infrastructure required | None | None | DynamoDB + SQS (already have it) |
| Bring your own infra | No | No | **Yes** |

> Lambda Durable Functions is the right choice when it's available in your region and you're on a supported runtime. fluxio is for everyone else — and for teams that want the same guarantees on infrastructure they already own and operate.

---

## How fluxio compares to Lambda Durable Functions

AWS Lambda Durable Functions (launched December 2025 at re:Invent) implements the same checkpoint-and-replay pattern as fluxio. If you're on a supported region and runtime, it's a solid AWS-native choice.

fluxio fills the gaps:

- **Region coverage** — Lambda Durable Functions launched in `us-east-2` only. fluxio works in `ap-southeast-1`, `eu-west-1`, `us-gov-west-1`, everywhere you already deploy.
- **Python version** — Lambda Durable Functions requires Python 3.13 or 3.14. fluxio supports Python 3.10, 3.11, 3.12, and 3.13.
- **Bring your own infrastructure** — fluxio runs on DynamoDB and SQS resources you already provision, audit, and own. No new managed service in your blast radius.
- **Inspectable state** — because fluxio's state lives in your own DynamoDB table, you can query, debug, and replay workflow state with standard AWS tooling, no SDK required.

---

## License

MIT
