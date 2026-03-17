# fluxio — Limitations and Known Constraints

This document exists so nobody deploys fluxio into production without a clear picture
of what it can and cannot do.

---

## What fluxio is

A proof-of-concept that demonstrates correct distributed systems primitives for
serverless workflow orchestration:
- Distributed mutex via DynamoDB conditional writes (exactly-once step claiming)
- Atomic fan-in barrier via DynamoDB map path writes
- Topological sort for dependency resolution
- Structured retry with DLQ routing

The patterns are correct. The implementation is not production-hardened.

---

## Scale ceiling

**Designed for:** tens of thousands to low hundreds of thousands of workflow
executions per day.

**Not designed for:** Amazon.com, Walmart.com, or any workload running millions
of executions per day with sustained high concurrency.

Specific limits:

- **SQS FIFO queue:** hard cap of 3,000 TPS with batching. Above ~5M executions/day
  with bursty traffic, you will hit queue throttling.
- **DynamoDB TransactWriteItems:** max 99 steps per workflow (100-item transaction limit).
  Validated at runtime with a clear error message.
- **Single table, single queue:** no sharding across multiple tables or queues.
  At very high throughput, both become bottlenecks.

---

## What "exactly-once" means here

fluxio guarantees exactly-once **claiming** of a step, not exactly-once **execution**
of your code.

If Lambda is killed mid-execution (OOM, 15-minute timeout, infrastructure failure)
after `claim_step()` succeeds but before `complete_step()` runs:
- The step is stuck in CLAIMED until `recover_stuck_steps()` resets it
- The step will then re-execute from the beginning

For non-idempotent operations (payment processing, external API calls with side effects),
your step code must implement its own idempotency:
- Check before acting: "has this payment already been processed for order X?"
- Use your provider's idempotency key support (Stripe, etc.)

---

## Stuck step recovery

A step stuck in CLAIMED will hang the workflow indefinitely without intervention.
To recover, call `state.recover_stuck_steps(workflow_id)` from a scheduled Lambda
(EventBridge every 5 minutes is a reasonable interval).

This is not automatic — you must wire it up in your infrastructure.

---

## Missing production features

The following are not implemented and would be required before serious production use:

- **Workflow cancellation** — no way to cancel an in-flight workflow
- **Schema versioning** — deploying a new workflow version breaks in-flight executions
  from the previous version if step names change
- **Structured observability** — only `print()` statements; no metrics, no tracing,
  no structured logging
- **Workflow listing / status dashboard** — no way to query "how many workflows are
  currently RUNNING?" without a full table scan
- **Large result handling** — step results are stored inline in DynamoDB items;
  results larger than a few KB should be stored in S3 with a reference
- **Backpressure** — no mechanism to limit concurrent workflow starts

---

## When to use something else instead

| Your situation | Better choice |
|---|---|
| High volume (>500K executions/day) | Step Functions Express |
| Long-running workflows needing exactly-once | Step Functions Standard |
| In us-east-2 on Python 3.13+ | Lambda Durable Functions |
| Need visual debugger and execution history | Step Functions (either) |
| Non-idempotent steps without your own idempotency layer | Step Functions Standard |

---

## Cost reality

At 100K executions/day with a 6-step workflow:
- DynamoDB writes: ~$4.40/day
- SQS: negligible
- Lambda execution: same as any other approach

Step Functions Express at the same scale: ~$0.60/day.

fluxio is not cheaper than Step Functions at scale. It is cheaper than Step Functions
**Standard** at low-to-medium volume, and it works in regions where Lambda Durable
Functions is not yet available.
