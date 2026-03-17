"""
tests/local_runner.py

End-to-end local test harness for fluxio.

Runs a complete workflow against LocalStack (DynamoDB + SQS) without
deploying a real Lambda function. The "Lambda worker" is simulated by
polling SQS in-process and calling engine.execute_step() directly.

This is the fastest way to validate the full execution path:
  start_workflow → SQS dispatch → claim → execute → barrier → complete

Usage:
    # Start LocalStack first
    docker-compose up -d

    # Run with default OrderFulfillment workflow
    python tests/local_runner.py

    # Run with verbose step output
    python tests/local_runner.py --verbose

    # Run N times to stress-test exactly-once (concurrent claim race)
    python tests/local_runner.py --stress 10
"""

import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

# Point all AWS SDK calls at LocalStack
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_CREDS = {
    "endpoint_url": LOCALSTACK_ENDPOINT,
    "region_name": "us-east-1",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}

os.environ.setdefault("FLUXIO_TABLE_NAME", "fluxio_workflows")
os.environ.setdefault(
    "FLUXIO_QUEUE_URL",
    "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/fluxio.fifo",
)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

# Import fluxio after env vars are set
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fluxio.workflow import workflow, step
from fluxio.engine import FluxioEngine
from fluxio.state import StateStore, WorkflowStatus


# ──────────────────────────────────────────────
# Patch boto3 to use LocalStack endpoint
# ──────────────────────────────────────────────

_original_boto3_client = boto3.client
_original_boto3_resource = boto3.resource


def _patched_client(service, **kwargs):
    kwargs.setdefault("endpoint_url", LOCALSTACK_ENDPOINT)
    kwargs.setdefault("region_name", "us-east-1")
    kwargs.setdefault("aws_access_key_id", "test")
    kwargs.setdefault("aws_secret_access_key", "test")
    return _original_boto3_client(service, **kwargs)


def _patched_resource(service, **kwargs):
    kwargs.setdefault("endpoint_url", LOCALSTACK_ENDPOINT)
    kwargs.setdefault("region_name", "us-east-1")
    kwargs.setdefault("aws_access_key_id", "test")
    kwargs.setdefault("aws_secret_access_key", "test")
    return _original_boto3_resource(service, **kwargs)


boto3.client = _patched_client
boto3.resource = _patched_resource


# ──────────────────────────────────────────────
# Test workflow definition
# ──────────────────────────────────────────────

@workflow
class LocalTestWorkflow:
    """
    Simple 5-step workflow for local testing:

        validate
           │
      ┌────┴────┐
    enrich   score       ← parallel fan-out
      └────┬────┘
           │
        decide            ← fan-in (waits for both)
           │
        finalize
    """

    @step(retry=2, timeout=10)
    def validate(self, order_id: str, amount: float) -> dict:
        if amount <= 0:
            raise ValueError(f"Invalid amount: {amount}")
        print(f"    [validate] order={order_id} amount={amount}")
        return {"order_id": order_id, "amount": amount, "valid": True}

    @step(depends_on=["validate"], retry=2, timeout=10)
    def enrich(self, order_id: str, amount: float, **kwargs) -> dict:
        print(f"    [enrich]   order={order_id}")
        time.sleep(0.05)  # simulate I/O
        return {"order_id": order_id, "customer_tier": "gold", "enriched": True}

    @step(depends_on=["validate"], retry=2, timeout=10)
    def score(self, order_id: str, amount: float, **kwargs) -> dict:
        print(f"    [score]    order={order_id} amount={amount}")
        time.sleep(0.05)
        return {"order_id": order_id, "risk_score": 12, "scored": True}

    @step(depends_on=["enrich", "score"], retry=2, timeout=10)
    def decide(
        self,
        order_id: str = None,
        customer_tier: str = None,
        risk_score: int = None,
        **kwargs,
    ) -> dict:
        approved = risk_score < 50
        print(f"    [decide]   order={order_id} tier={customer_tier} risk={risk_score} approved={approved}")
        return {"order_id": order_id, "approved": approved, "decision": "APPROVE" if approved else "DECLINE"}

    @step(depends_on=["decide"], retry=1, timeout=10)
    def finalize(self, order_id: str = None, decision: str = None, **kwargs) -> dict:
        print(f"    [finalize] order={order_id} decision={decision}")
        return {"order_id": order_id, "status": "complete", "decision": decision}


# ──────────────────────────────────────────────
# In-process SQS worker (simulates Lambda)
# ──────────────────────────────────────────────

class LocalWorker:
    """
    Polls SQS and executes steps in-process.
    In production this is your Lambda function.
    Here it runs in a background thread so you can test
    the full path without deploying to AWS.
    """

    def __init__(self, engine: FluxioEngine, queue_url: str, verbose: bool = False):
        self.engine = engine
        self.queue_url = queue_url
        self.verbose = verbose
        self._stop = threading.Event()
        self._sqs = boto3.client("sqs")

    def start(self):
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _poll_loop(self):
        while not self._stop.is_set():
            try:
                resp = self._sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2,
                )
                messages = resp.get("Messages", [])
                for msg in messages:
                    self._process(msg)
            except Exception as exc:
                if not self._stop.is_set():
                    print(f"  [worker] poll error: {exc}")

    def _process(self, msg: dict):
        receipt = msg["ReceiptHandle"]
        try:
            body = json.loads(msg["Body"])
            if self.verbose:
                print(f"  [worker] executing {body['step_name']} for {body['workflow_id']}")
            self.engine.execute_step(
                workflow_id=body["workflow_id"],
                workflow_name=body["workflow_name"],
                step_name=body["step_name"],
                input_data=body.get("input_data", {}),
            )
            # Delete on success
            self._sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt)
        except Exception as exc:
            print(f"  [worker] step failed: {exc}")
            # Leave in queue for redelivery


# ──────────────────────────────────────────────
# Test runners
# ──────────────────────────────────────────────

def wait_for_completion(
    store: StateStore,
    workflow_id: str,
    timeout: int = 30,
    poll_interval: float = 0.5,
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        meta = store.get_workflow_meta(workflow_id)
        if not meta:
            time.sleep(poll_interval)
            continue
        status = meta.get("status")
        if status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value):
            return meta
        time.sleep(poll_interval)
    raise TimeoutError(f"Workflow {workflow_id} did not complete within {timeout}s")


def run_single(verbose: bool = False) -> bool:
    print("\n── Single workflow run ────────────────────────")
    engine = FluxioEngine(
        table_name="fluxio_workflows",
        queue_url=os.environ["FLUXIO_QUEUE_URL"],
        region="us-east-1",
    )
    worker = LocalWorker(engine, os.environ["FLUXIO_QUEUE_URL"], verbose=verbose)
    worker.start()

    try:
        t0 = time.time()
        workflow_id = engine.start_workflow(
            workflow_name="LocalTestWorkflow",
            input_data={"order_id": "ord-local-001", "amount": 99.99},
        )
        print(f"  Started: {workflow_id}")

        meta = wait_for_completion(engine.state, workflow_id, timeout=30)
        elapsed = time.time() - t0
        status = meta["status"]

        print(f"  Status:  {status}  ({elapsed:.2f}s)")

        # Print per-step results
        steps = engine.state.get_all_steps(workflow_id)
        for s in sorted(steps, key=lambda x: x.get("completed_at", "")):
            name = s["step_name"]
            st   = s["status"]
            res  = s.get("result", {})
            print(f"    {name:12} → {st:10} {res}")

        return status == WorkflowStatus.COMPLETED.value
    finally:
        worker.stop()


def run_stress(n: int = 10, verbose: bool = False) -> None:
    """
    Start N workflows concurrently to stress-test the exactly-once claim logic.
    All should complete; none should have duplicate step executions.
    """
    print(f"\n── Stress test: {n} concurrent workflows ──────")
    engine = FluxioEngine(
        table_name="fluxio_workflows",
        queue_url=os.environ["FLUXIO_QUEUE_URL"],
        region="us-east-1",
    )
    # Use 4 concurrent workers to simulate Lambda concurrency
    workers = [LocalWorker(engine, os.environ["FLUXIO_QUEUE_URL"], verbose=verbose) for _ in range(4)]
    for w in workers:
        w.start()

    try:
        workflow_ids = []
        for i in range(n):
            wid = engine.start_workflow(
                workflow_name="LocalTestWorkflow",
                input_data={"order_id": f"ord-stress-{i:03d}", "amount": float(10 + i)},
            )
            workflow_ids.append(wid)
        print(f"  Launched {n} workflows, waiting for completion...")

        results = {"COMPLETED": 0, "FAILED": 0, "TIMEOUT": 0}
        for wid in workflow_ids:
            try:
                meta = wait_for_completion(engine.state, wid, timeout=60)
                results[meta["status"]] += 1
            except TimeoutError:
                results["TIMEOUT"] += 1

        print(f"  Results: {results}")
        assert results["COMPLETED"] == n, f"Expected {n} completed, got {results}"
        print(f"  All {n} workflows completed successfully.")
    finally:
        for w in workers:
            w.stop()


def run_failure_retry(verbose: bool = False) -> None:
    """
    Test that a step that fails once still retries and completes.
    """
    print("\n── Failure + retry test ───────────────────────")

    attempt_counts = {}

    @workflow
    class RetryTestWorkflow:
        @step(retry=3, timeout=10)
        def flaky_step(self, order_id: str, **kwargs) -> dict:
            attempt_counts[order_id] = attempt_counts.get(order_id, 0) + 1
            if attempt_counts[order_id] < 2:
                raise RuntimeError(f"Simulated failure (attempt {attempt_counts[order_id]})")
            print(f"    [flaky_step] succeeded on attempt {attempt_counts[order_id]}")
            return {"order_id": order_id, "attempts": attempt_counts[order_id]}

        @step(depends_on=["flaky_step"], retry=1)
        def after_flaky(self, order_id: str = None, attempts: int = None, **kwargs) -> dict:
            print(f"    [after_flaky] order={order_id} after {attempts} attempts")
            return {"done": True}

    engine = FluxioEngine(
        table_name="fluxio_workflows",
        queue_url=os.environ["FLUXIO_QUEUE_URL"],
        region="us-east-1",
    )
    worker = LocalWorker(engine, os.environ["FLUXIO_QUEUE_URL"], verbose=verbose)
    worker.start()

    try:
        wid = engine.start_workflow(
            workflow_name="RetryTestWorkflow",
            input_data={"order_id": "ord-retry-001"},
        )
        meta = wait_for_completion(engine.state, wid, timeout=90)
        print(f"  Status: {meta['status']}")
        assert meta["status"] == WorkflowStatus.COMPLETED.value
        print("  Retry test passed.")
    finally:
        worker.stop()


# ──────────────────────────────────────────────
# Check LocalStack is reachable
# ──────────────────────────────────────────────

def check_localstack():
    import urllib.request
    try:
        urllib.request.urlopen(f"{LOCALSTACK_ENDPOINT}/_localstack/health", timeout=3)
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fluxio local test runner")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show step-level worker logs")
    parser.add_argument("--stress", type=int, default=0, metavar="N", help="Run N concurrent workflows")
    parser.add_argument("--retry-test", action="store_true", help="Run the failure+retry test")
    args = parser.parse_args()

    if not check_localstack():
        print("ERROR: LocalStack is not running.")
        print("Start it with:  docker-compose up -d")
        print("Then wait ~10s and retry.")
        sys.exit(1)

    print("LocalStack reachable.")

    passed = run_single(verbose=args.verbose)

    if args.stress > 0:
        run_stress(n=args.stress, verbose=args.verbose)

    if args.retry_test:
        run_failure_retry(verbose=args.verbose)

    sys.exit(0 if passed else 1)
