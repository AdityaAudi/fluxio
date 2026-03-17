"""
examples/order_fulfillment.py

Real-world example: e-commerce order fulfillment workflow.
Runs in any AWS region on Python 3.10+ — no us-east-2 or runtime restrictions.

Execution graph:
                  validate_payment
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   reserve_inventory  charge_card  send_confirmation
          │            │            │
          └────────────┴────────────┘
                       │
                  fulfill_order
                       │
                  notify_warehouse
                       │
                  close_order

Demonstrates:
  - Sequential steps (validate → charge)
  - Parallel fan-out (3 steps run simultaneously after validate)
  - Fan-in barrier (fulfill_order waits for ALL 3 parallel steps)
  - Error handling and retry
  - How to start a workflow and check its status
"""

import os
import time
import boto3

from fluxio.workflow import workflow, step
from fluxio.engine import FluxioEngine


# ──────────────────────────────────────────────
# Workflow definition
# ──────────────────────────────────────────────

@workflow
class OrderFulfillment:
    """
    End-to-end order fulfillment. Each @step is a separate Lambda invocation.
    Steps with the same dependency wave run in parallel.
    """

    @step(retry=3, timeout=30)
    def validate_payment(self, order_id: str, amount: float, currency: str = "USD") -> dict:
        """
        Validate payment details. Entry step — receives raw workflow input.
        In production: call payment validation API, check fraud score.
        """
        print(f"Validating payment for order {order_id}: {amount} {currency}")
        if amount <= 0:
            raise ValueError(f"Invalid amount: {amount}")
        return {
            "order_id": order_id,
            "amount": amount,
            "currency": currency,
            "validation_token": f"tok_{order_id[:8]}",
        }

    @step(depends_on=["validate_payment"], retry=2, timeout=15)
    def reserve_inventory(self, order_id: str, amount: float, **kwargs) -> dict:
        """
        Reserve items in inventory. Runs in parallel with charge_card and send_confirmation.
        In production: call inventory service, decrement stock atomically.
        """
        print(f"Reserving inventory for order {order_id}")
        return {
            "order_id": order_id,
            "reservation_id": f"res_{order_id[:8]}",
            "reserved": True,
        }

    @step(depends_on=["validate_payment"], retry=3, timeout=20)
    def charge_card(self, order_id: str, amount: float, validation_token: str, **kwargs) -> dict:
        """
        Charge the customer's card. Runs in parallel with reserve_inventory.
        In production: call Stripe/Braintree with the validation token.
        """
        print(f"Charging {amount} for order {order_id} with token {validation_token}")
        return {
            "order_id": order_id,
            "charge_id": f"ch_{order_id[:8]}",
            "charged_amount": amount,
            "status": "succeeded",
        }

    @step(depends_on=["validate_payment"], retry=1, timeout=10)
    def send_confirmation(self, order_id: str, amount: float, currency: str = "USD", **kwargs) -> dict:
        """
        Send order confirmation email. Runs in parallel.
        Non-critical — only 1 retry since email failure shouldn't block order.
        """
        print(f"Sending confirmation email for order {order_id}")
        return {
            "order_id": order_id,
            "email_sent": True,
            "message_id": f"msg_{order_id[:8]}",
        }

    # ── Fan-in: waits for ALL 3 parallel branches ──────
    @step(
        depends_on=["reserve_inventory", "charge_card", "send_confirmation"],
        retry=2,
        timeout=30,
    )
    def fulfill_order(
        self,
        order_id: str,
        reservation_id: str = None,
        charge_id: str = None,
        email_sent: bool = False,
        **kwargs,
    ) -> dict:
        """
        Fan-in step. Only runs after ALL three parallel branches complete.
        Consolidates results and creates the fulfillment record.
        """
        print(f"Fulfilling order {order_id} — reservation={reservation_id}, charge={charge_id}")
        return {
            "order_id": order_id,
            "fulfillment_id": f"ful_{order_id[:8]}",
            "reservation_id": reservation_id,
            "charge_id": charge_id,
            "email_sent": email_sent,
            "status": "fulfilled",
        }

    @step(depends_on=["fulfill_order"], retry=3, timeout=15)
    def notify_warehouse(self, order_id: str, fulfillment_id: str, **kwargs) -> dict:
        """
        Send pick-and-pack instructions to warehouse system.
        """
        print(f"Notifying warehouse for fulfillment {fulfillment_id}")
        return {
            "order_id": order_id,
            "fulfillment_id": fulfillment_id,
            "warehouse_notified": True,
            "pick_ticket": f"pk_{fulfillment_id}",
        }

    @step(depends_on=["notify_warehouse"], retry=1, timeout=10)
    def close_order(self, order_id: str, pick_ticket: str, **kwargs) -> dict:
        """
        Final step: mark order as complete in OMS.
        """
        print(f"Closing order {order_id} with pick ticket {pick_ticket}")
        return {
            "order_id": order_id,
            "status": "complete",
            "pick_ticket": pick_ticket,
        }


# ──────────────────────────────────────────────
# Print the execution plan before running
# ──────────────────────────────────────────────

def show_execution_plan():
    from fluxio.workflow import get_workflow
    wf_def = get_workflow("OrderFulfillment")
    waves = wf_def.execution_order()
    print("\nOrderFulfillment execution plan:")
    for i, wave in enumerate(waves):
        parallel = "parallel" if len(wave) > 1 else "sequential"
        print(f"  Wave {i} ({parallel}): {', '.join(wave)}")
    print()


# ──────────────────────────────────────────────
# Start a workflow run
# ──────────────────────────────────────────────

def run_order(order_id: str, amount: float):
    engine = FluxioEngine(
        table_name=os.environ.get("FLUXIO_TABLE_NAME", "fluxio_workflows"),
        queue_url=os.environ["FLUXIO_QUEUE_URL"],
    )

    show_execution_plan()

    workflow_id = engine.start_workflow(
        workflow_name="OrderFulfillment",
        input_data={
            "order_id": order_id,
            "amount": amount,
            "currency": "USD",
        },
    )

    print(f"Started workflow: {workflow_id}")
    print(f"Track at: aws dynamodb query --table-name fluxio_workflows "
          f"--key-condition-expression 'PK = :id' "
          f"--expression-attribute-values '{{\":id\":{{\"S\":\"{workflow_id}\"}}}}'")
    return workflow_id


if __name__ == "__main__":
    import sys
    order_id = sys.argv[1] if len(sys.argv) > 1 else "ord-demo-001"
    amount   = float(sys.argv[2]) if len(sys.argv) > 2 else 149.99
    run_order(order_id, amount)
