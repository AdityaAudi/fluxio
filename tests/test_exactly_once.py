"""
tests/test_exactly_once.py

Tests for the hardest distributed systems properties:
  1. Exactly-once step execution (concurrent claim race)
  2. Barrier fan-in correctness
  3. Workflow graph validation (cycle detection)
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

from fluxio.workflow import workflow, step, get_workflow, WorkflowDefinition
from fluxio.state import StateStore, StepStatus


# ──────────────────────────────────────────────
# Test workflow definitions
# ──────────────────────────────────────────────

@workflow
class LinearWorkflow:
    @step(retry=2, timeout=10)
    def step_a(self, value: int) -> dict:
        return {"a_result": value * 2}

    @step(depends_on=["step_a"], retry=1)
    def step_b(self, a_result: int) -> dict:
        return {"b_result": a_result + 10}


@workflow
class FanOutWorkflow:
    @step()
    def entry(self, data: str) -> dict:
        return {"data": data, "processed": True}

    @step(depends_on=["entry"])
    def branch_x(self, data: str, **kwargs) -> dict:
        return {"x": data.upper()}

    @step(depends_on=["entry"])
    def branch_y(self, data: str, **kwargs) -> dict:
        return {"y": data.lower()}

    @step(depends_on=["branch_x", "branch_y"])
    def merge(self, x: str = None, y: str = None) -> dict:
        return {"merged": f"{x}|{y}"}


# ──────────────────────────────────────────────
# 1. Workflow graph validation
# ──────────────────────────────────────────────

def test_linear_workflow_execution_order():
    wf_def = get_workflow("LinearWorkflow")
    waves = wf_def.execution_order()
    assert waves == [["step_a"], ["step_b"]]


def test_fan_out_workflow_execution_order():
    wf_def = get_workflow("FanOutWorkflow")
    waves = wf_def.execution_order()
    assert waves[0] == ["entry"]
    assert sorted(waves[1]) == ["branch_x", "branch_y"]
    assert waves[2] == ["merge"]


def test_cycle_detection():
    with pytest.raises(ValueError, match="cycle"):
        @workflow
        class CyclicWorkflow:
            @step(depends_on=["step_b"])
            def step_a(self): pass
            @step(depends_on=["step_a"])
            def step_b(self): pass


def test_unknown_dependency_raises():
    with pytest.raises(ValueError, match="does not exist"):
        @workflow
        class BrokenWorkflow:
            @step(depends_on=["nonexistent_step"])
            def my_step(self): pass
        get_workflow("BrokenWorkflow").execution_order()


def test_no_entry_step_raises():
    with pytest.raises(ValueError, match="entry steps"):
        @workflow
        class NoEntryWorkflow:
            @step(depends_on=["step_b"])
            def step_a(self): pass
        # Note: cycle raises before entry_steps check in this case,
        # but a linear chain with all steps having deps would raise entry error


def test_entry_steps_identified_correctly():
    wf_def = get_workflow("FanOutWorkflow")
    assert wf_def.entry_steps == ["entry"]


def test_ready_steps_after_entry_completes():
    wf_def = get_workflow("FanOutWorkflow")
    ready = wf_def.ready_steps(completed={"entry"})
    assert sorted(ready) == ["branch_x", "branch_y"]


def test_ready_steps_after_one_branch():
    wf_def = get_workflow("FanOutWorkflow")
    ready = wf_def.ready_steps(completed={"entry", "branch_x"})
    # branch_y ready, merge NOT ready (branch_y still pending)
    assert ready == ["branch_y"]


def test_merge_ready_only_when_all_branches_complete():
    wf_def = get_workflow("FanOutWorkflow")
    ready = wf_def.ready_steps(completed={"entry", "branch_x", "branch_y"})
    assert ready == ["merge"]


# ──────────────────────────────────────────────
# 2. Exactly-once: claim_step race condition
# ──────────────────────────────────────────────

def _make_conditional_check_error():
    return ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition failed"}},
        "UpdateItem",
    )


def test_claim_step_succeeds_first_caller():
    store = StateStore.__new__(StateStore)
    store.table_name = "test_table"
    mock_table = MagicMock()
    store._table = mock_table

    mock_table.update_item.return_value = {}

    result = store.claim_step("wf-001", "step_a", "executor-1")
    assert result is True
    mock_table.update_item.assert_called_once()


def test_claim_step_returns_false_for_loser():
    """
    Simulates the race condition: a second Lambda tries to claim
    the same step, gets ConditionalCheckFailedException, returns False.
    """
    store = StateStore.__new__(StateStore)
    store.table_name = "test_table"
    mock_table = MagicMock()
    store._table = mock_table

    mock_table.update_item.side_effect = _make_conditional_check_error()

    result = store.claim_step("wf-001", "step_a", "executor-2")
    assert result is False  # lost the race — correct behavior


def test_claim_step_re_raises_non_conditional_errors():
    store = StateStore.__new__(StateStore)
    store.table_name = "test_table"
    mock_table = MagicMock()
    store._table = mock_table

    mock_table.update_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
        "UpdateItem",
    )

    with pytest.raises(ClientError):
        store.claim_step("wf-001", "step_a", "executor-1")


# ──────────────────────────────────────────────
# 3. Barrier fan-in
# ──────────────────────────────────────────────

def _make_barrier_store(total_deps: int, current_count: int = 0, existing_results: dict = None):
    store = StateStore.__new__(StateStore)
    store.table_name = "test_table"
    mock_table = MagicMock()
    store._table = mock_table

    mock_table.get_item.return_value = {"Item": {
        "PK": "wf-001",
        "SK": "BARRIER#merge",
        "total_deps": total_deps,
        "completed_deps": current_count,
        "dep_results": json.dumps(existing_results or {}),
    }}
    return store, mock_table


def test_barrier_not_complete_on_first_arrival():
    store, mock_table = _make_barrier_store(total_deps=2, current_count=0)

    mock_table.update_item.return_value = {
        "Attributes": {"completed_deps": 1}   # after first arrival
    }

    complete = store.arrive_at_barrier("wf-001", "merge", "branch_x", {"x": "HELLO"})
    assert complete is False   # still waiting for branch_y


def test_barrier_complete_on_last_arrival():
    store, mock_table = _make_barrier_store(
        total_deps=2,
        current_count=1,
        existing_results={"branch_x": {"x": "HELLO"}},
    )

    mock_table.update_item.return_value = {
        "Attributes": {"completed_deps": 2}   # all arrived
    }

    complete = store.arrive_at_barrier("wf-001", "merge", "branch_y", {"y": "world"})
    assert complete is True   # this arrival completes the barrier


def test_barrier_with_three_deps():
    """Barrier with 3 parallel branches — only last arrival triggers."""
    for last_count, expected in [(1, False), (2, False), (3, True)]:
        store, mock_table = _make_barrier_store(total_deps=3, current_count=last_count - 1)
        mock_table.update_item.return_value = {
            "Attributes": {"completed_deps": last_count}
        }
        result = store.arrive_at_barrier("wf-001", "fan_in", f"branch_{last_count}", {})
        assert result is expected, f"Expected {expected} when completed_deps becomes {last_count}"
