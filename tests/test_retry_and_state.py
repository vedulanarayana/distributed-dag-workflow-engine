from app.state.wal import WriteAheadLog
from app.state.state_manager import StateManager
from app.retry.retry_manager import RetryManager
from app.config import TaskState


def make_manager(tmp_path):
    wal = WriteAheadLog(str(tmp_path / "wal.log"))
    return StateManager(str(tmp_path / "state.db"), wal)


def test_retry_count_increments_and_persists(tmp_path):
    sm = make_manager(tmp_path)
    rm = RetryManager(sm, max_retries=3)

    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("not yet")

    ok = rm.execute_with_retry("t1", "wf1", flaky)

    assert ok is True
    state = sm.get_state("t1")
    assert state["state"] == TaskState.SUCCEEDED
    assert state["retry_count"] == 2  # two retries before it succeeded on the third try


def test_task_goes_to_dead_letter_after_max_retries(tmp_path):
    sm = make_manager(tmp_path)
    rm = RetryManager(sm, max_retries=2)

    def always_fails():
        raise RuntimeError("permanent failure")

    ok = rm.execute_with_retry("t1", "wf1", always_fails)

    assert ok is False
    assert sm.get_state("t1")["state"] == TaskState.FAILED


def test_idempotency_key_blocks_duplicate_transition(tmp_path):
    sm = make_manager(tmp_path)
    first = sm.transition_state("t1", "wf1", TaskState.PENDING, TaskState.RUNNING, idempotency_key="k1")
    second = sm.transition_state("t1", "wf1", TaskState.PENDING, TaskState.RUNNING, idempotency_key="k1")
    assert first is True
    assert second is False
