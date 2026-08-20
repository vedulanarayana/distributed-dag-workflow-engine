import asyncio
import time
from app.dag.graph import DAG
from app.state.wal import WriteAheadLog
from app.state.state_manager import StateManager
from app.retry.retry_manager import RetryManager
from app.worker.dispatcher import WorkerDispatcher


def test_same_level_tasks_run_concurrently(tmp_path):
    wal = WriteAheadLog(str(tmp_path / "wal.log"))
    sm = StateManager(str(tmp_path / "state.db"), wal)
    rm = RetryManager(sm, max_retries=0)
    dispatcher = WorkerDispatcher(sm, rm)

    dag = DAG()
    dag.add_node("a", func=lambda node: time.sleep(0.3))
    dag.add_node("b", func=lambda node: time.sleep(0.3))

    start = time.time()
    asyncio.run(dispatcher.execute_workflow(dag))
    elapsed = time.time() - start

    # if these actually overlap, total time is close to 0.3s, not 0.6s+
    assert elapsed < 0.5
