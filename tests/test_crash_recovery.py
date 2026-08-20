import subprocess
import sys
import textwrap
from app.state.wal import WriteAheadLog
from app.state.state_manager import StateManager
from app.config import TaskState


def test_killed_worker_process_state_is_recoverable(tmp_path):
    wal_path = tmp_path / "wal.log"
    db_path = tmp_path / "state.db"

    worker_script = textwrap.dedent(f"""
        import os, signal
        from app.state.wal import WriteAheadLog
        from app.state.state_manager import StateManager
        from app.config import TaskState

        wal = WriteAheadLog(r"{wal_path}")
        sm = StateManager(r"{db_path}", wal)
        sm.transition_state("task_1", "wf_1", TaskState.PENDING, TaskState.RUNNING)
        os.kill(os.getpid(), signal.SIGKILL)
    """)

    proc = subprocess.run([sys.executable, "-c", worker_script])
    assert proc.returncode != 0

    wal = WriteAheadLog(str(wal_path))
    sm = StateManager(str(db_path), wal)
    sm.recover_from_wal()

    state = sm.get_state("task_1")
    assert state is not None
    assert state["state"] == TaskState.RUNNING
