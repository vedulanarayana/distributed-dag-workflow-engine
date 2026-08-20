from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
WAL_FILE = DATA_DIR / "wal.log"
STATE_DB = DATA_DIR / "state.db"


class TaskState:
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"


MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1
BACKOFF_MULTIPLIER = 2
MAX_BACKOFF_SECONDS = 60

DATA_DIR.mkdir(parents=True, exist_ok=True)
