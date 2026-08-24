import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict


class WriteAheadLog:
    """
    Every state change gets written here before it's applied to the
    real state store. If the process dies between those two steps,
    replaying this file on startup gets you back to where you were.
    """

    def __init__(self, wal_file: str):
        self.wal_file = wal_file
        if not os.path.exists(self.wal_file):
            open(self.wal_file, "w").close()

    def append(self, record: Dict) -> bool:
        record["timestamp"] = datetime.now(timezone.utc).isoformat()
        record.setdefault("transaction_id", str(uuid.uuid4()))
        try:
            with open(self.wal_file, "a") as f:
                f.write(json.dumps(record) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return True
        except OSError as e:
            print(f"wal append failed: {e}")
            return False

    def read_all(self) -> list:
        if not os.path.exists(self.wal_file):
            return []
        records = []
        with open(self.wal_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def clear(self):
        open(self.wal_file, "w").close()
