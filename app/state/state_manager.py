import sqlite3
import json
from typing import Dict, Optional


class StateManager:

    def __init__(self, db_path, wal):
        self.db_path = str(db_path)
        self.wal = wal
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_states (
                task_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                state TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                payload TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                idempotency_key TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dead_letters (
                task_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                error TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                definition TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def save_workflow(self, workflow_id, definition: Dict):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO workflows (workflow_id, definition) VALUES (?, ?)",
            (workflow_id, json.dumps(definition)),
        )
        conn.commit()
        conn.close()

    def load_all_workflows(self) -> Dict[str, Dict]:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT workflow_id, definition FROM workflows").fetchall()
        conn.close()
        return {workflow_id: json.loads(definition) for workflow_id, definition in rows}

    def transition_state(self, task_id, workflow_id, old_state, new_state,
                          idempotency_key=None, payload=None, increment_retry=False):
        if idempotency_key and self._is_duplicate(idempotency_key):
            return False

        record = {"task_id": task_id, "workflow_id": workflow_id,
                   "old_state": old_state, "new_state": new_state, "payload": payload or {}}
        if not self.wal.append(record):
            return False

        self._apply_transition(task_id, workflow_id, new_state, payload, increment_retry)

        if idempotency_key:
            self._store_idempotency_key(idempotency_key, task_id)

        return True

    def _apply_transition(self, task_id, workflow_id, new_state, payload=None, increment_retry=False):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT retry_count FROM task_states WHERE task_id = ?", (task_id,)).fetchone()
        current_retry = row[0] if row else 0
        new_retry = current_retry + 1 if increment_retry else current_retry

        conn.execute("""
            INSERT INTO task_states (task_id, workflow_id, state, retry_count, payload, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(task_id) DO UPDATE SET
                state = excluded.state,
                retry_count = excluded.retry_count,
                payload = excluded.payload,
                updated_at = CURRENT_TIMESTAMP
        """, (task_id, workflow_id, new_state, new_retry, json.dumps(payload or {})))
        conn.commit()
        conn.close()

    def send_to_dead_letter(self, task_id, workflow_id, error):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO dead_letters (task_id, workflow_id, error) VALUES (?, ?, ?)",
            (task_id, workflow_id, error),
        )
        conn.commit()
        conn.close()

    def get_state(self, task_id) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT task_id, workflow_id, state, retry_count, payload, updated_at FROM task_states WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        return {"task_id": row[0], "workflow_id": row[1], "state": row[2],
                "retry_count": row[3], "payload": json.loads(row[4]), "updated_at": row[5]}

    def _is_duplicate(self, key) -> bool:
        conn = sqlite3.connect(self.db_path)
        exists = conn.execute("SELECT 1 FROM idempotency_keys WHERE idempotency_key = ?", (key,)).fetchone()
        conn.close()
        return exists is not None

    def _store_idempotency_key(self, key, task_id):
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT OR IGNORE INTO idempotency_keys (idempotency_key, task_id) VALUES (?, ?)", (key, task_id))
        conn.commit()
        conn.close()

    def recover_from_wal(self):
        records = sorted(self.wal.read_all(), key=lambda r: r.get("timestamp", ""))
        for r in records:
            self._apply_transition(r["task_id"], r.get("workflow_id", ""), r["new_state"], r.get("payload"))
        self.wal.clear()
