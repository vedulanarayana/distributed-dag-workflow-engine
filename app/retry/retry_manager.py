from app.config import MAX_RETRIES, TaskState
from app.retry.backoff import ExponentialBackoff


class RetryManager:

    def __init__(self, state_manager, max_retries: int = MAX_RETRIES):
        self.state_manager = state_manager
        self.max_retries = max_retries
        self.backoff = ExponentialBackoff()

    def execute_with_retry(self, task_id, workflow_id, func, *args, **kwargs) -> bool:
        for attempt in range(self.max_retries + 1):
            self.state_manager.transition_state(
                task_id, workflow_id,
                TaskState.PENDING if attempt == 0 else TaskState.RETRYING,
                TaskState.RUNNING,
                increment_retry=(attempt > 0),
            )
            try:
                func(*args, **kwargs)
                self.state_manager.transition_state(task_id, workflow_id, TaskState.RUNNING, TaskState.SUCCEEDED)
                return True
            except Exception as e:
                if attempt == self.max_retries:
                    self.state_manager.transition_state(
                        task_id, workflow_id, TaskState.RUNNING, TaskState.FAILED, payload={"error": str(e)}
                    )
                    self.state_manager.send_to_dead_letter(task_id, workflow_id, str(e))
                    return False
                self.state_manager.transition_state(task_id, workflow_id, TaskState.RUNNING, TaskState.RETRYING)
                self.backoff.wait(attempt)
        return False
