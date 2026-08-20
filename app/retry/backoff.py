import time
import random
from app.config import INITIAL_BACKOFF_SECONDS, BACKOFF_MULTIPLIER, MAX_BACKOFF_SECONDS


class ExponentialBackoff:

    def __init__(self, initial=INITIAL_BACKOFF_SECONDS, multiplier=BACKOFF_MULTIPLIER,
                 max_backoff=MAX_BACKOFF_SECONDS, jitter_factor=0.1):
        self.initial = initial
        self.multiplier = multiplier
        self.max_backoff = max_backoff
        self.jitter_factor = jitter_factor

    def get_delay(self, attempt: int) -> float:
        delay = min(self.initial * (self.multiplier ** attempt), self.max_backoff)
        return delay + random.uniform(0, self.jitter_factor * delay)

    def wait(self, attempt: int):
        time.sleep(self.get_delay(attempt))

    def get_retry_schedule(self, max_retries: int) -> list:
        return [self.get_delay(i) for i in range(max_retries)]
