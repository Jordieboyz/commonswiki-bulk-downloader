import threading
import time

class AdaptiveRateLimiter:
  def __init__(self, base : float = 1.0, max  : float= 60.0, factor : float= 2.0):
    self.base = base
    self.max = max
    self.factor = factor

    self._delay = base
    self._lock = threading.Lock()
    self._pause = threading.Event()
    self._pause.set()

  def wait(self):
    self._pause.wait()

  def success(self):
    with self._lock:
      self._delay = max(self.base, self._delay / self.factor)

  def backoff(self, retry_after : float | None = None):
    with self._lock:
      if retry_after is not None:
        self._delay = min(self.max, retry_after)
      else:
        self._delay = min(self.max, self._delay * self.factor)

      self._pause.clear()
      delay = self._delay

    # global sleep
    time.sleep(delay)
    self._pause.set()