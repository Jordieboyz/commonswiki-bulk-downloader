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

    self._sleeping = False

  def wait(self):
    self._pause.wait()
    time.sleep(self._delay)

  def success(self):
    with self._lock:
      self._delay = max(self.base, self._delay / self.factor)

  def backoff(self, retry_after : float | None = None):
    with self._lock:
      if self._sleeping:
        return
      
      self._sleeping = True
      self._pause.clear()

      if retry_after is not None:
        delay = min(self.max, float(retry_after))
      else:
        delay = min(self.max, self._delay * self.factor)

      self._delay = delay
    # global sleep
    time.sleep(delay)

    with self._lock:
      self._sleeping = False
      self._pause.set()