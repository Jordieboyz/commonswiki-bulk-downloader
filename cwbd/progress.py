import time
import sys
import mmap
import threading

class PhaseProgressMonitor(threading.Thread):
  def __init__(self, total, phase, progress_file = None):
    super().__init__(daemon=True)

    self.progress_file = progress_file
    self.total = total

    self.start_time = time.time()
    
    self.last_update = 0
    self.last_matches_update = 0

    self.update_interval = .05
    self.match_interval = 1
    
    self._stop = False
    self.phase = phase
    
    self._current = 0
    self._matches = 0

    self._start()

  def run(self):
    while not self._stop:
      now = time.time()

      if self.progress_file and now - self.last_matches_update >= self.match_interval:
        self._matches = self.count_newlines_mmap()
        self.last_matches_update = now

      if self._current or self._matches:
        if now - self.last_update >= self.update_interval:
          self.update()
          self.last_update = now

      time.sleep(0.05)

  def _start(self):
    sys.stdout.write(f"\n[INFO] Phase '{self.phase}' Started!\n")
    sys.stdout.flush()
    self.start()

  def count_newlines_mmap(self):
    try:
      with open(self.progress_file , "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
          return mm[:].count(b"\n")
    except:
      return 0
    
  def update(self):
    elapsed = time.time() - self.start_time
    rate = self._current / elapsed if elapsed > 0 else 0
    remaining = (self.total - self._current) / rate if rate > 0 else 0
    percent = (self._current / self.total) * 100 if self.total else 0

    progress_line = (
        f'[PROGRESS] {self._current:,}/{self.total:,} '
        f'({percent:5.2f}%) | {rate:,.0f} lines/s | '
        f'ETA: {format_time(remaining)}'
    )

    phase_line = f'[{self.phase.upper()}] Entries: {self._matches:,}' if self.phase else ''

    # Pad lines to terminal width
    terminal_width = 120
    progress_line = progress_line.ljust(terminal_width)
    phase_line = phase_line.ljust(terminal_width)

    # If this is not the first print, move cursor up 2 lines to overwrite previous
    if getattr(self, "_printed_once", False):
        sys.stdout.write("\033[F\033[F")  # move cursor up 2 lines

    sys.stdout.write(progress_line + "\n" + phase_line + "\n")
    sys.stdout.flush()

    self._printed_once = True


  def finish(self):
    self.update()
    self._stop = True
    self.join()
    sys.stdout.write(f"[INFO] Phase '{self.phase}' Completed!\n")
    sys.stdout.flush()

def format_time(seconds):
  seconds = int(seconds)
  h = seconds // 3600
  m = (seconds % 3600) // 60
  s = seconds % 60
  if h > 0:
      return f"{h}h {m}m"
  elif m > 0:
      return f"{m}m {s}s"
  else:
      return f"{s}s"

def get_phase_total(phase):
    return 

