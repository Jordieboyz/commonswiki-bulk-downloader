from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pathlib import Path

from .context import ProgramContext
from .progress import PhaseProgressMonitor
from .cwbd_utils import *
from .rateLimiter import AdaptiveRateLimiter

def download_file(session : requests.Session, rate : AdaptiveRateLimiter, out_dir : Path, file_title : str, max_retries  : int = 5):
  """
  Download a wikimedia commons file via Special:FilePath and save it to the ../imgs directory.
  Skips download if the file already exists locally.

  Args:
      ctx (ProgramContext): A class containing all settings, sets and filepaths for the program.
      file_title (str): Exact wikimedia filename including extension.
      outfolder (str): Folder path where the file should be saved.

  Return:
      None

  Notes:
      - Uses stream download to handle lage images efficiently.
      - Succesful downloads are tracked in ctx.downloads_set.
  """
  url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{file_title}"
  out_path = out_dir / file_title

  for attempt in range(1, max_retries + 1):
    rate.wait()

    try:
      r = session.get(url, stream=True, timeout=20)

      if r.status_code == 200:
        with open(out_path, "wb") as f:
          for chunk in r.iter_content(2 * 1024 * 1024):
            f.write(chunk)

        rate.success()
        return file_title, True

      elif r.status_code == 429:
        retry_after = r.headers.get("Retry-After")
        rate.backoff(retry_after if retry_after and retry_after.isdigit() else None)
        continue

      elif 500 <= r.status_code < 600:
        rate.backoff()
        continue

    except:
      return file_title, False
  return file_title, False


def download_media_files(pctx : ProgramContext):
  """
  Download all media files associated with the input categories in the program context.

  Args:
      pctx (ProgramContext): A class containing all settings, sets and filepaths for the program.

  Returns:
      None
  
  Notes: 
      - Created output folders for each category automatically.
      - Skips files that are already downloaded.
      - Track failed downloads in a dedicated file pctx.invalid_files.
      - Uses a ThreadPoolExecutor to perform paralell downloads based on pctx.max_workers.
  """
  pctx.output_dir.mkdir(parents=True, exist_ok=True)
  phase_str = 'download'

  try:
    pctx.downloads_set.update(pctx.downloaded_files.read_text(encoding='utf-8').splitlines())
    pctx.failed_downloads_set.update(pctx.invalid_files.read_text(encoding='utf-8').splitlines())
  except FileNotFoundError:
    pass

  file_map = get_json_data(pctx.found_files)
  if not file_map:
    print('No downloadable files found! Run \'cwbd fetch\' first.')
    return
  else:
    n_files = sum(int(entry['n_files']) for entry in file_map.values())
    save_position(pctx.progress_scanner, fformat(phase_str, 'files', 'total', sep=':'), n_files)

    n_categories = len(file_map)
    save_position(pctx.progress_scanner, fformat(phase_str, 'categories', 'total', sep=':'), n_categories)

  completed_categories = get_progress_dl_categories(pctx.progress_scanner) # (cat, value)

   
  #----------------------------
  # Category Loop
  #--------------------------------

  for category, meta in file_map.items():
    if pctx.rsearch:
      if not any(category.startswith(c) for c in pctx.categories):
        continue
    else:
      if category not in pctx.categories:
        continue
    
    if (category, meta['n_files']) in completed_categories:
      continue

    if not (files := meta.get("files", [])):
      continue

    out_dir : Path = pctx.output_dir / category
    out_dir.mkdir(parents=True, exist_ok=True)

    start = load_position(pctx.progress_scanner, 
                          fformat(phase_str, category, sep=':'))
    total = len(files) if len(files) < 100 else 100 # arbitrary number to prevent downloadign thousands for all categories... could do a proportinate amount? or cli arg?
    if start >= total:
      continue
    
    # otherwise these get skipped during existance check
    # This is mostly a fix for an issues when the user exits the program before the 
    # category is finished downloading all files...
    pctx.downloads_set.update(files[:start])

    files = files[start:total]
    if not files:
      continue
    
    rate_limiter = AdaptiveRateLimiter()

    # Setup tracker
    tracker = PhaseProgressMonitor(total, category, pctx.downloaded_files) # maybe we need to check Entries: 
    tracker._current = start

    with requests.Session() as s:
      s.headers.update({
        "User-Agent": "user@gmail.com",
        "Referer": "https://commons.wikimedia.org/"
      })

      with ThreadPoolExecutor(max_workers=pctx.max_workers) as executor:
        futures = {
          executor.submit(download_file, s, rate_limiter, out_dir, f): f for f in files
        }

        for f in as_completed(futures):
          file_title, succes = f.result()

          if succes:
            pctx.downloads_set.add(file_title)
            tracker._current += 1

            save_position(pctx.progress_scanner, 
                          fformat(phase_str, category, sep=':'),
                          tracker._current)
              
          else:
            pctx.failed_downloads_set.add(file_title)
  
      tracker.finish()          

    pctx.downloaded_files.write_text(
      '\n'.join(pctx.downloads_set), encoding='utf-8'
    )

    pctx.invalid_files.write_text(
      '\n'.join(pctx.failed_downloads_set), encoding='utf-8'
    )
