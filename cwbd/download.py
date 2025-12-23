import os
from itertools import repeat
from concurrent.futures import ThreadPoolExecutor
import requests
from pathlib import Path
from queue import Queue
import threading

from .context import ProgramContext
from .progress import PhaseProgressMonitor
from .cwbd_utils import *

def collect_existing_filenames(folders : list[str]):
  """
  Walks through folder to find all filenames of downloaded images.

  Args:
      folders (list[str]): Folder to search for file titles that already have been downloaded.

  Return:
      list[str]: All filenames of downloaded images.
  """
  filenames = set()

  for folder in folders:
    for root, dirs, files in os.walk(folder):
      for f in files:
        filenames.add(f)
  return filenames


def download_file(ctx : ProgramContext, file_title : str, category : str, tracker : PhaseProgressMonitor, 
                  log_queue : Queue, dl_queue : Queue):
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
  
  DOWNLOAD_HEADERS = {
    "User-Agent": "user@gmail.com",
    "Referer": "https://commons.wikimedia.org/"
  }

  # prevent double processing
  if file_title in ctx.failed_downloads_set:
    return

  if file_title in ctx.downloads_set:
    return

  url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{file_title}"
  
  out_folder = os.path.join(ctx.output_dir, category)
  os.makedirs(out_folder, exist_ok=True)

  out_path = os.path.join(out_folder, file_title)

  try:
    r = requests.get(url, stream=True, timeout=20, headers=DOWNLOAD_HEADERS)
    if r.status_code == 200:
      with open(out_path, "wb") as f:
        for chunk in r.iter_content(1024*1024):
          f.write(chunk)

      ctx.downloads_set.add(file_title)
      dl_queue.put(file_title)

      # print(f"[SUCCES] Downloaded: {file_title}")
  except Exception as e:
    ctx.failed_downloads_set.add(file_title)
    # print(f"[ERROR] Failed to download {file_title} : {e}")
  finally:
    if tracker:
      tracker._current += 1
      tracker._matches += 1
    log_queue.put((category, tracker._current))

def writer_thread(file : Path, queue : Queue):
  with file.open('a', encoding='utf-8') as f:
    while True:
      item = queue.get()
      if item is None:
        queue.task_done()
        break
      
      f.write(item+'\n')
      f.flush()

      queue.task_done()

def safe_pos_thread(file : Path, queue : Queue):
    while True:
      item = queue.get()
      if item is None:
        queue.task_done()
        break
      
      if len(item) == 2:
        cat, n = item
        save_position(file, fformat('download', cat, 'size', sep=':'), n)

      queue.task_done()

# def download_media_files(infile: str, categories: list[str], out : str, n_workers : int = 10):
def download_media_files(pctx :ProgramContext):
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
  
  try:
    with open(pctx.downloaded_files, 'r', encoding='utf-8') as downloads:
      pctx.downloads_set = set(downloads.read().splitlines())

    with open(pctx.invalid_files, 'r', encoding='utf-8') as invalid_downloads:
      pctx.failed_downloads_set = set(invalid_downloads.read().splitlines())
  except FileNotFoundError:
    pass

  file_category_titles_dict = get_json_data(pctx.found_files)
  if not file_category_titles_dict:
    print('No downloadable files found!')
    print('Make sure to run \'cwbd fetch <arguments>\' first!')
    return
  else:
    n_files = sum(int(entry['n_files']) for entry in file_category_titles_dict.values())
    save_position(pctx.progress_scanner, fformat('download', 'size', sep=':'), n_files)




   # TODO: check in progression fril which categories we have had to prevent a lot of calls to single file existences.
  for category, subfields in file_category_titles_dict.items():
    # Add recusive-search for easy download of all files found
    if pctx.rsearch:
      if not any(category.startswith(c) for c in pctx.categories):
        continue
    else:
      if not any(category == c for c in pctx.categories):
        continue

    if not (files := subfields.get("files", [])):
      continue
    
    cats = get_progress_dl_categories(pctx.progress_scanner)
    if (category,subfields['n_files'] ) in cats  : # and value == total or size???
      continue

    cat_n_files = subfields.get("n_files", 0)
    
    start = load_position(pctx.progress_scanner, fformat('download', category, 'size', sep=':'))
    total = 100

    log_writer = Queue()
    dl_writer = Queue()

    log_thread = threading.Thread(target=safe_pos_thread, args=(pctx.progress_scanner, log_writer), daemon=True)
    log_thread.start()

    dl_thread = threading.Thread(target=writer_thread, args=(pctx.downloaded_files, dl_writer), daemon=True)
    dl_thread.start()


    tracker = PhaseProgressMonitor(total, category)
    tracker._current = start
    tracker._matches = start

    with ThreadPoolExecutor(max_workers=pctx.max_workers) as executor:
      executor.map(download_file, repeat(pctx), files[start:total], repeat(category), repeat(tracker), repeat(log_writer), repeat(dl_writer))

    tracker.finish()

    log_writer.join()
    log_writer.put(None)
    log_thread.join()

    dl_writer.join()
    dl_writer.put(None)
    dl_thread.join()

    # save_position(pctx.progress_scanner, fformat('download', category, 'size', sep=':'), cat_n_files)


    try:
      # if we quit during the processing of a category, the next time we run, it will append the progress from the category... 
      # TODO: multithread fiel writing is unsafe, but is the solution to track the amount of donwloaded files instead of whole categories...
      with open(pctx.downloaded_files, 'w', encoding='utf-8') as Wdownloads, \
        open(pctx.invalid_files, 'w', encoding='utf-8') as Ldownloads:
        
        # write succesful downloads after category completed
        for item in pctx.downloads_set:
          Wdownloads.write(item + '\n')
        Wdownloads.flush()

        # write failed downloads after category completed
        for item in pctx.failed_downloads_set:
          Ldownloads.write(item + '\n')
        Ldownloads.flush()

    except Exception as e:
      print(e)
      pass

