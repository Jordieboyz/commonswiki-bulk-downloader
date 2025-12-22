import os
from itertools import repeat
from concurrent.futures import ThreadPoolExecutor
import requests

from .context import ProgramContext
from .download_utils import get_json_data
from .progress import PhaseProgressMonitor

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


def download_file(ctx : ProgramContext, file_title : str, outfolder : str, tracker : PhaseProgressMonitor):
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
  out_path = os.path.join(outfolder, file_title)

  try:
    r = requests.get(url, stream=True, timeout=20, headers=DOWNLOAD_HEADERS)
    if r.status_code == 200:
      with open(out_path, "wb") as f:
        for chunk in r.iter_content(1024*1024):
          f.write(chunk)

      ctx.downloads_set.add(file_title)
      # print(f"[SUCCES] Downloaded: {file_title}")
  except Exception as e:
    ctx.failed_downloads_set.add(file_title)
    # print(f"[ERROR] Failed to download {file_title} : {e}")
  finally:
    if tracker:
      tracker._current += 1
      tracker._matches += 1


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
    with open(pctx.invalid_files, 'r+') as f:
      pctx.failed_downloads_set = set(f.read().splitlines())
  except FileNotFoundError:
    open(pctx.invalid_files, 'w').close()

  pctx.downloads_set = collect_existing_filenames([pctx.output_dir])
  
  file_category_titles_dict = get_json_data(pctx.found_files)
  if not file_category_titles_dict:
    print('No downloadable files found!')
    print('Make sure to run \'cwbd fetch <arguments>\' first!')
    return

  for category, subfields in file_category_titles_dict.items():
    if not any(category == c for c in pctx.categories):
      continue

    if not (files := subfields.get("files", [])):
      continue
    
    tracker = PhaseProgressMonitor(subfields.get("n_files", 0), category)

    out_folder = os.path.join(pctx.output_dir, category.replace('_', ' '))
    os.makedirs(out_folder, exist_ok=True)

    with ThreadPoolExecutor(max_workers=pctx.max_workers) as executor:
      executor.map(download_file, repeat(pctx), files, repeat(out_folder), repeat(tracker))
    
    tracker.finish()

    # write failed downloads after category completed
    with open(pctx.invalid_files, 'a') as f:
      for item in pctx.failed_downloads_set:
        f.write(item + '\n')
      f.flush()