import json
import shutil
from pathlib import Path

from .cli import get_cli_input
from .download_utils import *
from .scanner import scan_commons_db
from .download import download_media_files
from .context import ProgramContext

def get_phase_max(dump_file : Path, ctx : ProgramContext):
  """
  Determine the maximum of matches to expect from a specific scan phase.

  Args:
      dump_file (Path): Path to the SQL dump file being processed.
      ctx (ProgramContext): A class containing all settings, sets and filepaths for the program.

  Returns:
    int: Maximum expected matches for this scan phase.

  Notes:
      - Used to set ctx.max_phase_matches when recursive search is disabled
  """
  match dump_file:
    case _ if dump_file == ctx.linktarget_dump:
      return len(ctx.process_categories)
    case _ if dump_file == ctx.category_dump:
      # TODO: ??? request amount of files in category through API
      return 0
    case _ if dump_file == ctx.page_dump:
      return len(ctx.program_set)
  return 0


def find_media_file_titles(ctx : ProgramContext):
  """
  Scan SQL dump files to find all media file titles associated with the input categories.

  Args:
      ctx (ProgramContext): A class containing all settings, sets and filepaths for the program.

  Returns:
      None

  Notes:
      - Process linktarget, categorylinks and page dump files sequentially.
      - Updates ctx.program_set after every phase completes.
  """
  for dump_file, output_file in ctx.program_files.items():
    if not ctx.rsearch:
      ctx.max_phase_matches = get_phase_max(dump_file, ctx)
    
    scan_commons_db(dump_file, output_file, ctx)

    if dump_file == ctx.page_dump:
      ctx.program_set = get_title_set(output_file)
    else:
      ctx.program_set = get_id_set(output_file)


def retrace(ctx : ProgramContext):
  """
  Construct and ordered dictionary of media files mapped to their categories and page files.

  Args:
      ctx (ProgramContext): A class containing all settings, sets and filepaths for the program.
    
  Retuns:
    dict[str, dict]: Dictionary mapping category media titles to metadata, including:
      - 'id': Linktarget ID
      - 'n_files: Number of files found
      - 'files': List of filenames belonging to the category
  
  Notes:
      - Aggregates files per media title ina  structure suitable for JSON export.
  """
  pfiles = ctx.program_files
  ordered_downloads = dict()
  lt_id_to_title = {}
  
  with open(pfiles[ctx.linktarget_dump], 'r', encoding='utf-8') as link:
    for line in link:
      id, title = line.rstrip('\n').split('\t', 1)
      lt_id_to_title[int(id)] = title

  page_id_to_file = {}
  with open(pfiles[ctx.page_dump], 'r', encoding='utf-8') as page:
    for line in page:
      id, title = line.rstrip('\n').split('\t', 1)
      page_id_to_file[int(id)] = title

  with open(pfiles[ctx.category_dump], 'r', encoding='utf-8') as cl:
    for line in cl:

      id, _, tid = line.split('\t')
      
      id = int(id)
      tid = int(tid)

      if id not in page_id_to_file:
        continue

      title = lt_id_to_title.get(tid)
      if not title:
        continue
      
      if title not in ordered_downloads:
        ordered_downloads[title] = {
          'id' : int(id),
          'n_files' : 0,
          'files' : [],
        }

      entry = ordered_downloads[title]
      entry['files'].append(page_id_to_file[id])
      entry['n_files'] += 1

  return ordered_downloads

def update_found_files(json_file : Path, data: dict):
  """
    Update the JSON file tracking discovered media files with new entries.

    Args:
        json_file (Path): Path to json file storing previously found files.
        data (dict): Dictionary of newly found media files (output from retrace)

    Returns:
        None

    Notes:
        - Merges new files with existing entries, avoiding duplicates
  """
  if json_file.exists():
    with json_file.open('r', encoding='utf-8') as f:
      existing = json.load(f)
  else:
      existing = {}
  
  for cat, subfields in data.items():
    if cat not in existing:
      existing[cat] = {
        'id' : subfields['id'],
        'n_files' : len(subfields['files']),
        'files' : subfields['files'],
      }
    else:
      existing_files = set(existing[cat]['files'])
      new_files = [f for f in subfields[ 'files'] if f not in existing_files]
      existing[cat]['files'].extend(new_files)
      existing[cat]['n_files'] = len(existing[cat]['files'])

  with json_file.open('w', encoding='utf-8') as f:
    json.dump(existing, f, indent=2, ensure_ascii=False)

def load_normalized_categories_from_file(infile : str):
  with open(infile, 'r', encoding='utf-8') as f:
    return set(normalize(norm_title) for norm_title in f.read().splitlines() if norm_title.strip())

def clean_program_files():
  """
  Remove program-generated files and directories used for bookkeeping and resuming runs.

  This function deletes only internal artifacts created by the program itself and
  explicitly avoids removing any user-provided inputs or downloaded media files.

  The following items are removed if present:
      - Scanner checkpoint directory and its contents
      - Scanner progress file
      - Per-phase scan output files
      - Categorized file title JSON output
      - Python __pycache__ directories inside the project tree

  Args:
      ctx (ProgramContext): Active program context containing derived paths.

  Return:
      None

  Notes:
      - This operation is destructive and cannot be undone.
      - Safe to call multiple times.
      - Missing files or directories are ignored silently.
  """
  ctx = ProgramContext()

  if ctx.checkpoint_dir.exists():
    shutil.rmtree(ctx.checkpoint_dir, ignore_errors=True)

  if ctx.found_files.exists():
    ctx.found_files.unlink()

  if ctx.invalid_files.exists():
    ctx.invalid_files.unlink()

  project_root = Path.cwd()

  for pycache in project_root.rglob("__pycache__"):
    if pycache.is_dir():
      shutil.rmtree(pycache, ignore_errors=True)


def main():
  """
  Main entry point for the Commonswiki Bulk Downloader CLI.

  Workflow:
      1. Parse CLI arguments.
      2. Initialize ProgramContext with input categories, dump paths, output directory, and settings.
      3. Determine which categories have not been processed yet.
      4. Scan dump files to find media file titles for each category.
      5. Update JSON tracking file with newly discovered files.
      6. Download media files concurrently to the output directory.

  Notes:
      - Intended to be run via the `cwbd` console script or `python -m cwbd.main`.
      - Supports incremental runs by skipping categories already processed.
      - Uses ProgramContext to track progress, failed downloads, and maximum workers.
  """
  args = get_cli_input()

  match args.command:
    case 'clean':
      clean_program_files()
    case 'fetch':
      pctx = ProgramContext.init_fetch(
        dumps_dir=Path(args.dumps_dir),
        input_categories=load_normalized_categories_from_file(args.category_file),
        recursive_search=args.recursive_search
      )

      pctx.process_categories = pctx.categories - set(get_json_data(pctx.found_files))
      if pctx.process_categories:
        find_media_file_titles(pctx)
        update_found_files(pctx.found_files, retrace(pctx))
    
    case 'download':
      pctx = ProgramContext.init_download(
        output_dir=Path(args.output_dir),
        input_categories=load_normalized_categories_from_file(args.category_file),
        max_workers=args.workers,
      )
          
      download_media_files(pctx)

    case 'run':
      pctx = ProgramContext.init_run(
        dumps_dir=Path(args.dumps_dir),
        output_dir=Path(args.output_dir),
        input_categories=load_normalized_categories_from_file(args.category_file),
        recursive_search=args.recursive_search,
        max_workers=args.workers,
      )
      
      pctx.process_categories = pctx.categories - set(get_json_data(pctx.found_files))
      if pctx.process_categories:
        find_media_file_titles(pctx)
        update_found_files(pctx.found_files, retrace(pctx))

      download_media_files(pctx)

    case _ :
      print('Not a valid CLI argument, check the documenation for possible CLI arguments.')

if __name__ == '__main__':
  main()