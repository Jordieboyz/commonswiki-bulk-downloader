import json
from pathlib import Path

from .cli import get_cli_input
from .download_utils import *
from .scanner import scan_commons_db
from .download import download_media_files
from .context import ProgramContext

# TODO: ??? request amount of files in category through API

def get_phase_max(dump_file : Path, ctx : ProgramContext):
  match dump_file:
    case _ if dump_file == ctx.linktarget_dump:
      return len(ctx.process_categories)
    case _ if dump_file == ctx.category_dump:
      return 0
    case _ if dump_file == ctx.page_dump:
      return len(ctx.program_set)
  return 0


def find_media_file_titles(ctx : ProgramContext):

  for dump_file, output_file in ctx.program_files.items():
    print(f'Processing: {dump_file}...')

    if not ctx.recursive_search:
      ctx.max_phase_matches = get_phase_max(dump_file, ctx)
    
    scan_commons_db(dump_file, output_file, ctx)

    if dump_file == ctx.page_dump:
      ctx.program_set = get_title_set(output_file)
    else:
      ctx.program_set = get_id_set(output_file)


def retrace(ctx : ProgramContext):
  pfiles = ctx.program_files
  ordered_downloads = dict()
  lt_id_to_title = {}
  
  with open(pfiles[ctx.linktarget_dump], 'r') as link:
    for line in link:
      id, title = line.rstrip('\n').split('\t', 1)
      lt_id_to_title[int(id)] = title

      ordered_downloads[title] = {
        'id' : int(id),
        'n_files' : 0,
        'files' : [],
      }

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
      
      entry = ordered_downloads[title]
      entry['files'].append(page_id_to_file[id])
      entry['n_files'] += 1

  return ordered_downloads

def update_found_files(json_file : Path, data: dict):
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

def main():
  args = get_cli_input()

  ctx = ProgramContext(
    dump_dir=Path(args.dumps_dir),
    output_dir=Path(args.output_dir),
    input_categories=load_normalized_categories_from_file(args.category_file),
    recursive_search=args.no_recursive_search,
    max_workers=args.workers,
  )

  ctx.process_categories = ctx.input_categories - set(get_json_data(ctx.found_files))
  if ctx.process_categories:
    find_media_file_titles(ctx)
    update_found_files(ctx.found_files, retrace(ctx.program_files))

  download_media_files(ctx)

if __name__ == '__main__':
  main()
  
