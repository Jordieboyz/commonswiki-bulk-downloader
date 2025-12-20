import gzip
import re
from functools import partial

from download_utils import *
from context import ProgramContext

def count_lines_fast(file) :
  try:
    with open(file, 'rb') as f:
      return sum(chunk.count(b'\n') for chunk in iter(lambda: f.read(1024 * 1024), b''))
  except FileNotFoundError:
    return 0
    
def scan_commons_db(infile : str, outfile : str, pctx : ProgramContext):
  """
  Scan a compressed Wikimedia Commons SQL dump. Extract relevant rows and save parsed output to a file.
  Supports resume functionality for huge files.

  Args:
      infile (str): Path to the compressed '.sql.gz' dump file.
      scanfile (str): File used to store resume/progress positions.
      outfile (str): Destination file to append extracted results.
      save_interval (int): Number of lines between progress saves.

  Return:
      None

  Notes:
      - Supports incremental scanning using a simple key=value progress file.
      - Uses TABLE_PARSER_PATTERN to determinehandler + regex per table.
      - Only lines containing "INSERT INTO '{table}' are processed.
      - Handles extremely large SQL dumps without re-scanning completed parts.
  """
  db_entry = str(infile).split(".", 1)[0].split("-")[-1]
  parser = get_parser(db_entry, pctx)
  scanfile = pctx.progress_scanner

  # if use_checkpoints:
  formatted_prog_str = fformat(infile, parser['handler'].func.__name__, pctx.save_interval, sep=':')
  formatted_size_str = fformat(infile, 'size', sep=':')
    
  start = load_position(scanfile, formatted_prog_str)
  end = load_position(scanfile, formatted_size_str)

  found_matches = count_lines_fast(outfile) # load from file
  if pctx.max_phase_matches and found_matches >= pctx.max_phase_matches:
    print('Already found all matches')
    return

  lc = 0
  
  if not end or start < end:
    with gzip.open(infile, 'rt', encoding='utf-8', errors='ignore') as inf,\
      open(outfile, "a", encoding="utf-8") as outf:

      for line in inf:
        lc += 1

        if f"INSERT INTO `{db_entry}`" not in line or lc < start:
          continue
        
        if lc % pctx.save_interval == 0:
          save_position(scanfile, formatted_prog_str, lc)
          outf.flush()

        if (matches := extract_match(line, parser)):
          for match in matches:
            outf.write(fformat(match, newline=True))
            save_position(scanfile, formatted_prog_str, lc)

            found_matches += 1
            if pctx.max_phase_matches and found_matches >= pctx.max_phase_matches:
                # save final linecount to prevent recalculation, since these are huge files. 
              return
  else:
    lc = start

  # save final linecount to prevent recalculation, since these are huge files. 
  save_position(scanfile, formatted_prog_str, lc)
  save_position(scanfile, formatted_size_str, lc)

def extract_match(line : str, pp : dict):
  """
  Extract regex matches from a SQL INSERT line and pass results to a parser
  
  Args:
      line (str): A line from the SQL dump.
      pp (dict): A pattern/handler dictionary with:
          - 'regex': compiled regex used to find matches
          - 'handler': function to process each match

  Return:
      List: A list of processed match results (filtered by handler)
  """
  matches = []
  for m in pp['regex'].findall(line):
    if (res := pp['handler'](m)):
      matches.append(res)
  return matches

def lt_handler(ctx : ProgramContext, match : tuple):
  """
  Parse a row from the 'linktarget' dump.

  Args:
      match (tuple): Regex captured fields.

  Return:
      str | None: Formatted '{id}\t{title}' if valid, otherwise None

  Notes:
      - Only accepts categories starting with 'Taken_wih_'
      - Backslashes are removed for safe output
  """
  id, ns, title = match
  
  try:
    id = int(id)
    ns = int(ns)
  except ValueError:
    return
  
  if ns == WikiNamespace.CATEGORY:
    for cat in ctx.process_categories:
      if ctx.recursive_search:
        if title.startswith(cat):
          return fformat(id, title)
      else:
        if title == cat:
          return fformat(id, title)
  return None
      
def cl_handler(ctx : ProgramContext, match):
  """
  Parse a row from the 'CategoryLinks' dump.

  Args:
      match (tuple): Regex captured fields.

  Return:
      str | None: Formatted '{from}\t{sortkey}' if valid, otherwise None

  Notes:
      - Only accepts rows of type 'file'
      - target_id must appear in LT_ID_SET to qualify 
  """
  _from, sortkey, _, sortkey_prefix, type, _, target_id = match
  try:
    _from = int(_from)
    target_id = int(target_id)
  except ValueError:
    return None
  
  if type == 'file':
    if target_id in ctx.program_set:
      if not sortkey_prefix:
        return fformat(_from, sortkey, target_id)
  return None

def page_handler(ctx : ProgramContext, match):
  """
  Parse a row from the 'Pages' dump.

  Args:
      match (tuple): Regex captured fields.

  Return:
      str | None: Formatted '{id}\t{title}' if valid, otherwise None

  Notes:
      - Only entries with namespace FILE (6) are considered
      - Id must appear in CL_ID_SET to qualify 
  """
  id, ns, title = match
    
  try:
    id = int(id)
    ns = int(ns) 
  except ValueError:
    return
  
  if id in ctx.program_set:
    if ns == WikiNamespace.FILE:
      if os.path.splitext(title)[1].lower() in ('.jpg', '.jpeg'):
        return fformat(id, title)
      


CATEGORYLINKS_REGEX = re.compile(
  r'\('
  r'(\d+),'                  # From           : int
  r"'([^']*)',"              # Sortkey        : string
  r"'([^']*)',"              # Timestamp      : str
  r"'([^']*)',"              # Sortkey_prefix : str
  r"'(page|subcat|file)',"   # Type           : enum
  r'(\d+),'                  # Collation_id   : int
  r'(\d+)'                   # Target_id      : int
  r'\)'
)

PAGE_REGEX = re.compile(
  r"\("
  r"(\d+),"               # ID        : int
  r"(-?\d+),"             # Namespace : int (can be negative)
  r"'([^']*)'"            # Title     : str
  r"(?:,.*?)*"            # Ignore all remaining fields
  r"\)"
)

LINKTARGET_REGEX =  re.compile(
  r'\('
  r'(\d+),'                 # ID        : int 
  r'(-?\d+),'               # Namespace : int (can be negative)
  r"'(.*?)'"                # Title     : str
  r'\)'
)

def get_parser(id_str : str, ctx : ProgramContext):
  match id_str:
    case 'linktarget': 
      return {
        'regex' : LINKTARGET_REGEX, 
        'handler' : partial(lt_handler, ctx)
      }
    case 'categorylinks':
      return  {
        'regex' : CATEGORYLINKS_REGEX, 
        'handler' : partial(cl_handler, ctx)
      }    
    case 'page':
      return  {
        'regex' : PAGE_REGEX, 
        'handler' : partial(page_handler, ctx)
      }
  
  # not a valid db entry
  return None