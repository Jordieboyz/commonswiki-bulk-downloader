import os
import json

class WikiNamespace():
  """
  Namespaces used to identify wikimedia type
  """
  MAIN = 0              
  TALK = 1
  USER = 2
  USER_TALK = 3
  PROJECT = 4
  PROJECT_TALK = 5
  FILE = 6
  FILE_TALK = 7
  MEDIAWIKI = 8
  MEDIAWIKI_TALK = 9
  TEMPLATE = 10
  TEMPLATE_TALK = 11
  HELP = 12
  HELP_TALK = 13
  CATEGORY = 14
  CATEGORY_TALK = 15


def fformat(*parameters, sep : str ='\t', newline : bool = False):
  return sep.join("" if p is None else str(p) for p in parameters) + ('\n' if newline else '')

def normalize_cat_input(raw_cats : list[str]):
  return { normalize(item) for item in raw_cats}

def normalize(s : str):
  return s.strip().replace(' ',  '_')

# check file wether some categories have been processed before.
def get_json_data(infile : str):
  if not os.path.exists(infile):
    return {}
  
  try:
    with open(infile, 'r', encoding='utf-8') as f:
      return json.load(f)
  except (json.JSONDecodeError, OSError, ValueError) as e:
    print(f'Failed to read processed categories from {infile} : {e}!')
    return {}
  

def load_position(scanner_file : str, dump_file : str):
  """
  Read the last saved line position for a given dump file configuration.

  Args:
      scanner_file (str): Path to the progress/save file.
      dump_file (str): The pogress key identifying the dump scan.

  Return:
      int: The stored line number or 0 if not found
  """
  try:
    with open(scanner_file, 'r') as f:
      # skip signature
      next(f, None)
      for line in f:
        key, value = line.split('=', 1)
        if key.strip() == dump_file:
          return int(value.strip())
  except:
    return 0
  return 0

def save_position(scanner_file : str, dump_file : str, position: int):
  """  
  Save or update the current scan position for a SQL dump.

  Args:
      scanner_file (str): Path to the progress/save file.
      dump_file (str): The pogress key identifying the dump scan.
      position (int): The line number to store

  Return:
      None

  Notes:
      - Stores all keys in simple '{key}=value' pairs.
      - preserves old entries while updting the specified one.
  """
  signature = ""
  positions = {}
  if os.path.exists(scanner_file):
    with open(scanner_file, 'r') as f:
      
      signature = f.readline()

      for line in f:
        key, value = line.split('=', 1)
        positions[key.strip()] = int(value.strip())
  
  positions[dump_file] = position

  with open(scanner_file, 'w') as f:
    f.write(signature)
    for key, value in positions.items():
      f.write(fformat(key, value, sep='=', newline=True))

def get_id_set(input_file : str):
  """
  Load the first column of a file as a set of integers.

  Args:
      input_file (str): Path to the file formatted as '{id}\t{title}.

  Return:
      set[int]: All IDs found in the file.
  """
  return get_set(input_file, 0)

def get_title_set(input_file : str):
  """
  Load the second column of a file as a set of strings.

  Args:
      input_file (str): Path to the file formatted as '{id}\t{title}.

  Return:
      set[str]: All titles found in the file.

  Notes:
      - newlines are trimmed.
  """
  return get_set(input_file, 1)

def get_set(input_file : str, idx : bool = 0, sep='\t'):
  """
  Generic loader for two-column files.

  Args:
      input_file (str): Path to the file formatted as '{id}\t{title}.
      idx (int): column selector (0 = id, 1 = title).

  Return:
      set[int] | set[str]: A set of ints (idx = 0) or a set of strings (idx = 1).

  Notes:
      - newlines are trimmed.
  """
  data = set()
  with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
      columns = line.split(sep)

      try:
        id = int(columns[0])
        title = str(columns[1])
        tid = int(columns[2]) if len(columns) >= 3 else None
      except ValueError:
        print('value error???')
        continue

      match idx:
        case 0: 
          data.add(id)
        case 1: 
          data.add(title.strip('\n'))
        case 2: 
          data.add(tid)
  return data