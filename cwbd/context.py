from dataclasses import dataclass, field
from pathlib import Path

from .download_utils import get_json_data

@dataclass
class ProgramContext:
  """
  Central runtime context holding configuration, paths and state shared across all program phases.

  Args:
      dumps_dir (Path): Directory containing Wikimedia Commons SQL dump files.
      output_dir (Path): Directory where downloaded images will be stored.

  Notes:
      - This object is intented to be passed across scanner and donwload modules.
      - Most mutable fields are used to track prograss and resume interrupted runs.

  """
  dump_dir : Path
  output_dir : Path

  page_dump: Path = field(init=False)
  category_dump: Path = field(init=False)
  linktarget_dump: Path = field(init=False)

  checkpoint_dir : Path = field(init=False)
  progress_scanner : Path = field(init=False)

  program_set : set = field(default_factory=set)
  input_categories : set = field(default_factory=set)
  process_categories : set = field(default_factory=set)
  
  pfiles : dict =  field(default_factory=dict)

  save_interval : int = 100
  max_workers : int = field(default_factory=int)

  recursive_search : bool = field(default_factory=bool)
  max_phase_matches = 0

  found_files : Path = field(init=False)
  invalid_files : Path = field(init=False)

  downloads_set : set = field(default_factory=set)
  failed_downloads_set : set = field(default_factory=set)

  def __post_init__(self):
    """
    Initialize derived paths, checkpoint files and scanner state after dataclass construction.

    Notes:
        - Validates precence of required dump files
        - Creates checkpoint directory if it does not exist.
        - Initializes per-phase scan output files.
        - Resets scanner progress when category input has changed significantly.
    """
    # setup for fetching system
    self.page_dump = self.dump_dir / 'commonswiki-latest-page.sql.gz'
    self.category_dump = self.dump_dir / 'commonswiki-latest-categorylinks.sql.gz'
    self.linktarget_dump = self.dump_dir / 'commonswiki-latest-linktarget.sql.gz'

    self.checkpoint_dir = Path('checkpoint')
    self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    self.progress_scanner = self.checkpoint_dir / 'progress_scanner.txt'

    self.init_program_files()

    # setup for download system
    self.found_files = Path('Categorized_file_titles.json')
    self.invalid_files = self.output_dir / 'invalid.txt'

    self.reset_scanner()

  def init_program_files(self):
    """"
    Initialize output files for each SQL dump scan phase.

    Notes:
        - Each dump file is mapped to a corresponding scan output file.
        - Output files are stored inside the checkpoint directory.
        - Abort initialization if any of the required dump files is missing.
    """
    for path in (self.linktarget_dump, self.category_dump, self.page_dump):
      if not path.is_file():
        print(f'[ERROR] Missing Required file {path}')
        return None
      else:
        base =  Path(path).name.split('.')[0]
        outfile = f'{base[base.rfind("-") +1:]}_scan_output.txt'
        self.pfiles[path] = self.checkpoint_dir / outfile
  
  def reset_scanner(self):
    """"
    Reset scanner progress when input categories differ from previous runs.

    Notes:
        - Scanner progress is preserved if categories are unchanged.
        - Progress is reset is new categories are detected.
        - Existing scan output files are deleted when a reset occurs.
    """
    # reset on new category search, keep on resuming previous category input
    with open(self.progress_scanner, 'a+', encoding='utf-8') as f:
      # since we open in 'a+' we need to reset the pointer to check the first line
      f.seek(0)
      first_line = f.readline().rstrip('\n')
      existing = set(x.strip() for x in first_line.split(',')) if first_line else set()
      
      found_cats = get_json_data(self.found_files)
      if not self.input_categories.issubset(existing) and not all(cat in found_cats for cat in self.input_categories):
        f.truncate(0)
        f.write(",".join(sorted(self.input_categories))+'\n')

        try:
          self.program_files[self.linktarget_dump].unlink()
          self.program_files[self.category_dump].unlink()
          self.program_files[self.page_dump].unlink()
        except:
          pass

  @property
  def program_files(self):
    if not self.pfiles:
      self.init_program_files()
    return self.pfiles