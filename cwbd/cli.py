import argparse
import sys
import os

# TODO: seperate module using command like fetch, download and clean

def get_cli_input():
  """
  Parse and validate command-line arguments for the Wikimedia Commons image Downloader.
  
  Args:
      None
  
  Return:
      argparse.Namespace:
          Parsed command line arguments containing:
            - category_file (str): Path to file listing target Wikimedia Commons categories.
            - dumps_dir (str): Directory containing Wikimedia Commons SQL dump files.
            - output_dir (str): Directory where downloaded images will be stored.
            - workers (int): Number of parallel download threads.
            - no_recursive_search (bool): Wether to disable recursive subcategory traversal.
  
  Notes:
      - The function terminates the program if required files or directories do not exist.
      - This function should be called once at program startup
  """
  parser = argparse.ArgumentParser(
    prog="cwbd",
    description="Commons Wikimedia Bulk Donwloader (SQL dump based)"
  )
  
  subparsers = parser.add_subparsers(
    dest="command",
    required=True
  )

  # -------------------------------------------------------------------
  # Fetch
  # -------------------------------------------------------------------
  fetch = subparsers.add_parser(
    "fetch", 
    help="Scan SQL dumps and build media index"
  )

  fetch.add_argument("--category-file","-c", type=str,  required=True, 
                      help="File that holds the desired categories" )
  
  fetch.add_argument("--dumps-dir",    "-d", type=str,  required=True, 
                      help="Directory containing Commons SQL dump files")
  
  fetch.add_argument("--no-recursive-search", action='store_false',  
                      help="Prevent program to recursively obtain all media files in subcategories")

  # -------------------------------------------------------------------
  # Download
  # -------------------------------------------------------------------
  download = subparsers.add_parser(
    "download",
    help="Download media files from previously fetched index"
  )

  download.add_argument("--category-file","-c", type=str,  required=True, 
                        help="File that holds the desired categories" )

  download.add_argument("--output-dir",   "-o", type=str,  required=True, 
                      help="Directory to store downloaded images")
  
  download.add_argument("--workers",      "-w", type=int,  default=10,    
                      help="Number of parallel download threads")
  
  # -------------------------------------------------------------------
  # Clean
  # -------------------------------------------------------------------
  clean = subparsers.add_parser(
    "clean",
    help="Remove generated folders and files used during fetch or download phase."
  )

  args = parser.parse_args()

  if args.command != "clean":
    if not os.path.isfile(args.category_file):
      print(f'[ERROR] File does not exist: {args.category_file}', file=sys.stderr)
      sys.exit(1)

  if args.commadn == "fetch":
    if not os.path.isdir(args.dumps_dir):
      print(f'[ERROR] Directory does not exist: {args.dumps_dir}', file=sys.stderr)
      sys.exit(1)
  
  return args