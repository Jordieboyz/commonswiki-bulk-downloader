# Wikimedia Commons Media Downloader

This project provides a command-line tool to extract and download media files from [Wikimedia commons](https://commons.wikimedia.org/) based on specific categories. 
It is designed to efficiently process extremely large SQL dump files (pages, categories and linktargets) and download media files in parallel while avoiding duplicate downloads.

---

## Features
- Parse Wikimedia Commons SQL dumps to find media files in user-specified categories.
- Track progress across runs to avoid re-processing already scanned categories.
- Download images in parallel using multithreading with configurable workers count.
- Skip already downloaded or invalid files.
- Supports recursive category scanning if needed.
- Store metadata of downloaded files in JSON for easy tracking

## Installation

1. Clone the repository
```bash
git clone <repo_url>
cd <repo_folder>
```
2. Install required Python Packages:
```bash
pip install requests
```
3. Prepare Wikimedia Commons [SQL dumps](https://dumps.wikimedia.org/commonswiki/latest/):  
`commonswiki-latest-categorylinks.sql.gz`    
`commonswiki-latest-page.sql.gz`  
`commonswiki-latest-linktarget.sql.gz`

Place them in a directory (e.g. `./dumps`).

4. Create a category file listing desired categories (one per line), e.g. `categories.txt` 

## Usage
Run the script via the command line:
```bash
python main.py --category-file categories.txt --dumps-dir ./dumps --output-dir ./downloads --workers 10
```

### Arguments
| **Option                  | Short | Type  | Description **                                                   
|---------------------------|-------|-------|------------------------------------------------------------------
| --category-file           | -c    | str   | Path to the file containing desired categories (one per line).   
| --dumps-dir               | -d    | str   | Directory containing Commons SQL dump files.                     
| --output-dir              | -o    | str   | Directory where downloaded images will be saved.                 
| --workers                 | -w    | int   | Number of parallel download threads (default: 10).               
| --no-recursive-search     |       | flag  | Disable recursive search for subcategories.                      


## Program Workflow  
![Workflow Diagram](res/project_workflow.png)  
1. Initialization  
`ProgramContext` sets up paths, loads previous progress and ensures output directories exist.
2. Load Categories  
Categories from the input file are normalized and filtered against previously processed categories.
3. Scan Dumps  
Each SQL dump is scanned sequentially:
- `linktarget_dump` -> maps link targets to titles.
- `category_dump` -> maps categories to link targets.
- `page_dump` -> maps page IDs to media filenames.
4. Retrace files  
Combine results from all dumps to asseciate files with categories.
5. Update Metadata  
`Categorized_file_titles.json` is updated with downloaded or discovered files to prevent reprocessing.
6. Download Media
- Existing files are skipped.
- Failed downloads are logged in `invalid.txt`.
- Downloads run in parallel.

## Notes
- The tool is optimized for very large Wikimedia dumps. Performace depends on available memory and CPU threads
- Ensure network connectivity for downloading media files.