# --- Standard Library Imports ------------------------------------------------

import csv           
import logging        
import os             
import re             
import sys          
import time          
from pathlib import Path  
import openpyxl           
import requests           
from tqdm import tqdm     


# =============================================================================
# SECTION 1: GLOBAL CONFIGURATION
# =============================================================================

DEFAULT_INPUT_FILE = "list.csv"             # Default CSV/XLSX input file name
DEFAULT_OUTPUT_DIR = "./downloaded_videos"  # Default output folder for downloaded videos
MAX_RETRIES        = 3                      
RETRY_DELAY        = 5                   
CHUNK_SIZE         = 1024 * 1024            
REQUEST_TIMEOUT    = 60                     


# =============================================================================
# SECTION 2: LOGGING CONFIGURATION
# Format: timestamp  LEVEL     message
# =============================================================================

LOG_FILE = "download_log.txt"              # Log file created in the same folder as the script

logging.basicConfig(                       
    level=logging.INFO,                   
    format="%(asctime)s  %(levelname)-8s  %(message)s",  # Timestamp + padded level + message
    handlers=[                             
        logging.FileHandler(               
            LOG_FILE,                     
            encoding="utf-8",            
        ),
        logging.StreamHandler(            
            sys.stdout,                    
        ),
    ],
)

log = logging.getLogger(__name__)          

# =============================================================================
# SECTION 3: HELPER FUNCTION — sanitize_filename()
# Removes characters that are illegal in Windows/macOS/Linux filenames.
# Used to clean both Video IDs and titles before using them as file names.
# =============================================================================

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = name.strip()                          
    name = name.strip(".")                     
    return name or "untitled"                   


# =============================================================================
# SECTION 4: CORE FUNCTION — read_input_file()
# Opens the input file (.xlsx or .csv), reads all non-empty rows, and returns
# a list of dicts — one per video — with keys: video_id, title, download_link.
# =============================================================================

def read_input_file(path: str) -> list[dict]:
    path = Path(path)   
    rows = []         


    if path.suffix.lower() in (".xlsx", ".xls"):       
        wb = openpyxl.load_workbook(path)               
        ws = wb.active                                 

        headers = [                                     
            str(cell.value).strip().lower()             
            if cell.value else ""                       
            for cell in ws[1]                          
        ]

        for row in ws.iter_rows(min_row=2, values_only=True):  
            row_dict = dict(zip(headers, row))                 
            if not any(row_dict.values()):                    
                continue
            rows.append({                                      
                "video_id":      row_dict.get("video id", ""),       
                "title":         row_dict.get("video title", ""),   
                "download_link": row_dict.get("download url", ""),  
            })

    elif path.suffix.lower() == ".csv":                
        with open(path, newline="", encoding="utf-8-sig") as f:  
            reader = csv.DictReader(f)                
            for row in reader:                         
                norm = {                               
                    k.strip().lower(): v
                    for k, v in row.items()
                }
                if not any(norm.values()):           
                    continue
                rows.append({                         
                    "video_id":      norm.get("video id", ""),      
                    "title":         norm.get("video title", ""),    
                    "download_link": norm.get("download url", ""),
                })

    else:
        log.error("Unsupported file format '%s'. Use .xlsx or .csv.", path.suffix)  
        sys.exit(1)                                     

    return rows                                         


# =============================================================================
# SECTION 5: HELPER FUNCTION — is_youtube_or_vimeo()
# Returns True if the URL is a Vimeo or YouTube watch-page link.
# Direct .mp4 URLs return False and are handled by download_direct().
# =============================================================================

def is_youtube_or_vimeo(url: str) -> bool:
    return any(                                      
        domain in url                                  
        for domain in ( "vimeo.com/")  
    )


# =============================================================================
# SECTION 6: DOWNLOAD FUNCTION — download_with_ytdlp()
# Uses yt-dlp to download a Vimeo or YouTube watch-page URL.
# The Video ID is used as the output filename (primary key).
# All errors are logged verbosely — nothing is suppressed.
# =============================================================================

def download_with_ytdlp(url: str, output_path: Path) -> bool:
    try:
        import yt_dlp                                   
    except ImportError:
        log.error("yt-dlp is NOT installed.")           
        log.error("Fix: run  pip install yt-dlp")       
        return False                                    

    ydl_opts = {                                        
        "outtmpl": str(output_path.with_suffix("")) + ".%(ext)s",
        "quiet":        False,                          
        "no_warnings":  False,                          
        "ignoreerrors": False,                         
        "retries":      MAX_RETRIES,                  
    }

    try:
        log.info("yt-dlp starting download: %s", url)            
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:               
            info = ydl.extract_info(url, download=True)        
            if info is None:                                      
                log.error("yt-dlp got no video info — URL may be private, deleted, or invalid.")
                log.error("  URL: %s", url)                     
                return False                                   

        log.info("yt-dlp completed OK: %s", url)                
        return True                                             

    except yt_dlp.utils.DownloadError as exc:                   
        log.error("yt-dlp DownloadError:")                         
        log.error("  URL   : %s", url)                             
        log.error("  Reason: %s", exc)                            
        return False                                               

    except Exception as exc:                                      
        log.error("yt-dlp unexpected error:")                      
        log.error("  URL   : %s", url)                             
        log.error("  Reason: %s", exc)                             
        log.exception("  Full traceback:")                        
        return False                                               


# =============================================================================
# SECTION 7: DOWNLOAD FUNCTION — download_direct()
# Downloads a direct file URL (e.g. ending in .mp4) using HTTP streaming.
# Downloads in 1 MB chunks with a live progress bar.
# Retries up to MAX_RETRIES times on any network error.
# =============================================================================

def download_direct(url: str, output_path: Path) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):          
        try:
            with requests.get(                          
                url,                                    
                stream=True,                          
                timeout=REQUEST_TIMEOUT,                
            ) as r:
                r.raise_for_status()                    

                total = int(r.headers.get("Content-Length", 0)) 

                with (                                
                    open(output_path, "wb") as f,       
                    tqdm(                             
                        total=total,                 
                        unit="B",                    
                        unit_scale=True,               
                        unit_divisor=1024,              
                        desc=output_path.name[:40],   
                        leave=False,                    
                    ) as bar
                ):
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:                   
                            f.write(chunk)         
                            bar.update(len(chunk))    

            return True                                 

        except requests.RequestException as exc:      
            log.warning(                               
                "Attempt %d/%d failed — %s",
                attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:                  
                log.warning("Retrying in %d seconds...", RETRY_DELAY) 
                time.sleep(RETRY_DELAY)                

    log.error("All %d attempts failed for: %s", MAX_RETRIES, url) 
    return False                                        


# =============================================================================
# SECTION 8: ORCHESTRATOR FUNCTION — download_video()
# Handles the complete download flow for a single video record.
# =============================================================================

def download_video(row: dict, output_dir: Path) -> str:
   
    # --- Extract fields from the row -----------------------------------------

    url    = str(row.get("download_link") or "").strip()   
    title  = str(row.get("title")         or "").strip()   
    vid_id = str(row.get("video_id")      or "").strip()   

    # --- Validate URL --------------------------------------------------------

    if not url:                                             
        log.warning("NO URL — video_id='%s' title='%s' — skipping", vid_id, title)  
        return "no_url"                                     

    # --- Validate Video ID ---------------------------------------------------

    if not vid_id:                                         
        log.warning("NO VIDEO ID — title='%s' url='%s' — using 'no_id'", title, url) 
        vid_id = "no_id"                                 

    # --- Build Output Filename using Video ID as Primary Key -----------------

    safe_id = sanitize_filename(vid_id)                     

    if is_youtube_or_vimeo(url):                            
        output_path = output_dir / safe_id                  
    else:                                              
        ext = Path(url.split("?")[0]).suffix or ".mp
        output_path = output_dir / f"{safe_id}{ext}"      

    # --- Skip Check using Video ID as Primary Key ----------------------------

    existing = list(output_dir.glob(f"{safe_id}.*"))    
    if existing:                                            
        log.info("SKIP  [ID=%s] already on disk: %s", vid_id, existing[0].name) 
        return "skipped"                                    

    # --- Start Download ------------------------------------------------------

    log.info("START [ID=%s] %s", vid_id, title)          
    log.info("  URL      : %s", url)                        
    log.info("  Saving as: %s", output_path.name)           

    if is_youtube_or_vimeo(url):                         
        success = download_with_ytdlp(url, output_path)  
    else:                                                   
        success = download_direct(url, output_path)         

    # --- Log Result ----------------------------------------------------------

    if success:
        log.info("OK    [ID=%s] %s", vid_id, title)         
        return "ok"                                         
    else:
        log.error("FAIL  [ID=%s] %s", vid_id, title)        
        log.error("  URL: %s", url)                         
        return "failed"                                   


# =============================================================================
# SECTION 9: ENTRY POINT — main()
# Parses CLI arguments, runs startup checks, reads the input file,
# loops through all videos, and prints a final summary report.
# =============================================================================

def main():
    # ---  Argument Parsing --------------------------------------------

    parser = argparse.ArgumentParser(                          
        description="Econ Engineering — Vimeo Video Bulk Downloader v1.2.0",
    )
    parser.add_argument(                                       
        "--input", "-i",
        default=DEFAULT_INPUT_FILE,                           
        help=f"Path to input .xlsx or .csv file (default: {DEFAULT_INPUT_FILE})",
    )
    parser.add_argument(                                      
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,                           
        help=f"Folder to save downloaded videos (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()                            

    # ---  Create Output Directory -------------------------------------

    output_dir = Path(args.output)                           
    output_dir.mkdir(parents=True, exist_ok=True)              

    # ---  Startup Log -------------------------------------------------

    log.info("=" * 60)
    log.info("  Econ Engineering Video Downloader  v1.2.0")
    log.info("=" * 60)
    log.info("Input       : %s", args.input)                   
    log.info("Output      : %s", output_dir.resolve())        
    log.info("Python      : %s", sys.version)                  
    log.info("Filename Key: Video ID (primary key)")           
    log.info("-" * 60)

    # --- Library Check -----------------------------------------------

    all_ok = True                                           

    try:
        import requests as _r                                  
        log.info("requests : OK  (v%s)", _r.__version__)
    except ImportError:
        log.error("requests : MISSING — run: pip install requests")
        all_ok = False                                         

    try:
        import openpyxl as _o                                  
        log.info("openpyxl : OK  (v%s)", _o.__version__)
    except ImportError:
        log.error("openpyxl : MISSING — run: pip install openpyxl")
        all_ok = False

    try:
        import tqdm as _t                                      
        log.info("tqdm     : OK  (v%s)", _t.__version__)
    except ImportError:
        log.error("tqdm     : MISSING — run: pip install tqdm")
        all_ok = False

    try:
        import yt_dlp as _y                                  
        log.info("yt-dlp   : OK  (v%s)", _y.version.__version__)
    except ImportError:
        log.error("yt-dlp   : MISSING — run: pip install yt-dlp")
        all_ok = False

    if not all_ok:                                            
        log.error("-" * 60)
        log.error("One or more libraries are missing.")
        log.error("Run:  pip install requests openpyxl yt-dlp tqdm")
        log.error("Then re-run the script.")
        sys.exit(1)                                          

    # --- Input File Check --------------------------------------------

    log.info("-" * 60)
    if not Path(args.input).exists():                         
        log.error("INPUT FILE NOT FOUND: '%s'", args.input)   
        log.error("Make sure '%s' is in the same folder as this script.", args.input)
        sys.exit(1)                                           

    # ---  Read Input File ---------------------------------------------

    rows  = read_input_file(args.input)                        
    total = len(rows)                                        
    log.info("Found %d video(s) to process.", total)           
    log.info("=" * 60)

    # ---  Download Loop -----------------------------------------------

    counts = {"ok": 0, "skipped": 0, "failed": 0, "no_url": 0}

    for i, row in enumerate(rows, start=1):                   
        log.info("--- (%d / %d) ---", i, total)                
        result = download_video(row, output_dir)               
        counts[result] += 1                                  

    # ---  Final Summary -----------------------------------------------

    log.info("=" * 60)
    log.info("DOWNLOAD COMPLETE — SUMMARY")
    log.info("=" * 60)
    log.info("  Total      : %d", total)                      
    log.info("  Downloaded : %d", counts["ok"])            
    log.info("  Skipped    : %d", counts["skipped"])           
    log.info("  Failed     : %d", counts["failed"])          
    log.info("  No URL     : %d", counts["no_url"])          
    log.info("-" * 60)
    log.info("  Output folder : %s", output_dir.resolve())     
    log.info("  Log file      : %s", Path(LOG_FILE).resolve()) 
    log.info("=" * 60)


if __name__ == "__main__":   
    main()                  