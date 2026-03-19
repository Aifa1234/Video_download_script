# --- Standard Library Imports ------------------------------------------------

import argparse       # Parses command-line arguments like --input and --output
import csv            # Reads CSV input files row by row
import logging        # Writes log messages to terminal and log file
import os             # OS-level utilities (not directly used but good practice)
import re             # Regular expressions for cleaning illegal filename characters
import sys            # System utilities — used to exit on critical errors
import time           # Adds delay between retry attempts on failed downloads
from pathlib import Path  # Cross-platform file and folder path handling
import openpyxl           # Reads Microsoft Excel .xlsx files
import requests           # Makes HTTP requests for direct file downloads
from tqdm import tqdm     # Shows a live progress bar in the terminal


# =============================================================================
# SECTION 1: GLOBAL CONFIGURATION
# All tunable constants are defined here at the top for easy access.
# Change these values to adjust script behaviour without editing logic below.
# =============================================================================

DEFAULT_INPUT_FILE = "list.csv"             # Default CSV/XLSX input file name
DEFAULT_OUTPUT_DIR = "./downloaded_videos"  # Default output folder for downloaded videos
MAX_RETRIES        = 3                      # How many times to retry a failed download
RETRY_DELAY        = 5                      # Seconds to wait between each retry attempt
CHUNK_SIZE         = 1024 * 1024            # File download chunk size: 1 MB per chunk
REQUEST_TIMEOUT    = 60                     # Seconds before cancelling an unresponsive request


# =============================================================================
# SECTION 2: LOGGING CONFIGURATION
# Configures dual logging — every message goes to both terminal and log file.
# Format: timestamp  LEVEL     message
# =============================================================================

LOG_FILE = "download_log.txt"              # Log file created in the same folder as the script

logging.basicConfig(                       # Set up the global logging configuration
    level=logging.INFO,                    # Log INFO, WARNING, ERROR, CRITICAL (skip DEBUG)
    format="%(asctime)s  %(levelname)-8s  %(message)s",  # Timestamp + padded level + message
    handlers=[                             # Send log output to two destinations simultaneously
        logging.FileHandler(               # Handler 1: write all logs to a text file on disk
            LOG_FILE,                      # Path to the log file
            encoding="utf-8",             # UTF-8 handles special characters in video titles
        ),
        logging.StreamHandler(             # Handler 2: print all logs to the terminal window
            sys.stdout,                    # sys.stdout = Command Prompt / PowerShell output
        ),
    ],
)

log = logging.getLogger(__name__)          # Create named logger used throughout this script


# =============================================================================
# SECTION 3: HELPER FUNCTION — sanitize_filename()
# Removes characters that are illegal in Windows/macOS/Linux filenames.
# Used to clean both Video IDs and titles before using them as file names.
# =============================================================================

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name)  # Replace each illegal character with underscore
    name = name.strip()                          # Strip leading and trailing whitespace
    name = name.strip(".")                       # Strip leading/trailing dots (invalid on Windows)
    return name or "untitled"                    # Return 'untitled' if name is empty after cleaning


# =============================================================================
# SECTION 4: CORE FUNCTION — read_input_file()
# Opens the input file (.xlsx or .csv), reads all non-empty rows, and returns
# a list of dicts — one per video — with keys: video_id, title, download_link.
# =============================================================================

def read_input_file(path: str) -> list[dict]:
    path = Path(path)   # Convert string path to Path object for easy manipulation
    rows = []           # Empty list to collect all valid video records

    # -------------------------------------------------------------------------
    # BRANCH A: Excel file (.xlsx or .xls)
    # -------------------------------------------------------------------------
    if path.suffix.lower() in (".xlsx", ".xls"):        # Detect Excel file by extension
        wb = openpyxl.load_workbook(path)               # Load the entire Excel workbook into memory
        ws = wb.active                                  # Select the first (active) worksheet

        headers = [                                     # Build list of normalized header names
            str(cell.value).strip().lower()             # Lowercase and strip each header cell value
            if cell.value else ""                       # Use empty string for blank header cells
            for cell in ws[1]                           # Iterate over every cell in row 1 (header row)
        ]

        for row in ws.iter_rows(min_row=2, values_only=True):  # Loop all data rows, skip header row 1
            row_dict = dict(zip(headers, row))                 # Pair each header with its cell value
            if not any(row_dict.values()):                     # Skip row if every cell is empty
                continue
            rows.append({                                       # Append cleaned record to list
                "video_id":      row_dict.get("video id", ""),       # Map 'Video ID' column
                "title":         row_dict.get("video title", ""),    # Map 'Video Title' column
                "download_link": row_dict.get("download url", ""),   # Map 'Download URL' column
            })

    # -------------------------------------------------------------------------
    # BRANCH B: CSV file (.csv)
    # -------------------------------------------------------------------------
    elif path.suffix.lower() == ".csv":                 # Detect CSV file by extension
        with open(path, newline="", encoding="utf-8-sig") as f:  # Open with utf-8-sig to strip Excel BOM
            reader = csv.DictReader(f)                  # Read CSV using first row as column headers
            for row in reader:                          # Loop through each data row
                norm = {                                # Normalize all keys: lowercase + strip spaces
                    k.strip().lower(): v
                    for k, v in row.items()
                }
                if not any(norm.values()):              # Skip entirely empty rows
                    continue
                rows.append({                           # Append cleaned record to list
                    "video_id":      norm.get("video id", ""),       # Map 'Video ID' column
                    "title":         norm.get("video title", ""),    # Map 'Video Title' column
                    "download_link": norm.get("download url", ""),   # Map 'Download URL' column
                })

    # -------------------------------------------------------------------------
    # BRANCH C: Unsupported file format
    # -------------------------------------------------------------------------
    else:
        log.error("Unsupported file format '%s'. Use .xlsx or .csv.", path.suffix)  # Log the bad extension
        sys.exit(1)                                     # Exit immediately — cannot proceed without data

    return rows                                         # Return the complete list of video records


# =============================================================================
# SECTION 5: HELPER FUNCTION — is_youtube_or_vimeo()
# Returns True if the URL is a Vimeo or YouTube watch-page link.
# These URLs cannot be downloaded directly — they require yt-dlp.
# Direct .mp4 URLs return False and are handled by download_direct().
# =============================================================================

def is_youtube_or_vimeo(url: str) -> bool:
    return any(                                         # Return True if any known domain is in the URL
        domain in url                                   # Check if this domain substring exists in the URL
        for domain in ("youtu.be", "youtube.com", "vimeo.com/")  # Known watch-page domains
    )


# =============================================================================
# SECTION 6: DOWNLOAD FUNCTION — download_with_ytdlp()
# Uses yt-dlp to download a Vimeo or YouTube watch-page URL.
# The Video ID is used as the output filename (primary key).
# All errors are logged verbosely — nothing is suppressed.
# =============================================================================

def download_with_ytdlp(url: str, output_path: Path) -> bool:
    try:
        import yt_dlp                                   # Lazy import — only load yt-dlp when needed
    except ImportError:
        log.error("yt-dlp is NOT installed.")           # Log clear error if library missing
        log.error("Fix: run  pip install yt-dlp")       # Log the install command to fix it
        return False                                    # Cannot download without yt-dlp

    ydl_opts = {                                        # Configuration dictionary for yt-dlp
        "outtmpl": str(output_path.with_suffix("")) + ".%(ext)s",  # Filename = VideoID + real extension
        "quiet":        False,                          # Show yt-dlp output — errors visible in terminal
        "no_warnings":  False,                          # Show all warnings — do not suppress any
        "ignoreerrors": False,                          # Raise errors instead of silently skipping
        "retries":      MAX_RETRIES,                    # Number of internal retries yt-dlp will attempt
    }

    try:
        log.info("yt-dlp starting download: %s", url)              # Log start of yt-dlp download
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:                    # Create yt-dlp downloader instance
            info = ydl.extract_info(url, download=True)             # Extract metadata and download video
            if info is None:                                        # No info returned = bad/private URL
                log.error("yt-dlp got no video info — URL may be private, deleted, or invalid.")
                log.error("  URL: %s", url)                         # Log the problematic URL
                return False                                        # Signal failure

        log.info("yt-dlp completed OK: %s", url)                   # Log successful download completion
        return True                                                 # Signal success

    except yt_dlp.utils.DownloadError as exc:                      # yt-dlp raised a download error
        log.error("yt-dlp DownloadError:")                         # Log error category
        log.error("  URL   : %s", url)                             # Log the URL that failed
        log.error("  Reason: %s", exc)                             # Log the exact error message
        return False                                               # Signal failure

    except Exception as exc:                                       # Any other unexpected error
        log.error("yt-dlp unexpected error:")                      # Log error category
        log.error("  URL   : %s", url)                             # Log the URL that failed
        log.error("  Reason: %s", exc)                             # Log the error message
        log.exception("  Full traceback:")                         # Log full Python traceback
        return False                                               # Signal failure


# =============================================================================
# SECTION 7: DOWNLOAD FUNCTION — download_direct()
# Downloads a direct file URL (e.g. ending in .mp4) using HTTP streaming.
# Downloads in 1 MB chunks with a live progress bar.
# Retries up to MAX_RETRIES times on any network error.
# =============================================================================

def download_direct(url: str, output_path: Path) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):           # Try downloading up to MAX_RETRIES times
        try:
            with requests.get(                          # Open a streaming HTTP GET request
                url,                                    # The direct download URL
                stream=True,                            # Stream in chunks — don't buffer entire file
                timeout=REQUEST_TIMEOUT,                # Abort if no response within timeout seconds
            ) as r:
                r.raise_for_status()                    # Raise HTTPError for 4xx / 5xx status codes

                total = int(r.headers.get("Content-Length", 0))  # Get total file size from headers

                with (                                  # Open file for writing AND show progress bar
                    open(output_path, "wb") as f,       # Open output file in binary write mode
                    tqdm(                               # Create progress bar for this download
                        total=total,                    # Total bytes for percentage calculation
                        unit="B",                       # Display unit: Bytes
                        unit_scale=True,                # Auto-scale: B → KB → MB → GB
                        unit_divisor=1024,              # Use 1024 for binary scaling (KiB, MiB)
                        desc=output_path.name[:40],     # Show first 40 chars of filename as label
                        leave=False,                    # Clear bar from terminal after completion
                    ) as bar
                ):
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):  # Read file in 1 MB pieces
                        if chunk:                       # Skip empty keep-alive chunks
                            f.write(chunk)              # Write chunk to output file on disk
                            bar.update(len(chunk))      # Advance progress bar by chunk size

            return True                                 # File fully written — return success

        except requests.RequestException as exc:        # Catch all requests/network errors
            log.warning(                                # Log as warning (not error — retrying)
                "Attempt %d/%d failed — %s",
                attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:                   # If retries remain
                log.warning("Retrying in %d seconds...", RETRY_DELAY)  # Warn about retry delay
                time.sleep(RETRY_DELAY)                 # Wait before next attempt

    log.error("All %d attempts failed for: %s", MAX_RETRIES, url)  # Log total failure after all retries
    return False                                        # All retries exhausted — return failure


# =============================================================================
# SECTION 8: ORCHESTRATOR FUNCTION — download_video()
# Handles the complete download flow for a single video record.
#
# KEY BEHAVIOUR — Video ID as Primary Key:
#   The filename is based ONLY on the Video ID.
#   Example: Video ID 1156438752 → saved as 1156438752.mp4
#   This means:
#     - Duplicate titles never cause skipping
#     - Re-running the script safely skips already-downloaded Video IDs
#     - Every file on disk is uniquely identified by its Video ID
# =============================================================================

def download_video(row: dict, output_dir: Path) -> str:
   
    # --- Extract fields from the row -----------------------------------------

    url    = str(row.get("download_link") or "").strip()   # Get and clean the download URL
    title  = str(row.get("title")         or "").strip()   # Get and clean the video title (for logging)
    vid_id = str(row.get("video_id")      or "").strip()   # Get and clean the Video ID (primary key)

    # --- Validate URL --------------------------------------------------------

    if not url:                                             # If URL is empty or missing
        log.warning("NO URL — video_id='%s' title='%s' — skipping", vid_id, title)  # Log skip reason
        return "no_url"                                     # Return no_url status

    # --- Validate Video ID ---------------------------------------------------

    if not vid_id:                                          # If Video ID is missing
        log.warning("NO VIDEO ID — title='%s' url='%s' — using 'no_id'", title, url)  # Warn about missing ID
        vid_id = "no_id"                                    # Fall back to placeholder ID

    # --- Build Output Filename using Video ID as Primary Key -----------------

    safe_id = sanitize_filename(vid_id)                     # Clean Video ID for safe use as filename

    if is_youtube_or_vimeo(url):                            # Watch-page URL — yt-dlp adds extension
        output_path = output_dir / safe_id                  # e.g. downloaded_videos/1156438752
    else:                                                   # Direct file URL — extract extension
        ext = Path(url.split("?")[0]).suffix or ".mp4"      # Get extension from URL or default to .mp4
        output_path = output_dir / f"{safe_id}{ext}"        # e.g. downloaded_videos/1156438752.mp4

    # --- Skip Check using Video ID as Primary Key ----------------------------

    existing = list(output_dir.glob(f"{safe_id}.*"))        # Look for any file named <VideoID>.*
    if existing:                                            # File with this Video ID already exists
        log.info("SKIP  [ID=%s] already on disk: %s", vid_id, existing[0].name)  # Log skip
        return "skipped"                                    # Return skipped status

    # --- Start Download ------------------------------------------------------

    log.info("START [ID=%s] %s", vid_id, title)             # Log start with Video ID and title
    log.info("  URL      : %s", url)                        # Log the download URL
    log.info("  Saving as: %s", output_path.name)           # Log the exact filename being used

    if is_youtube_or_vimeo(url):                            # Route watch-page URLs to yt-dlp
        success = download_with_ytdlp(url, output_path)     # Download via yt-dlp
    else:                                                   # Route direct URLs to requests
        success = download_direct(url, output_path)         # Download via chunked HTTP

    # --- Log Result ----------------------------------------------------------

    if success:
        log.info("OK    [ID=%s] %s", vid_id, title)         # Log successful download
        return "ok"                                         # Return ok status
    else:
        log.error("FAIL  [ID=%s] %s", vid_id, title)        # Log failed download
        log.error("  URL: %s", url)                         # Log the failed URL for investigation
        return "failed"                                     # Return failed status


# =============================================================================
# SECTION 9: ENTRY POINT — main()
# Parses CLI arguments, runs startup checks, reads the input file,
# loops through all videos, and prints a final summary report.
# =============================================================================

def main():
    # --- Step 1: Argument Parsing --------------------------------------------

    parser = argparse.ArgumentParser(                          # Create CLI argument parser
        description="Econ Engineering — Vimeo Video Bulk Downloader v1.2.0",
    )
    parser.add_argument(                                       # --input argument
        "--input", "-i",
        default=DEFAULT_INPUT_FILE,                            # Default: list.csv
        help=f"Path to input .xlsx or .csv file (default: {DEFAULT_INPUT_FILE})",
    )
    parser.add_argument(                                       # --output argument
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,                            # Default: ./downloaded_videos
        help=f"Folder to save downloaded videos (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()                                 # Parse arguments from command line

    # --- Step 2: Create Output Directory -------------------------------------

    output_dir = Path(args.output)                             # Convert to Path object
    output_dir.mkdir(parents=True, exist_ok=True)              # Create folder (+ parents) if missing

    # --- Step 3: Startup Log -------------------------------------------------

    log.info("=" * 60)
    log.info("  Econ Engineering Video Downloader  v1.2.0")
    log.info("=" * 60)
    log.info("Input       : %s", args.input)                   # Log input file path
    log.info("Output      : %s", output_dir.resolve())         # Log full absolute output path
    log.info("Python      : %s", sys.version)                  # Log Python version
    log.info("Filename Key: Video ID (primary key)")           # Log filename strategy
    log.info("-" * 60)

    # --- Step 4: Library Check -----------------------------------------------
    # Verify all required third-party libraries are installed before starting

    all_ok = True                                              # Track if all libraries are present

    try:
        import requests as _r                                  # Check requests
        log.info("requests : OK  (v%s)", _r.__version__)
    except ImportError:
        log.error("requests : MISSING — run: pip install requests")
        all_ok = False                                         # Mark as not OK

    try:
        import openpyxl as _o                                  # Check openpyxl
        log.info("openpyxl : OK  (v%s)", _o.__version__)
    except ImportError:
        log.error("openpyxl : MISSING — run: pip install openpyxl")
        all_ok = False

    try:
        import tqdm as _t                                      # Check tqdm
        log.info("tqdm     : OK  (v%s)", _t.__version__)
    except ImportError:
        log.error("tqdm     : MISSING — run: pip install tqdm")
        all_ok = False

    try:
        import yt_dlp as _y                                    # Check yt-dlp
        log.info("yt-dlp   : OK  (v%s)", _y.version.__version__)
    except ImportError:
        log.error("yt-dlp   : MISSING — run: pip install yt-dlp")
        all_ok = False

    if not all_ok:                                             # If any library is missing
        log.error("-" * 60)
        log.error("One or more libraries are missing.")
        log.error("Run:  pip install requests openpyxl yt-dlp tqdm")
        log.error("Then re-run the script.")
        sys.exit(1)                                            # Exit — cannot run without libraries

    # --- Step 5: Input File Check --------------------------------------------

    log.info("-" * 60)
    if not Path(args.input).exists():                          # Check input file exists on disk
        log.error("INPUT FILE NOT FOUND: '%s'", args.input)   # Log clear error
        log.error("Make sure '%s' is in the same folder as this script.", args.input)
        sys.exit(1)                                            # Exit — no input file = nothing to do

    # --- Step 6: Read Input File ---------------------------------------------

    rows  = read_input_file(args.input)                        # Parse all video records from file
    total = len(rows)                                          # Count total videos
    log.info("Found %d video(s) to process.", total)           # Log total count
    log.info("=" * 60)

    # --- Step 7: Download Loop -----------------------------------------------

    counts = {"ok": 0, "skipped": 0, "failed": 0, "no_url": 0}  # Result counters

    for i, row in enumerate(rows, start=1):                    # Loop each video (1-indexed)
        log.info("--- (%d / %d) ---", i, total)                # Log progress counter
        result = download_video(row, output_dir)               # Run full download flow for this video
        counts[result] += 1                                    # Increment matching result counter

    # --- Step 8: Final Summary -----------------------------------------------

    log.info("=" * 60)
    log.info("DOWNLOAD COMPLETE — SUMMARY")
    log.info("=" * 60)
    log.info("  Total      : %d", total)                       # All videos in input file
    log.info("  Downloaded : %d", counts["ok"])                # Successfully downloaded
    log.info("  Skipped    : %d", counts["skipped"])           # Already existed on disk
    log.info("  Failed     : %d", counts["failed"])            # Download failed after retries
    log.info("  No URL     : %d", counts["no_url"])            # Rows with no download URL
    log.info("-" * 60)
    log.info("  Output folder : %s", output_dir.resolve())     # Where videos were saved
    log.info("  Log file      : %s", Path(LOG_FILE).resolve()) # Where log was saved
    log.info("=" * 60)


if __name__ == "__main__":   
    main()                  