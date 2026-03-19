# =============================================================================
# PROJECT   : Vimeo Video Bulk Downloader
# TEAM      : Econ Engineering Team
# PURPOSE   : Migrate ~5,000 Vimeo video files to local / external storage
# VERSION   : 1.0.0
# DATE      : March 2026
# =============================================================================
#
# DESCRIPTION:
#   This script automates the bulk downloading of video files from Vimeo.
#   It reads a structured Excel (.xlsx) or CSV file containing video metadata
#   and download URLs, then downloads each file to a specified output directory
#   (local drive or external hard drive).
#
# FEATURES:
#   - Reads input from .xlsx or .csv files
#   - Downloads direct file URLs using the requests library
#   - Downloads Vimeo / YouTube watch-page URLs using yt-dlp
#   - Skips already-downloaded files (safe to re-run overnight)
#   - Retries failed downloads up to 3 times
#   - Logs all activity to terminal and download_log.txt
#   - Displays live progress bar per file
#   - Prints a summary report at the end
#
# USAGE:
#   python download_videos.py --input videos.xlsx --output "E:\Vimeo_Archive"
#
# REQUIREMENTS:
#   pip install requests openpyxl yt-dlp tqdm
# =============================================================================


# --- Standard Library Imports ------------------------------------------------

import argparse       # Parses command-line arguments (--input, --output)
import logging        # Handles writing log messages to file and terminal
import os             # Provides OS-level file and directory utilities
import re             # Regular expressions — used for filename sanitization
import sys            # System utilities — used to exit script on critical errors
import time           # Used to add delay between retry attempts
from pathlib import Path  # Object-oriented file path handling (cross-platform)

# --- Third-Party Library Imports ---------------------------------------------

import openpyxl           # Reads Microsoft Excel (.xlsx) files
import requests           # Makes HTTP requests to download files from URLs
from tqdm import tqdm     # Displays a live progress bar in the terminal


# =============================================================================
# SECTION 1: GLOBAL CONFIGURATION
# These constants control the behaviour of the script.
# Change these values here to adjust retry count, chunk size, etc.
# =============================================================================

DEFAULT_INPUT_FILE = "videos.xlsx"          # Default input file if --input not provided
DEFAULT_OUTPUT_DIR = "./downloaded_videos"  # Default output folder if --output not provided
MAX_RETRIES        = 3                      # Number of times to retry a failed download
RETRY_DELAY        = 5                      # Seconds to wait between retry attempts
CHUNK_SIZE         = 1024 * 1024            # Download chunk size: 1 MB per chunk
REQUEST_TIMEOUT    = 60                     # Seconds before an HTTP request times out


# =============================================================================
# SECTION 2: LOGGING CONFIGURATION
# Sets up dual logging: messages go to both the terminal and a log file.
# =============================================================================

LOG_FILE = "download_log.txt"              # Name of the log file to be created on disk

logging.basicConfig(                       # Configure the root logger with global settings
    level=logging.INFO,                    # Minimum level: INFO and above (DEBUG is ignored)
    format="%(asctime)s  %(levelname)-8s  %(message)s",  # Log line: timestamp + level + message
    handlers=[                             # List of destinations where logs are sent
        logging.FileHandler(               # Handler 1: writes log messages to a file
            LOG_FILE,                      # The file to write logs into
            encoding="utf-8"              # UTF-8 encoding supports special characters in titles
        ),
        logging.StreamHandler(             # Handler 2: writes log messages to the terminal
            sys.stdout                     # sys.stdout = standard output (Command Prompt / terminal)
        ),
    ],
)

log = logging.getLogger(__name__)          # Create a logger instance for use throughout this script


# =============================================================================
# SECTION 3: HELPER FUNCTION — sanitize_filename()
# Cleans a video title so it can be safely used as a filename on any OS.
# =============================================================================

def sanitize_filename(name: str) -> str:
    """
    Remove or replace characters that are illegal in filenames.
    Works safely on Windows, macOS, and Linux.

    Args:
        name (str): Raw video title from the input file.

    Returns:
        str: A cleaned string safe to use as a filename.
    """
    name = re.sub(r'[\\/*?:"<>|]', "_", name)  # Replace illegal filename characters with underscore
    name = name.strip()                          # Remove leading and trailing whitespace
    name = name.strip(".")                       # Remove leading/trailing dots (illegal on Windows)
    return name or "untitled"                    # If name is empty after cleaning, fall back to 'untitled'


# =============================================================================
# SECTION 4: CORE FUNCTION — read_input_file()
# Reads the Excel or CSV input file and returns a list of video records.
# =============================================================================

def read_input_file(path: str) -> list[dict]:
    """
    Read the input file (.xlsx or .csv) and return a list of video records.
    Each record is a dict with keys: video_id, title, download_link.

    Args:
        path (str): File path to the input Excel or CSV file.

    Returns:
        list[dict]: List of video records extracted from the file.
    """
    path = Path(path)    # Convert the string path to a Path object for easier manipulation
    rows = []            # Initialize an empty list to collect all video records

    if path.suffix.lower() in (".xlsx", ".xls"):     # Check if the input file is an Excel file
        wb = openpyxl.load_workbook(path)             # Open and load the Excel workbook from disk
        ws = wb.active                                # Select the active (first) worksheet

        # Read the first row as column headers, normalize to lowercase with no extra spaces
        headers = [
            str(cell.value).strip().lower()           # Convert each header cell to lowercase string
            if cell.value else ""                     # Use empty string if the cell is blank
            for cell in ws[1]                         # Iterate over every cell in the first row
        ]

        for row in ws.iter_rows(min_row=2, values_only=True):    # Loop through all rows (skip header row 1)
            row_dict = dict(zip(headers, row))                   # Map each header to its cell value
            if not any(row_dict.values()):                       # Skip the row if all cells are empty
                continue
            rows.append({                                         # Add a cleaned record dict to the list
                "video_id":      row_dict.get("video id", ""),        # Extract the Video ID column
                "title":         row_dict.get("video title", ""),     # Extract the Video Title column
                "download_link": row_dict.get("download link", ""),   # Extract the Download Link column
            })

    elif path.suffix.lower() == ".csv":               # Check if the input file is a CSV file
        import csv                                    # Import csv module (only needed for CSV path)
        with open(path, newline="", encoding="utf-8-sig") as f:   # Open CSV; utf-8-sig handles Excel BOM
            reader = csv.DictReader(f)                # Read CSV as dictionary using first row as headers
            for row in reader:                        # Loop through each data row in the CSV
                norm = {                              # Normalize all header keys to lowercase
                    k.strip().lower(): v              # Strip spaces and lowercase each key
                    for k, v in row.items()           # Iterate over all key-value pairs in the row
                }
                if not any(norm.values()):            # Skip the row if all values are empty
                    continue
                rows.append({                         # Add a cleaned record dict to the list
                    "video_id":      norm.get("video id", ""),        # Extract the Video ID column
                    "title":         norm.get("video title", ""),     # Extract the Video Title column
                    "download_link": norm.get("download link", ""),   # Extract the Download Link column
                })

    else:                                             # File format is not supported
        log.error("Unsupported file format: %s", path.suffix)   # Log an error with the bad extension
        sys.exit(1)                                   # Exit the script immediately with error code 1

    return rows                                       # Return the full list of video records


# =============================================================================
# SECTION 5: HELPER FUNCTION — is_youtube_or_vimeo()
# Detects if a URL is a watch-page link (needs yt-dlp) vs a direct file URL.
# =============================================================================

def is_youtube_or_vimeo(url: str) -> bool:
    """
    Check if a URL is a Vimeo or YouTube watch-page link.
    These cannot be downloaded directly and require yt-dlp.

    Args:
        url (str): The download URL from the input file.

    Returns:
        bool: True if yt-dlp is needed, False if direct download is possible.
    """
    return any(                                        # Return True if any domain string is found in the URL
        d in url                                       # Check if domain string d exists within the URL
        for d in ("youtu.be", "youtube.com", "vimeo.com/")  # Domains that require yt-dlp
    )


# =============================================================================
# SECTION 6: DOWNLOAD FUNCTION — download_with_ytdlp()
# Downloads a Vimeo or YouTube watch-page URL using the yt-dlp library.
# =============================================================================

def download_with_ytdlp(url: str, output_path: Path) -> bool:
    """
    Download a video from a Vimeo or YouTube watch-page URL using yt-dlp.

    Args:
        url (str): The Vimeo or YouTube watch-page URL.
        output_path (Path): The desired output file path (without extension).

    Returns:
        bool: True if download succeeded, False if it failed.
    """
    try:
        import yt_dlp                                  # Import yt-dlp (lazy import — only loaded when needed)
    except ImportError:                                # Handle case where yt-dlp is not installed
        log.error("yt-dlp is not installed. Run: pip install yt-dlp")  # Log a helpful error message
        return False                                   # Return False to signal download failure

    ydl_opts = {                                       # Dictionary of options passed to yt-dlp
        "outtmpl": str(output_path.with_suffix("")) + ".%(ext)s",  # Output filename — yt-dlp adds extension
        "quiet": True,                                 # Suppress yt-dlp's own verbose output
        "no_warnings": True,                           # Suppress yt-dlp warning messages
        "retries": MAX_RETRIES,                        # Tell yt-dlp to retry failed downloads internally
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:       # Create a YoutubeDL instance with our options
            ydl.download([url])                        # Start the download — pass URL as a list
        return True                                    # Return True to signal successful download

    except Exception as exc:                           # Catch any exception that yt-dlp raises
        log.error("yt-dlp failed for %s: %s", url, exc)   # Log the URL and the error message
        return False                                   # Return False to signal download failure


# =============================================================================
# SECTION 7: DOWNLOAD FUNCTION — download_direct()
# Downloads a direct file URL using the requests library with retry logic.
# =============================================================================

def download_direct(url: str, output_path: Path) -> bool:
    """
    Download a direct file URL using HTTP streaming with a progress bar.
    Retries up to MAX_RETRIES times on failure.

    Args:
        url (str): Direct file download URL (e.g. ending in .mp4).
        output_path (Path): Full path where the file will be saved.

    Returns:
        bool: True if download succeeded, False if all retries failed.
    """
    for attempt in range(1, MAX_RETRIES + 1):          # Loop from attempt 1 to MAX_RETRIES (inclusive)
        try:
            with requests.get(                         # Open an HTTP GET request to the URL
                url,                                   # The direct download URL
                stream=True,                           # Stream response — don't load entire file into memory
                timeout=REQUEST_TIMEOUT                # Cancel if server doesn't respond in time
            ) as r:
                r.raise_for_status()                   # Raise exception if HTTP status is 4xx or 5xx

                total = int(                           # Get the total file size in bytes for progress bar
                    r.headers.get("Content-Length", 0) # Read Content-Length header; default 0 if missing
                )

                with open(output_path, "wb") as f, tqdm(    # Open output file AND show progress bar together
                    total=total,                             # Total file size for progress calculation
                    unit="B",                                # Unit label shown in progress bar
                    unit_scale=True,                         # Auto-scale units (B → KB → MB)
                    unit_divisor=1024,                       # Use 1024 for binary file size scaling
                    desc=output_path.name[:40],              # Show first 40 chars of filename as bar label
                    leave=False,                             # Remove progress bar after completion
                ) as bar:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):  # Read file in CHUNK_SIZE byte pieces
                        if chunk:                            # Only process non-empty chunks
                            f.write(chunk)                   # Write the chunk to the output file on disk
                            bar.update(len(chunk))           # Advance progress bar by size of this chunk

            return True                                # All chunks written successfully — return True

        except requests.RequestException as exc:       # Catch any network or HTTP error
            log.warning(                               # Log a warning (not error — we will retry)
                "Attempt %d/%d failed for %s: %s",    # Message template with placeholders
                attempt, MAX_RETRIES, url, exc         # Values for each placeholder
            )
            if attempt < MAX_RETRIES:                  # If we still have retries remaining
                time.sleep(RETRY_DELAY)                # Wait RETRY_DELAY seconds before next attempt

    return False                                       # All retry attempts exhausted — return False


# =============================================================================
# SECTION 8: ORCHESTRATOR FUNCTION — download_video()
# Controls the full download flow for a single video row.
# =============================================================================

def download_video(row: dict, output_dir: Path) -> str:
    """
    Handle the complete download process for one video record.
    Determines URL type, checks for existing files, and routes
    to the correct download function.

    Args:
        row (dict): A single video record with keys: video_id, title, download_link.
        output_dir (Path): The directory where the video file will be saved.

    Returns:
        str: Result status — one of 'ok', 'skipped', 'failed', or 'no_url'.
    """
    url    = str(row.get("download_link") or "").strip()    # Extract and clean the download URL
    title  = str(row.get("title") or "untitled").strip()    # Extract and clean the video title
    vid_id = str(row.get("video_id") or "").strip()         # Extract and clean the video ID

    if not url:                                              # Check if the URL is empty or missing
        log.warning(                                         # Log a warning for this row
            "No URL for video_id=%s title=%s — skipping",   # Warning message template
            vid_id, title                                    # Values for the placeholders
        )
        return "no_url"                                      # Return 'no_url' status and skip this video

    safe_title = sanitize_filename(title)                    # Clean the title to make it a valid filename

    if is_youtube_or_vimeo(url):                             # Check if this is a watch-page URL
        output_path = output_dir / f"{safe_title}"           # yt-dlp will auto-add the correct extension
    else:
        ext = Path(url.split("?")[0]).suffix or ".mp4"       # Extract extension from URL; default to .mp4
        output_path = output_dir / f"{safe_title}{ext}"      # Build full output path with extension

    existing = list(output_dir.glob(f"{safe_title}.*"))      # Search for any existing file with this title
    if existing:                                             # If a matching file already exists on disk
        log.info("SKIP  already exists: %s", existing[0].name)  # Log that we are skipping this file
        return "skipped"                                     # Return 'skipped' status

    log.info("START [%s] %s", vid_id, title)                 # Log that we are starting this download

    if is_youtube_or_vimeo(url):                             # Route to yt-dlp for watch-page URLs
        success = download_with_ytdlp(url, output_path)      # Attempt download via yt-dlp
    else:                                                    # Route to requests for direct file URLs
        success = download_direct(url, output_path)          # Attempt direct chunked download

    if success:                                              # Check if the download completed successfully
        log.info("OK    [%s] %s", vid_id, title)             # Log success
        return "ok"                                          # Return 'ok' status
    else:
        log.error("FAIL  [%s] %s  url=%s", vid_id, title, url)  # Log failure with URL for debugging
        return "failed"                                      # Return 'failed' status


# =============================================================================
# SECTION 9: ENTRY POINT — main()
# Parses arguments, reads the input file, runs all downloads, prints summary.
# =============================================================================

def main():
    """
    Main entry point for the script.
    Parses CLI arguments, reads the input file, downloads all videos,
    and prints a final summary report.
    """

    # --- Argument Parser Setup -----------------------------------------------

    parser = argparse.ArgumentParser(                         # Create a command-line argument parser
        description="Download Vimeo videos listed in an XLSX or CSV file."  # Shown in --help output
    )
    parser.add_argument(                                      # Define the --input argument
        "--input", "-i",                                      # Long and short flag names
        default=DEFAULT_INPUT_FILE,                           # Use default if not provided by user
        help=f"Path to input XLSX or CSV file (default: {DEFAULT_INPUT_FILE})",  # Help text
    )
    parser.add_argument(                                      # Define the --output argument
        "--output", "-o",                                     # Long and short flag names
        default=DEFAULT_OUTPUT_DIR,                           # Use default if not provided by user
        help=f"Destination folder for downloaded videos (default: {DEFAULT_OUTPUT_DIR})",  # Help text
    )
    args = parser.parse_args()                                # Parse the actual arguments from command line

    # --- Output Directory Setup ----------------------------------------------

    output_dir = Path(args.output)                            # Convert output path string to a Path object
    output_dir.mkdir(parents=True, exist_ok=True)             # Create the folder (and parents) if not exists

    # --- Startup Log ---------------------------------------------------------

    log.info("=" * 60)                                        # Print a separator line to the log
    log.info("Econ Engineering Video Downloader")             # Log the project name
    log.info("Input  : %s", args.input)                       # Log the input file path
    log.info("Output : %s", output_dir.resolve())             # Log the full absolute output path
    log.info("=" * 60)                                        # Print another separator line

    # --- Read Input File -----------------------------------------------------

    rows  = read_input_file(args.input)                       # Read all video records from the input file
    total = len(rows)                                         # Count total number of videos to process
    log.info("Found %d video(s) in input file.", total)       # Log how many videos were found

    # --- Download Loop -------------------------------------------------------

    counts = {                                                # Dictionary to track result counts
        "ok":      0,                                         # Counter for successful downloads
        "skipped": 0,                                         # Counter for skipped (already exists) files
        "failed":  0,                                         # Counter for failed downloads
        "no_url":  0,                                         # Counter for rows with no URL
    }

    for i, row in enumerate(rows, start=1):                   # Loop through each video record (1-indexed)
        log.info("--- (%d/%d) ---", i, total)                 # Log current progress (e.g. 42/5000)
        result = download_video(row, output_dir)              # Download this video and get result status
        counts[result] += 1                                   # Increment the appropriate result counter

    # --- Final Summary -------------------------------------------------------

    log.info("=" * 60)                                        # Print a separator line
    log.info("SUMMARY")                                       # Print the summary header
    log.info("  Total    : %d", total)                        # Log total number of videos processed
    log.info("  Success  : %d", counts["ok"])                 # Log number of successfully downloaded files
    log.info("  Skipped  : %d", counts["skipped"])            # Log number of skipped (existing) files
    log.info("  Failed   : %d", counts["failed"])             # Log number of failed downloads
    log.info("  No URL   : %d", counts["no_url"])             # Log number of rows with missing URLs
    log.info("Log saved to: %s", LOG_FILE)                    # Remind user where the log file is saved
    log.info("=" * 60)                                        # Print final separator line


# =============================================================================
# SCRIPT ENTRY GUARD
# Ensures main() only runs when this file is executed directly,
# not when it is imported as a module by another script.
# =============================================================================

if __name__ == "__main__":    # Check if this script is being run directly (not imported)
    main()                    # Call the main function to start the program