# --- Standard Library Imports ------------------------------------------------

import argparse                        # CLI argument parsing
import csv                             # CSV file reading
import json                            # progress.json read/write  ← FIX 1: moved to top level
import logging                         # Logging framework
import os                              # Environment variable access
import re                              # Regex for filename sanitization
import sys                             # System exit
import time                            # Delays between retries
from datetime import datetime          # Timestamps  ← FIX 1: moved to top level
from pathlib import Path               # Cross-platform path handling

# --- Third-Party Imports -----------------------------------------------------

import openpyxl                        # Read Excel .xlsx files
import requests                        # HTTP downloads
from tqdm import tqdm                  # Progress bars


# =============================================================================
# SECTION 1: GLOBAL CONFIGURATION
# All settings readable from environment variables.
# CLI arguments override environment variables when both are provided.
#
# Environment variables:
#   VIMEO_INPUT_FILE      Path to input .csv or .xlsx file
#   VIMEO_OUTPUT_DIR      Folder to save downloaded videos
#   VIMEO_LOG_DIR         Folder for dated log files
#   VIMEO_PROGRESS_FILE   Path to progress.json resume file
#   VIMEO_MAX_RETRIES     Max retry attempts per video (int)
#   VIMEO_RETRY_DELAY     Base delay in seconds between retries (int)
#   VIMEO_RETRY_DELAY_429 Delay in seconds for HTTP 429/503 responses (int)
#   VIMEO_CHUNK_SIZE      Download chunk size in bytes (int)
#   VIMEO_TIMEOUT         HTTP request timeout in seconds (int)
# =============================================================================

DEFAULT_INPUT_FILE  = os.environ.get("VIMEO_INPUT_FILE","list.csv")
DEFAULT_OUTPUT_DIR  = os.environ.get("VIMEO_OUTPUT_DIR","./downloaded_videos")
LOG_DIR             = os.environ.get("VIMEO_LOG_DIR","logs")
PROGRESS_FILE       = os.environ.get("VIMEO_PROGRESS_FILE","Progress.json")

MAX_RETRIES         = int(os.environ.get("VIMEO_MAX_RETRIES","5"))
RETRY_DELAY_BASE    = int(os.environ.get("VIMEO_RETRY_DELAY","5"))
RETRY_DELAY_429     = int(os.environ.get("VIMEO_RETRY_DELAY_429","60"))
CHUNK_SIZE          = int(os.environ.get("VIMEO_CHUNK_SIZE",str(1024 * 1024)))
REQUEST_TIMEOUT     = int(os.environ.get("VIMEO_TIMEOUT","60"))


# =============================================================================
# SECTION 2: HTTP STATUS CODE CLASSIFICATION
#
# Three tiers — each handled differently in download_direct():
#
#   NO_RETRY_CODES        — Permanent client errors. The same request will
#                           always fail. Do not retry. Log and move on.
#
#   RETRY_WITH_WAIT_CODES — Rate limiting or temporary overload. Server is
#                           alive but throttling. Wait RETRY_DELAY_429 seconds
#                           before retrying.  ← FIX 2: 503 now handled here
#
#   RETRYABLE_CODES       — Transient server errors. Server-side problem that
#                           may resolve. Retry with exponential backoff.
#                           ← FIX 3: all 5xx codes now explicitly handled
# =============================================================================

NO_RETRY_CODES = {

    # ── 4xx Client Errors — problem is with the request itself ───────────────
    # These will never succeed without fixing the URL or permissions.

    400,   # Bad Request          — malformed URL or invalid parameters
    401,   # Unauthorized         — authentication required, token expired
    403,   # Forbidden            — video is private, access denied, IP blocked
    404,   # Not Found            — video deleted, URL invalid, or link expired
    405,   # Method Not Allowed   — wrong HTTP method used
    406,   # Not Acceptable       — server cannot produce requested format
    407,   # Proxy Auth Required  — proxy authentication needed
    409,   # Conflict             — resource state conflict
    410,   # Gone                 — resource permanently removed from server
    411,   # Length Required      — Content-Length header required
    412,   # Precondition Failed  — server precondition not met
    413,   # Payload Too Large    — request body too large
    414,   # URI Too Long         — URL is too long for server
    415,   # Unsupported Media    — media type not supported
    416,   # Range Not Satisfiable — byte range request invalid
    451,   # Unavailable Legal    — content blocked for legal reasons (DMCA)

}

RETRY_WITH_WAIT_CODES = {

    # ── Rate limiting / temporary overload ───────────────────────────────────
    # Server is throttling requests. Wait RETRY_DELAY_429 seconds then retry.

    429,   # Too Many Requests    — standard rate limit response
    503,   # Service Unavailable  — server temporarily overloaded  ← FIX 2

}

RETRYABLE_CODES = {

    # ── 5xx Server Errors — problem is on the server side ────────────────────
    # These are temporary. The server may recover. Retry with backoff.

    500,   # Internal Server Error   — generic server crash, usually temporary
    502,   # Bad Gateway             — upstream server invalid response
    504,   # Gateway Timeout         — upstream server timed out
    507,   # Insufficient Storage    — server disk space issue
    508,   # Loop Detected           — server redirect loop
    509,   # Bandwidth Exceeded      — server bandwidth limit hit
    520,   # Unknown Error           — Cloudflare: unexpected origin response
    521,   # Web Server Down         — Cloudflare: origin server offline
    522,   # Connection Timed Out    — Cloudflare: origin connection timed out
    523,   # Origin Unreachable      — Cloudflare: cannot route to origin
    524,   # Timeout                 — Cloudflare: connection timed out
    525,   # SSL Handshake Failed    — Cloudflare: SSL negotiation failed
    526,   # Invalid SSL Cert        — Cloudflare: invalid SSL cert at origin
    527,   # Railgun Error           — Cloudflare: Railgun connection error
    530,   # Site Frozen             — Cloudflare: origin 1xxx error

}


# =============================================================================
# SECTION 3: LOGGING CONFIGURATION
# Each run creates a new dated log file inside the logs/ folder.
# A separate failed.log is maintained with structured failure reports.
# All logs also print to terminal simultaneously.
# =============================================================================

def setup_logging(log_dir: str) -> tuple[logging.Logger, str]:
    Path(log_dir).mkdir(parents=True, exist_ok=True)           # Create logs/ if missing

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")   # e.g. 2026-03-22_13-45-01
    log_file  = str(Path(log_dir) / f"{timestamp}.log")        # Full path to this run's log

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),   # Write to dated log file
            logging.StreamHandler(sys.stdout),                  # Also print to terminal
        ],
    )

    log = logging.getLogger(__name__)
    log.info("Log file : %s", log_file)
    return log, log_file


def log_failed(
    log_dir:     str,
    video_id:    str,
    title:       str,
    url:         str,
    reason:      str,
    method:      str = "unknown",
    attempts:    int = 0,
    http_status: int = 0,
    file_size:   int = 0,
):
    failed_log   = Path(log_dir) / "failed.log"
    reason_lower = reason.lower()
    ts           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep          = "=" * 80
    dash         = "-" * 80

    # ── HTTP failures — short table-style entry ───────────────────────────────

    HTTP_MESSAGES = {
        400: ("Bad Request",           "Fix the URL — malformed or invalid parameters.",         False),
        401: ("Unauthorized",          "Re-export download URL from Vimeo — token expired.",     False),
        403: ("Forbidden",             "Video is private or access denied. Check permissions.",  False),
        404: ("Not Found",             "Video deleted or link expired. Re-export URL.",           False),
        405: ("Method Not Allowed",    "Wrong HTTP method. Check URL format.",                    False),
        406: ("Not Acceptable",        "Server cannot return requested format.",                  False),
        407: ("Proxy Auth Required",   "Configure proxy credentials.",                            False),
        409: ("Conflict",              "Resource state conflict. Re-export URL.",                 False),
        410: ("Gone",                  "Permanently deleted. Remove from input file.",            False),
        411: ("Length Required",       "Missing Content-Length header.",                          False),
        412: ("Precondition Failed",   "Server precondition not met.",                            False),
        413: ("Payload Too Large",     "Request too large.",                                      False),
        414: ("URI Too Long",          "URL too long for server.",                                False),
        415: ("Unsupported Media",     "Media type not supported.",                               False),
        416: ("Range Not Satisfiable", "Invalid byte range.",                                     False),
        429: ("Too Many Requests",     "Rate limited. Retry: --retry-failed",                     True),
        451: ("Unavailable Legal",     "DMCA or legal block. Cannot download.",                   False),
        500: ("Internal Server Error", "Server crashed. Retry: --retry-failed",                   True),
        502: ("Bad Gateway",           "Upstream error. Retry: --retry-failed",                   True),
        503: ("Service Unavailable",   "Server overloaded. Retry: --retry-failed",                True),
        504: ("Gateway Timeout",       "Upstream timeout. Retry: --retry-failed",                 True),
        520: ("Cloudflare Error",      "Origin unexpected response. Retry: --retry-failed",       True),
        521: ("Web Server Down",       "Origin offline. Retry: --retry-failed",                   True),
        522: ("Connection Timed Out",  "Origin timed out. Retry: --retry-failed",                 True),
        523: ("Origin Unreachable",    "Cannot route to origin. Retry: --retry-failed",           True),
        524: ("Cloudflare Timeout",    "Connection timed out. Retry: --retry-failed",             True),
        525: ("SSL Handshake Failed",  "SSL error. Retry: --retry-failed",                        True),
        526: ("Invalid SSL Cert",      "Bad SSL cert at origin. Retry: --retry-failed",           True),
    }

    if http_status and http_status in HTTP_MESSAGES:
        meaning, action, retryable = HTTP_MESSAGES[http_status]
        retry_label = "YES" if retryable else "NO"
        report = (
            f"{sep}\n"
            f"FAILED  |  {ts}\n"
            f"  ID      : {video_id}\n"
            f"  Title   : {title}\n"
            f"  Method  : {method}  |  Attempts: {attempts}/{MAX_RETRIES}\n"
            f"  HTTP    : {http_status} {meaning}\n"
            f"  Retry   : {retry_label}\n"
            f"  Action  : {action}\n"
            f"  URL     : {url}\n"
            f"{sep}\n\n"
        )

    elif http_status and http_status >= 400:
        # Unknown HTTP code — still keep it short
        retryable   = http_status >= 500
        retry_label = "YES" if retryable else "NO"
        report = (
            f"{sep}\n"
            f"FAILED  |  {ts}\n"
            f"  ID      : {video_id}\n"
            f"  Title   : {title}\n"
            f"  Method  : {method}  |  Attempts: {attempts}/{MAX_RETRIES}\n"
            f"  HTTP    : {http_status} (unrecognised)\n"
            f"  Retry   : {retry_label}\n"
            f"  Action  : {'Retry: --retry-failed' if retryable else 'Fix URL or remove entry'}\n"
            f"  URL     : {url}\n"
            f"{sep}\n\n"
        )

    else:
        # ── Non-HTTP failures — detailed explanation ──────────────────────────

        if "timeout" in reason_lower:
            category    = "TIMEOUT"
            explanation = f"No response within {REQUEST_TIMEOUT}s. Check connection speed or increase VIMEO_TIMEOUT."
            retryable   = True
            action      = "Retry: python download_videos.py --retry-failed\n   Or increase: set VIMEO_TIMEOUT=120"

        elif "connection" in reason_lower or "network" in reason_lower:
            category    = "NETWORK ERROR"
            explanation = "DNS failure, no internet, firewall blocking, or server unreachable."
            retryable   = True
            action      = "Check internet connection, then retry: python download_videos.py --retry-failed"

        elif "incomplete" in reason_lower:
            sz          = f"{file_size:,} bytes ({file_size/1e6:.2f} MB)" if file_size else "unknown"
            category    = "INCOMPLETE DOWNLOAD"
            explanation = f"Only {sz} received before connection dropped. Network interruption mid-download."
            retryable   = True
            action      = "Retry: python download_videos.py --retry-failed"

        elif "private" in reason_lower or "members only" in reason_lower:
            category    = "PRIVATE VIDEO"
            explanation = "yt-dlp: video is private or members-only. Account lacks download permission."
            retryable   = False
            action      = "Check Vimeo privacy settings. Generate a direct download link if you own the video."

        elif "geo" in reason_lower or "not available in your country" in reason_lower:
            category    = "GEO-BLOCKED"
            explanation = "Video restricted in current geographic region."
            retryable   = False
            action      = "Use a VPN set to a permitted region, or check Vimeo geo-restriction settings."

        elif "no info" in reason_lower or "returned no info" in reason_lower:
            category    = "NO VIDEO INFO"
            explanation = "yt-dlp could not extract info — video may be deleted or URL format unsupported."
            retryable   = False
            action      = "Verify URL is valid in browser. Re-export download URL from Vimeo."

        elif "not installed" in reason_lower:
            category    = "MISSING LIBRARY"
            explanation = "yt-dlp is not installed on this system."
            retryable   = False
            action      = "Run: pip install yt-dlp"

        else:
            category    = "UNKNOWN ERROR"
            explanation = "Unexpected error — check the dated log file in logs/ for the full traceback."
            retryable   = True
            action      = "Check logs/ for traceback. Retry: python download_videos.py --retry-failed"

        retry_label = "YES" if retryable else "NO"
        report = (
            f"{sep}\n"
            f"FAILED  |  {ts}\n"
            f"  ID          : {video_id}\n"
            f"  Title       : {title}\n"
            f"  Method      : {method}  |  Attempts: {attempts}/{MAX_RETRIES}\n"
            f"  Category    : {category}\n"
            f"  Retryable   : {retry_label}\n"
            f"{dash}\n"
            f"  Explanation : {explanation}\n"
            f"  Raw Error   : {reason}\n"
            f"  Action      : {action}\n"
            f"{dash}\n"
            f"  URL         : {url}\n"
            f"{sep}\n\n"
        )

    with open(failed_log, "a", encoding="utf-8") as f:
        f.write(report)


def load_progress(progress_file: str) -> dict:
    """
    Load progress.json or return empty dict if file not found.

    Args:
        progress_file (str): Path to progress.json

    Returns:
        dict: {video_id: {"status": str, "title": str, "url": str}}
    """
    path = Path(progress_file)
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(progress_file: str, progress: dict):
    path     = Path(progress_file)
    tmp_path = path.with_suffix(".tmp")                        # Write to .tmp first

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)

    tmp_path.replace(path)                                     # Atomic rename — safe on all OS


def init_progress(rows: list[dict], progress: dict) -> dict:
    for row in rows:
        vid_id = str(row.get("video_id") or "").strip()
        if vid_id and vid_id not in progress:
            progress[vid_id] = {
                "status": "pending",
                "title":  str(row.get("title") or ""),
                "url":    str(row.get("download_link") or ""),
            }
    return progress


# =============================================================================
# SECTION 5: HELPER — sanitize_filename()
# Strips characters illegal in Windows/macOS/Linux filenames.
# =============================================================================

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name)                 # Replace illegal chars
    name = name.strip().strip(".")                             # Remove edge whitespace/dots
    return name or "untitled"


# =============================================================================
# SECTION 6: CORE — read_input_file()
# Reads .xlsx or .csv and returns list of video record dicts.
# =============================================================================

def read_input_file(path: str) -> list[dict]:

    path = Path(path)
    rows = []

    if path.suffix.lower() in (".xlsx", ".xls"):
        wb      = openpyxl.load_workbook(path)
        ws      = wb.active
        headers = [str(c.value).strip().lower() if c.value else "" for c in ws[1]]
        for row in ws.iter_rows(min_row=2, values_only=True):
            rd = dict(zip(headers, row))
            if not any(rd.values()):
                continue
            rows.append({
                "video_id":      rd.get("video id", ""),
                "title":         rd.get("video title", ""),
                "download_link": rd.get("download url", ""),
            })

    elif path.suffix.lower() == ".csv":
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                norm = {k.strip().lower(): v for k, v in row.items()}
                if not any(norm.values()):
                    continue
                rows.append({
                    "video_id":      norm.get("video id", ""),
                    "title":         norm.get("video title", ""),
                    "download_link": norm.get("download url", ""),
                })

    else:
        logging.getLogger(__name__).error(
            "Unsupported format '%s' — use .csv or .xlsx", path.suffix
        )
        sys.exit(1)

    return rows


# =============================================================================
# SECTION 7: HELPER — is_direct_download()
# Detects whether a URL is a direct file link (requests) or
# a watch-page link (yt-dlp).
# =============================================================================

def is_direct_download(url: str) -> bool:
    direct_signals = (
        "progressive_redirect",        # Vimeo CDN direct link
        "player.vimeo.com/external",   # Vimeo external player direct link
        ".mp4", ".mov", ".webm",       # Common video file extensions
        ".mkv", ".avi",
    )
    return any(s in url.lower() for s in direct_signals)


# =============================================================================
# SECTION 8: DOWNLOAD — download_with_ytdlp()
# Downloads Vimeo/YouTube watch-page URLs via yt-dlp.
# =============================================================================

def download_with_ytdlp(
    url:         str,
    output_path: Path,
    log:         logging.Logger,
) -> tuple[bool, str]:
    try:
        import yt_dlp                                          # Lazy import
    except ImportError:
        log.error("yt-dlp not installed — run: pip install yt-dlp")
        return False, "yt-dlp not installed"

    ydl_opts = {
        "outtmpl":      str(output_path.with_suffix("")) + ".%(ext)s",
        "quiet":        False,
        "no_warnings":  False,
        "ignoreerrors": False,
        "retries":      MAX_RETRIES,
        "noprogress":   False,
    }

    try:
        log.info("  yt-dlp downloading: %s", url)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return False, "yt-dlp returned no info — URL may be private or deleted"
        return True, "ok"

    except yt_dlp.utils.DownloadError as exc:
        reason = f"yt-dlp DownloadError: {exc}"
        log.error("  %s", reason)
        return False, reason

    except Exception as exc:
        reason = f"yt-dlp unexpected error: {exc}"
        log.error("  %s", reason)
        log.exception("  Full traceback:")
        return False, reason


# =============================================================================
# SECTION 9: DOWNLOAD — download_direct()
# Chunked HTTP download with full three-tier HTTP status handling.
# Retries with exponential backoff. Cleans partial files on failure.
#
# FIX 2: RETRY_WITH_WAIT_CODES (429, 503) now checked explicitly
# FIX 3: RETRYABLE_CODES (5xx) now checked explicitly
# FIX 4: reason variable initialised before loop to prevent UnboundLocalError
# =============================================================================

def download_direct(
    url:         str,
    output_path: Path,
    log:         logging.Logger,
) -> tuple[bool, str]:
    reason = "Download did not start"                          # FIX 4: initialise before loop

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            with requests.get(
                url,
                stream=True,
                timeout=(10, REQUEST_TIMEOUT),                 # (connect_timeout, read_timeout)
            ) as r:

                # ── Tier 1: Permanent failures — stop immediately ─────────

                if r.status_code in NO_RETRY_CODES:
                    reason = (
                        f"HTTP {r.status_code} — permanent failure, will not retry. "
                        f"Fix the URL or permissions and re-run."
                    )
                    log.error("  %s", reason)
                    return False, reason                       # No retry

                # ── Tier 2: Rate limiting — wait longer then retry ────────
                # FIX 2: Now uses RETRY_WITH_WAIT_CODES set (429 AND 503)

                if r.status_code in RETRY_WITH_WAIT_CODES:
                    reason = f"HTTP {r.status_code} — rate limited or temporarily unavailable"
                    log.warning(
                        "  HTTP %d — waiting %ds before retry (attempt %d/%d)",
                        r.status_code, RETRY_DELAY_429, attempt, MAX_RETRIES,
                    )
                    time.sleep(RETRY_DELAY_429)                # Wait much longer
                    continue                                   # Retry after long wait

                # ── Tier 3: Transient server errors — retry with backoff ──
                # FIX 3: Now explicitly catches all RETRYABLE_CODES (500, 502, 504, 5xx...)

                if r.status_code in RETRYABLE_CODES:
                    reason = (
                        f"HTTP {r.status_code} — transient server error, "
                        f"retrying (attempt {attempt}/{MAX_RETRIES})"
                    )
                    log.warning("  %s", reason)
                    output_path.unlink(missing_ok=True)        # Clean up partial file
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAY_BASE * attempt
                        log.warning("  Retrying in %ds...", delay)
                        time.sleep(delay)
                    continue                                   # Retry with backoff

                # ── Any other unrecognised 4xx — treat as permanent ───────

                if 400 <= r.status_code < 500:
                    reason = (
                        f"HTTP {r.status_code} — unrecognised client error, "
                        f"treating as permanent failure"
                    )
                    log.error("  %s", reason)
                    return False, reason                       # No retry

                r.raise_for_status()                           # Raise for anything else

                # ── Download with per-file progress bar ───────────────────

                total = int(r.headers.get("Content-Length", 0))

                with (
                    open(output_path, "wb") as f,
                    tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"  {output_path.name[:35]}",
                        leave=False,
                        ncols=90,
                    ) as bar,
                ):
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
                            downloaded += len(chunk)

                # ── Validate download completeness ────────────────────────

                if total > 0 and downloaded < total:
                    reason = f"Incomplete download: received {downloaded:,} of {total:,} bytes"
                    log.warning("  %s — retrying", reason)
                    output_path.unlink(missing_ok=True)
                    time.sleep(RETRY_DELAY_BASE * attempt)
                    continue

                return True, "ok"                              # Success

        except requests.exceptions.Timeout:
            reason = f"Timeout — no response within {REQUEST_TIMEOUT}s"
            log.warning("  Attempt %d/%d — %s", attempt, MAX_RETRIES, reason)

        except requests.exceptions.ConnectionError as exc:
            reason = f"Connection error: {exc}"
            log.warning("  Attempt %d/%d — %s", attempt, MAX_RETRIES, reason)

        except requests.exceptions.HTTPError as exc:
            reason = f"HTTP error: {exc}"
            log.warning("  Attempt %d/%d — %s", attempt, MAX_RETRIES, reason)

        except Exception as exc:
            reason = f"Unexpected error: {exc}"
            log.error("  Attempt %d/%d — %s", attempt, MAX_RETRIES, reason)

        # ── Retry cleanup ─────────────────────────────────────────────────

        output_path.unlink(missing_ok=True)                    # Delete partial file

        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY_BASE * attempt                 # Exponential backoff
            log.warning("  Retrying in %ds... (%d/%d)", delay, attempt, MAX_RETRIES)
            time.sleep(delay)

    return False, reason                                       # All retries exhausted


# =============================================================================
# SECTION 10: ORCHESTRATOR — download_video()
# Full per-video download flow. Uses Video ID as primary key filename.
# Updates progress.json after every video.
# =============================================================================

def download_video(
    row:           dict,
    output_dir:    Path,
    progress:      dict,
    progress_file: str,
    log_dir:       str,
    log:           logging.Logger,
) -> str:

    url    = str(row.get("download_link") or "").strip()
    title  = str(row.get("title")         or "").strip()
    vid_id = str(row.get("video_id")      or "").strip()

    if not vid_id:
        log.warning("NO VIDEO ID — title='%s' — using 'no_id'", title)
        vid_id = "no_id"

    if progress.get(vid_id, {}).get("status") == "ok":
        log.info("SKIP  [ID=%s] already completed in previous run", vid_id)
        return "skipped"

    if not url:
        log.warning("NO URL  [ID=%s] title='%s' — skipping", vid_id, title)
        progress[vid_id] = {"status": "no_url", "title": title, "url": ""}
        save_progress(progress_file, progress)
        return "no_url"

    safe_id = sanitize_filename(vid_id)

    if is_direct_download(url):
        ext         = Path(url.split("?")[0]).suffix or ".mp4"
        output_path = output_dir / f"{safe_id}{ext}"
    else:
        output_path = output_dir / safe_id

    existing = list(output_dir.glob(f"{safe_id}.*"))
    if existing:
        log.info("SKIP  [ID=%s] already on disk: %s", vid_id, existing[0].name)
        progress[vid_id] = {"status": "ok", "title": title, "url": url}
        save_progress(progress_file, progress)
        return "skipped"

    log.info("START [ID=%s] %s", vid_id, title)
    log.info("  URL      : %s", url)
    log.info("  Saving as: %s", output_path.name)

    if is_direct_download(url):
        success, reason = download_direct(url, output_path, log)
    else:
        success, reason = download_with_ytdlp(url, output_path, log)

    if success:
        log.info("OK    [ID=%s] %s", vid_id, title)
        progress[vid_id] = {"status": "ok", "title": title, "url": url}
    else:
        log.error("FAIL  [ID=%s] %s — %s", vid_id, title, reason)
        progress[vid_id] = {"status": "failed", "title": title, "url": url, "reason": reason}

        # Extract HTTP status from reason string if present
        http_match  = re.search(r"HTTP (\d{3})", reason)
        http_status = int(http_match.group(1)) if http_match else 0
        method      = "requests" if is_direct_download(url) else "yt-dlp"

        log_failed(
            log_dir     = log_dir,
            video_id    = vid_id,
            title       = title,
            url         = url,
            reason      = reason,
            method      = method,
            attempts    = MAX_RETRIES,
            http_status = http_status,
        )
        output_path.unlink(missing_ok=True)                    # Clean up partial file

    save_progress(progress_file, progress)
    return "ok" if success else "failed"


# =============================================================================
# SECTION 11: OVERALL PROGRESS DISPLAY
# Persistent progress bar: files done/total, % complete, ETA.
# =============================================================================

class OverallProgress:

    def __init__(self, total: int, log: logging.Logger):
        self.total      = total
        self.done       = 0
        self.failed     = 0
        self.skipped    = 0
        self.start_time = time.time()
        self.log        = log
        self.bar        = tqdm(
            total=total,
            unit="file",
            desc="Overall",
            ncols=90,
            position=0,
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    def update(self, result: str, elapsed_secs: float):
        self.done += 1
        if result == "failed":
            self.failed += 1
        elif result in ("skipped", "no_url"):
            self.skipped += 1

        self.bar.update(1)

        elapsed_total = time.time() - self.start_time
        avg_per_file  = elapsed_total / self.done
        eta_secs      = avg_per_file * (self.total - self.done)
        pct           = (self.done / self.total) * 100

        self.log.info(
            "PROGRESS  %d/%d (%.1f%%) | Done:%d Skipped:%d Failed:%d | ETA:%s",
            self.done, self.total, pct,
            self.done - self.failed - self.skipped,
            self.skipped, self.failed,
            self._format_time(eta_secs),
        )

    def close(self):
        self.bar.close()

    @staticmethod
    def _format_time(seconds: float) -> str:
        s = int(seconds)
        if s >= 3600:
            return f"{s // 3600}h {(s % 3600) // 60}m"
        elif s >= 60:
            return f"{s // 60}m {s % 60}s"
        else:
            return f"{s}s"


# =============================================================================
# SECTION 12: ENTRY POINT — main()
# =============================================================================

def main():

    global LOG_DIR, PROGRESS_FILE, MAX_RETRIES, RETRY_DELAY_BASE
    global RETRY_DELAY_429, CHUNK_SIZE, REQUEST_TIMEOUT
# --- Step 1: Parse CLI arguments -----------------------------------------
 
    _epilog = (
        "Environment variables (all overridden by CLI args):\n"
        "  VIMEO_INPUT_FILE       Input .csv or .xlsx file\n"
        "  VIMEO_OUTPUT_DIR       Output folder for downloaded videos\n"
        "  VIMEO_LOG_DIR          Folder for dated log files\n"
        "  VIMEO_PROGRESS_FILE    Path to progress.json\n"
        "  VIMEO_MAX_RETRIES      Max retry attempts (int)\n"
        "  VIMEO_RETRY_DELAY      Base retry delay in seconds (int)\n"
        "  VIMEO_RETRY_DELAY_429  Delay for HTTP 429/503 in seconds (int)\n"
        "  VIMEO_CHUNK_SIZE       Download chunk size in bytes (int)\n"
        "  VIMEO_TIMEOUT          HTTP read timeout in seconds (int)\n"
        "\n"
        "Examples:\n"
        "  python download_videos.py\n"
        "  python download_videos.py -i list.csv -o E:/Vimeo_Archive\n"
        "  python download_videos.py --retry-failed\n"
    )
 
    parser = argparse.ArgumentParser(
        description="Econ Engineering — Vimeo Video Bulk Downloader v2.2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_epilog,
    )
    parser.add_argument("--input",  "-i", default=DEFAULT_INPUT_FILE,
                        help=f"Input file [env: VIMEO_INPUT_FILE] (default: {DEFAULT_INPUT_FILE})")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR,
                        help=f"Output folder [env: VIMEO_OUTPUT_DIR] (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--log-dir",       default=LOG_DIR,
                        help=f"Log folder [env: VIMEO_LOG_DIR] (default: {LOG_DIR})")
    parser.add_argument("--progress-file", default=PROGRESS_FILE,
                        help=f"Progress file [env: VIMEO_PROGRESS_FILE] (default: {PROGRESS_FILE})")
    parser.add_argument("--retry-failed",  action="store_true",
                        help="Re-attempt only videos marked as failed in progress.json")
    args = parser.parse_args()
 
    LOG_DIR       = args.log_dir
    PROGRESS_FILE = args.progress_file
    # --- Step 2: Logging + output dir ----------------------------------------

    log, log_file = setup_logging(args.log_dir)
    output_dir    = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Step 3: Startup banner ----------------------------------------------

    log.info("=" * 60)
    log.info("  Econ Engineering Video Downloader  v2.2.0")
    log.info("=" * 60)
    log.info("Input        : %s", args.input)
    log.info("Output       : %s", output_dir.resolve())
    log.info("Progress file: %s", PROGRESS_FILE)
    log.info("Log folder   : %s", Path(LOG_DIR).resolve())
    log.info("Python       : %s", sys.version.split()[0])
    log.info("Filename key : Video ID (primary key)")
    log.info("-" * 60)

    # --- Step 4: Library check -----------------------------------------------

    all_ok = True
    for lib, pkg in [("requests","requests"), ("openpyxl","openpyxl"),
                     ("tqdm","tqdm"), ("yt_dlp","yt-dlp")]:
        try:
            mod = __import__(lib)
            ver = getattr(mod, "__version__", None) or getattr(
                getattr(mod, "version", None), "__version__", "?")
            log.info("%-10s : OK  (v%s)", lib, ver)
        except ImportError:
            log.error("%-10s : MISSING — run: pip install %s", lib, pkg)
            all_ok = False

    if not all_ok:
        log.error("Run: pip install requests openpyxl yt-dlp tqdm")
        sys.exit(1)

    # --- Step 5: Input file check --------------------------------------------

    log.info("-" * 60)
    if not Path(args.input).exists():
        log.error("INPUT FILE NOT FOUND: '%s'", args.input)
        log.error("Make sure '%s' is in the same folder as this script.", args.input)
        sys.exit(1)

    # --- Step 6: Read input file ---------------------------------------------

    rows  = read_input_file(args.input)
    total = len(rows)
    log.info("Found %d video(s) in input file.", total)

    # --- Step 7: Load / init progress ----------------------------------------

    progress = load_progress(PROGRESS_FILE)
    progress = init_progress(rows, progress)

    # Disk verification — reset 'ok' entries whose files are missing on disk
    reset_count = 0
    for vid_id, entry in progress.items():
        if entry.get("status") == "ok":
            safe_id = sanitize_filename(vid_id)
            if not list(output_dir.glob(f"{safe_id}.*")):
                log.warning(
                    "RESET [ID=%s] marked ok but file missing on disk — re-queuing", vid_id
                )
                entry["status"] = "pending"
                reset_count += 1

    if reset_count > 0:
        log.warning("%d video(s) reset — files missing on disk", reset_count)

    save_progress(PROGRESS_FILE, progress)

    done_count    = sum(1 for v in progress.values() if v["status"] == "ok")
    failed_count  = sum(1 for v in progress.values() if v["status"] == "failed")
    pending_count = sum(1 for v in progress.values() if v["status"] == "pending")
    log.info("State — Done:%d | Failed:%d | Pending:%d | Reset:%d",
             done_count, failed_count, pending_count, reset_count)

    # --- Step 8: Filter rows -------------------------------------------------

    if args.retry_failed:
        rows = [r for r in rows
                if progress.get(str(r.get("video_id","")).strip(),{}).get("status") == "failed"]
        log.info("--retry-failed: %d video(s) to re-attempt", len(rows))
    else:
        rows = [r for r in rows
                if progress.get(str(r.get("video_id","")).strip(),{}).get("status") != "ok"]
        log.info("Remaining: %d video(s)", len(rows))

    if not rows:
        log.info("Nothing to download — all videos already completed.")
        log.info("To retry failures: python download_videos.py --retry-failed")
        sys.exit(0)

    log.info("=" * 60)

    # --- Step 9: Download loop -----------------------------------------------

    counts  = {"ok": 0, "skipped": 0, "failed": 0, "no_url": 0}
    overall = OverallProgress(len(rows), log)

    for i, row in enumerate(rows, start=1):
        log.info("--- (%d / %d) ---", i, len(rows))
        t_start = time.time()
        result  = download_video(row, output_dir, progress, PROGRESS_FILE, LOG_DIR, log)
        counts[result] += 1
        overall.update(result, time.time() - t_start)

    overall.close()

    # --- Step 10: Final summary ----------------------------------------------

    log.info("=" * 60)
    log.info("DOWNLOAD COMPLETE — SUMMARY")
    log.info("=" * 60)
    log.info("  Total      : %d", len(rows))
    log.info("  Downloaded : %d", counts["ok"])
    log.info("  Skipped    : %d", counts["skipped"])
    log.info("  Failed     : %d", counts["failed"])
    log.info("  No URL     : %d", counts["no_url"])
    log.info("-" * 60)
    log.info("  Output     : %s", output_dir.resolve())
    log.info("  Log file   : %s", log_file)
    if counts["failed"] > 0:
        log.info("  Failed log : %s", str(Path(LOG_DIR) / "failed.log"))
        log.info("  To retry   : python download_videos.py --retry-failed")
    log.info("  Progress   : %s", PROGRESS_FILE)
    log.info("=" * 60)


if __name__ == "__main__":
    main()