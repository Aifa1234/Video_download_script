# Vimeo Video Bulk Downloader

```
=============================================================================
PROJECT   : Vimeo Video Bulk Downloader
PURPOSE   : Migrate ~5,000 Vimeo video files to local / external storage
DATE      : March 2026
=============================================================================
```

---

## Table of Contents

1. [Overview](#1-overview)
2. [Project Structure](#3-project-structure)
3. [Script Architecture](#4-script-architecture)
4. [Technologies](#5-technologies)
5. [Setup](#6-setup)
6. [Usage](#7-usage)
7. [Environment Variables](#8-environment-variables)
8. [Input File Format](#9-input-file-format)
9. [File Naming](#10-file-naming)
10. [How It Works](#11-how-it-works)
11. [Dynamic Concurrency](#12-dynamic-concurrency)
12. [Progress Tracking](#13-progress-tracking)
13. [Logging & Output](#14-logging--output)
14. [Error Handling](#15-error-handling)
15. [Resume from Failure](#16-resume-from-failure)

---

## 1. Overview

Bulk downloader for ~5,000 Vimeo videos. Reads a CSV or Excel input file and downloads each video to a local or external drive. Designed to run unattended overnight with automatic resume, dynamic bandwidth-based concurrency, and structured logging.

**Key design decisions:**
- `<VideoID>_<VideoTitle>.mp4` filename format — unique and human-readable
- `progress.json` tracks every video — exact resume from any failure point
- Thread count auto-adjusts every 5 minutes based on measured bandwidth
- All config driven by `VIMEO_*` environment variables — no hardcoded values


## 3. Project Structure

```
SCRIPT/
│
├── download_videos.py            
├── list.csv                    
├── progress.json              
├── README.md                   
│
├── logs/                        
│   ├── 2026-03-22_22-10-01.log   
│   ├── 2026-03-23_08-00-00.log  
│   └── failed.log               
│
└── downloaded_videos/            
    ├── 1156438752_PWC_Delhi_Background_Visuals_Energy.mp4
    ├── 1156438753_Intro_Clip_Final.mp4
    └── ...
```

---

## 4. Script Architecture

| Section | Name | Description |
|---|---|---|
| 1 | Global Configuration | All constants loaded from `VIMEO_*` environment variables |
| 2 | HTTP Status Code Classification | Three tiers: `NO_RETRY_CODES` / `RETRY_WITH_WAIT_CODES` / `RETRYABLE_CODES` |
| 3 | Logging Configuration | `setup_logging()` — dated log + terminal. `log_failed()` — structured failure reports |
| 4 | Progress Tracking | `load_progress()` / `save_progress()` / `init_progress()` — atomic writes, thread-safe |
| 5a | `sanitize_filename()` | Strips illegal filename characters |
| 5b | `build_filename()` | Builds `<ID>_<Title>.<ext>` with 180-char length cap |
| 6 | `read_input_file()` | Parses `.xlsx` or `.csv` — case-insensitive header matching |
| 7a | `is_valid_url()` | Validates URL format before any download attempt |
| 7b | `is_direct_download()` | Routes URL to `requests` (direct) or `yt-dlp` (watch-page) |
| 8a | `BandwidthMonitor` | Measures Mbps every 5 min — maps to thread count via `BANDWIDTH_TIERS` |
| 8b | `download_with_ytdlp()` | Downloads Vimeo/YouTube watch-page URLs via yt-dlp |
| 9 | `download_direct()` | Chunked HTTP download — 3-tier error handling, disk/integrity checks |
| 10 | `download_video()` | Per-video orchestrator — validate → filename → download → log |
| 11 | `OverallProgress` | Thread-safe progress bar + ETA tracker |
| 12 | `main()` | Entry point — library check, bandwidth measure, parallel download loop |

---

## 5. Technologies

| Library | Type | Purpose |
|---|---|---|
| `requests` | Third-party | Chunked HTTP downloads for direct file URLs |
| `openpyxl` | Third-party | Reading `.xlsx` Excel input files |
| `yt-dlp` | Third-party | Downloading Vimeo / YouTube watch-page URLs |
| `tqdm` | Third-party | Per-file and overall progress bars |
| `concurrent.futures` | stdlib | Thread pool for parallel downloads |
| `threading` | stdlib | Locks for thread-safe shared state |
| `logging` | stdlib | Dual output — terminal + dated log file |
| `json` | stdlib | `progress.json` read/write |
| `argparse` | stdlib | CLI argument parsing |
| `pathlib` | stdlib | Cross-platform path handling |
| `shutil` | stdlib | Disk space check before download |
| `urllib.parse` | stdlib | URL format validation |
| `random` | stdlib | Retry jitter |
| `re` | stdlib | Filename sanitization |

---

## 6. Setup

**Install Python 3.10+** from [python.org](https://www.python.org/downloads/)
> ✅ Check **"Add Python to PATH"** during installation.

**Install dependencies:**
```bash
pip install requests openpyxl yt-dlp tqdm
```

**Prepare folder:**
```
C:\VideoDownloader\
├── download_videos.py
└── list.csv
```

---

## 7. Usage

```bash
# Default — reads list.csv, saves to ./downloaded_videos
python download_videos.py

# Custom input and output
python download_videos.py -i list.csv -o "E:\Vimeo_Archive"

# Retry only failed videos
python download_videos.py --retry-failed

# View all options
python download_videos.py --help
```

| Argument | Short | Default | Description |
|---|---|---|---|
| `--input` | `-i` | `list.csv` | Path to `.csv` or `.xlsx` input file |
| `--output` | `-o` | `./downloaded_videos` | Destination folder for downloaded videos |
| `--log-dir` | | `logs` | Folder for dated log files |
| `--progress-file` | | `progress.json` | Resume state file |
| `--retry-failed` | | `False` | Re-attempt failed videos only |

---

## 8. Environment Variables

All configuration is driven by `VIMEO_*` environment variables, eliminating hardcoded values. CLI arguments override environment variables when both are provided.

| Variable | Default | Description |
|---|---|---|
| `VIMEO_INPUT_FILE` | `list.csv` | Input file path |
| `VIMEO_OUTPUT_DIR` | `./downloaded_videos` | Output folder |
| `VIMEO_LOG_DIR` | `logs` | Log folder |
| `VIMEO_PROGRESS_FILE` | `progress.json` | Resume state file |
| `VIMEO_MAX_RETRIES` | `5` | Max retry attempts per video |
| `VIMEO_RETRY_DELAY` | `5` | Base retry delay in seconds |
| `VIMEO_RETRY_DELAY_429` | `60` | Delay for HTTP 429/503 responses |
| `VIMEO_CHUNK_SIZE` | `1048576` | Download chunk size in bytes (1 MB) |
| `VIMEO_TIMEOUT` | `60` | HTTP read timeout in seconds |
| `VIMEO_BW_INTERVAL` | `300` | Bandwidth recheck interval in seconds |
| `VIMEO_BW_TEST_URL` | *(1 MB test file)* | URL used for bandwidth measurement |

**PowerShell:**
```powershell
$env:VIMEO_INPUT_FILE = "list.csv"
$env:VIMEO_OUTPUT_DIR = "E:\Vimeo_Archive"
$env:VIMEO_MAX_RETRIES = "3"
python download_videos.py
```

**CMD:**
```cmd
set VIMEO_INPUT_FILE=list.csv
set VIMEO_OUTPUT_DIR=E:\Vimeo_Archive
python download_videos.py
```

---

## 9. Input File Format

Accepts `.csv` or `.xlsx` with these columns (matched case-insensitively):

| Column | Required | Used For |
|---|---|---|
| `Video ID` | ✅ | Primary key — prepended to filename |
| `Video Title` | ✅ | Appended to filename, used in logging |
| `Download URL` | ✅ | Direct `.mp4` URL or Vimeo/YouTube watch-page |
| All other columns | ❌ | Ignored |

> Empty rows skipped automatically.

---

## 10. File Naming

Every file saved as `<VideoID>_<VideoTitle>.<ext>`:

```
1156438752_PWC_Delhi_Background_Visuals_Energy_transition_v1.4.mp4
1156438753_Intro_Clip_Final.mp4
```

| Rule | Detail |
|---|---|
| ID prefix | Guarantees uniqueness even with duplicate titles |
| Title suffix | Human-readable — no need to cross-reference CSV |
| Sanitization | Illegal characters replaced with `_`, spaces → `_` |
| Length cap | Max 180 characters to avoid Windows path limit issues |

> **Previous behaviour (v1.x–v2.x):** Files were named by Video ID only (`1156438752.mp4`). Changed in v3.0.0 to ID+Title format for readability.

---

## 11. How It Works

```
main()
  ├── Parse CLI args + apply env vars
  ├── Setup dated log file in logs/
  ├── Check all libraries installed
  ├── Read input file → list of {video_id, title, download_link}
  ├── Load progress.json → disk verification → filter pending/failed
  ├── Initial bandwidth measurement → set starting thread count
  └── Parallel download loop:
        ├── Re-check bandwidth every 5 min → adjust thread count
        ├── Submit batch to ThreadPoolExecutor (batch size = thread count)
        └── Per thread — download_video():
              ├── Validate Video ID and URL format
              ├── Skip if already ok in progress.json
              ├── Skip if file already exists on disk
              ├── Build filename: <ID>_<Title>.<ext>
              ├── Route: direct URL  → download_direct()
              │         watch-page  → download_with_ytdlp()
              ├── Update progress.json (thread-safe)
              └── Write to failed.log if failed (thread-safe)
```

---

## 12. Dynamic Concurrency

Thread count adjusts automatically based on measured internet bandwidth, re-checked every 5 minutes throughout the overnight run:

| Bandwidth | Threads | Scenario |
|---|---|---|
| > 200 Mbps | 10 | Fiber at full capacity |
| 100–200 Mbps | 6 | Good speed, some congestion |
| 50–100 Mbps | 4 | Moderate bandwidth |
| 10–50 Mbps | 2 | Limited connection |
| < 10 Mbps | 1 | Poor connection — serial only |

- Bandwidth measured by timing a 1 MB test download
- Every measurement logged — thread count changes flagged explicitly
- Backs off when connection degrades, scales up when it recovers

**Sample log:**
```
BANDWIDTH CHECK — measuring...
BANDWIDTH — 215.8 Mbps (>200 Mbps) → 10 thread(s)
THREADS CHANGED — 6 → 10 (bandwidth: 215.8 Mbps)
```

---

## 13. Progress Tracking

**Per-file bar:**
```
  1156438752_PWC_Delhi.mp4  |████████░░|  78% | 245MB/322MB | 38.4MB/s
```

**Overall bar:**
```
Overall  |████████░░| 245/5000 [00:12<01:23, 45.2file/s]
```

**Log line after every video:**
```
PROGRESS  245/5000 (4.9%) | Done:240 Skipped:4 Failed:1 | ETA: 1h 23m
```

---

## 14. Logging & Output

Three separate outputs maintained simultaneously:

| Output | Contents |
|---|---|
| `logs/2026-03-22_22-10-01.log` | Full timestamped log — every event, warning, error per run |
| `logs/failed.log` | Structured failure reports — HTTP code, explanation, retryable flag, action |
| `progress.json` | Live resume state — updated after every single video |

**Sample log output:**
```
============================================================
  Econ Engineering Video Downloader  v3.0.0
============================================================
Input        : list.csv
Output       : E:\Vimeo_Archive
Progress file: progress.json
Log folder   : C:\VideoDownloader\logs
Python       : 3.12.2
Filename key : ID + Title (e.g. 1156438752_PWC_Delhi.mp4)
Concurrency  : Dynamic (bandwidth-based, checked every 300s)
------------------------------------------------------------
requests   : OK  (v2.32.3)
openpyxl   : OK  (v3.1.5)
tqdm       : OK  (v4.67.1)
yt_dlp     : OK  (v2026.03.17)
------------------------------------------------------------
Found 5000 video(s) in input file.
State — Done:0 | Failed:0 | Pending:5000 | Reset:0
============================================================
INITIAL BANDWIDTH CHECK
BANDWIDTH — 215.8 Mbps (>200 Mbps) → 10 thread(s)
============================================================
--- (1 / 5000) ---
START [ID=1156438752] PWC Delhi Energy Transition
  URL      : https://player.vimeo.com/...
  Saving as: 1156438752_PWC_Delhi_Energy_Transition.mp4
OK    [ID=1156438752]
PROGRESS  1/5000 (0.0%) | Done:1 Skipped:0 Failed:0 | ETA: 2h 18m
============================================================
DOWNLOAD COMPLETE — SUMMARY
  Total      : 5000
  Downloaded : 4950
  Skipped    : 45
  Failed     : 4
  No URL     : 1
------------------------------------------------------------
  Output     : E:\Vimeo_Archive
  Log file   : logs\2026-03-22_22-10-01.log
  Failed log : logs\failed.log
  Progress   : progress.json
  Final BW   : 198.4 Mbps | Final threads: 6
============================================================
```

**Sample failed.log entry (HTTP):**
```
================================================================================
FAILED  |  2026-03-22 13:45:01
  ID      : 1156438752
  Title   : PWC Delhi Energy Transition
  Method  : requests  |  Attempts: 5/5
  HTTP    : 403 Forbidden
  Retry   : NO
  Action  : Video is private or access denied. Check permissions.
================================================================================
```

**Sample failed.log entry (network):**
```
================================================================================
FAILED  |  2026-03-22 13:45:01
  ID          : 1156438753
  Category    : TIMEOUT
  Retryable   : YES
  Explanation : No response within 60s. Check connection or increase VIMEO_TIMEOUT.
  Raw Error   : Timeout after 60s
  Action      : Retry: python download_videos.py --retry-failed
================================================================================
```

---

## 15. Error Handling

**3-Tier HTTP system:**

| Tier | Codes | Behaviour |
|---|---|---|
| Permanent | 400, 401, 403, 404, 410, 451 + 10 more | Return immediately — no retry |
| Rate limited | 429, 503 | Wait 60s then retry |
| Transient | 500, 502, 504, 520–530 + more | Retry with exponential backoff + ±2s jitter |

**Additional guards:**

| Scenario | Handling |
|---|---|
| Corrupt `progress.json` | Renamed to `.corrupt`, starts fresh |
| 0-byte or corrupt file | Detected post-download, deleted, retried |
| Disk full | Checked via `Content-Length` before writing |
| Malformed URL | Validated before any request — fails immediately |
| `KeyboardInterrupt` (Ctrl+C) | Saves progress, closes tqdm cleanly, exits with code 0 |
| SSL error | Caught separately — retried with backoff |
| Incomplete download | Detected via byte count comparison — deleted and retried |
| OS error (disk/permissions) | Caught as `OSError` — returned immediately, no retry |

---

## 16. Resume from Failure

`progress.json` tracks every Video ID with a status:

```json
{
  "1156438752": { "status": "ok",      "title": "PWC Delhi..." },
  "1156438753": { "status": "failed",  "title": "...", "reason": "HTTP 403" },
  "1156438754": { "status": "pending", "title": "..." }
}
```

| Status | Meaning |
|---|---|
| `pending` | Registered but not yet attempted |
| `ok` | Successfully downloaded and verified on disk |
| `failed` | Failed after all retries — reason stored |
| `skipped` | File already existed on disk |
| `no_url` | Row had no download URL in input file |

- Written after **every single video** — atomic write (`.tmp` → rename)
- On restart — `ok` entries skipped, `pending`/`failed` re-attempted
- Startup disk check — `ok` entries without files on disk reset to `pending`
- To retry only failed: `python download_videos.py --retry-failed`

---

