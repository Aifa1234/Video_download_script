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
7. [Input File Format](#8-input-file-format)
8. [File Naming — Video ID as Primary Key](#9-file-naming--video-id-as-primary-key)
9. [How It Works](#10-how-it-works)
10. [Logging & Output](#11-logging--output)

---

## 1. Overview

Bulk downloader for ~5,000 Vimeo videos. Reads a CSV/XLSX input file, downloads each video to a local or external drive.

**Key design decision:** Video ID is the primary key — every file is saved as `<VideoID>.mp4`, guaranteeing uniqueness regardless of duplicate titles.

---


## 2. Project Structure

```
VideoDownloader/
│
├── download_videos.py       
├── list.csv                
├── download_log.txt       
├── README.md                
│
└── downloaded_videos/      
    ├── 1156438752.mp4       
    ├── 1156438753.mp4
    └── ...
```

---

## 3. Script Architecture

| Section | Name | Description |
|---|---|---|
| 1 | Global Configuration | Retry count, chunk size, timeouts, default paths |
| 2 | Logging Configuration | Dual logging — terminal + `download_log.txt` |
| 3 | `sanitize_filename()` | Strips illegal characters from Video IDs and titles |
| 4 | `read_input_file()` | Parses `.xlsx` or `.csv` — case-insensitive header matching |
| 5 | `is_youtube_or_vimeo()` | Detects watch-page URLs requiring `yt-dlp` |
| 6 | `download_with_ytdlp()` | Downloads Vimeo/YouTube URLs — verbose error logging |
| 7 | `download_direct()` | Chunked HTTP download with tqdm progress bar + retry |
| 8 | `download_video()` | Orchestrates per-video flow — Video ID as primary key |
| 9 | `main()` | CLI args, library check, file check, download loop, summary |

---

## 4. Technologies

| Library | Type | Purpose |
|---|---|---|
| `requests` | Third-party | Chunked HTTP downloads for direct file URLs |
| `openpyxl` | Third-party | Reading `.xlsx` input files |
| `yt-dlp` | Third-party | Downloading Vimeo / YouTube watch-page URLs |
| `tqdm` | Third-party | Live terminal progress bar |
| `logging` | stdlib | Dual output — terminal + log file |
| `argparse` | stdlib | CLI argument parsing |
| `csv` | stdlib | CSV file parsing |
| `pathlib` | stdlib | Cross-platform path handling |
| `re` | stdlib | Filename sanitization |

---

## 5. Setup

```bash
# Install dependencies
pip install requests openpyxl yt-dlp tqdm

# Requires Python 3.10+
python --version
```

Place `download_videos.py` and `list.csv` in the same folder.

---

## 6. Usage

```bash
# Default — reads list.csv, saves to ./downloaded_videos
python download_videos.py

# Custom paths
python download_videos.py -i list.csv -o "E:\Vimeo_Archive"

# Full argument reference
python download_videos.py --help
```

| Argument | Short | Default | Description |
|---|---|---|---|
| `--input` | `-i` | `list.csv` | Path to `.csv` or `.xlsx` input file |
| `--output` | `-o` | `./downloaded_videos` | Destination folder for downloaded videos |

---

## 7. Input File Format

| Column | Required | Notes |
|---|---|---|
| `Video ID` | ✅ | Primary key — used as output filename |
| `Video Title` | ✅ | Logging only — not used in filename |
| `Download URL` | ✅ | Direct `.mp4` URL or Vimeo/YouTube watch-page URL |
| All other columns | ❌ | Ignored by script |

> Headers matched case-insensitively. Empty rows skipped automatically.

---

## 8. File Naming — Video ID as Primary Key

Files are saved as `<VideoID>.mp4` — not by title.

| Video ID | Title | Saved As |
|---|---|---|
| `1156438752` | PWC Delhi Energy v1 | `1156438752.mp4` |
| `1156438753` | PWC Delhi Energy v1 | `1156438753.mp4` |

**Why:** Prior to v1.2.0, filenames were title-based. Since many videos share the same title, only the first was downloaded and duplicates were wrongly skipped. Video ID guarantees uniqueness and enables reliable resume.

---

## 9. How It Works

```
main()
  ├── Parse CLI args
  ├── Check libraries (requests, openpyxl, tqdm, yt-dlp)
  ├── Verify input file exists
  ├── read_input_file() → list of {video_id, title, download_link}
  └── For each record:
        download_video()
          ├── Validate URL and Video ID
          ├── Build filename from Video ID (primary key)
          ├── Skip if <VideoID>.* already exists on disk
          ├── is_youtube_or_vimeo()?
          │     YES → download_with_ytdlp()
          │     NO  → download_direct() [chunked, progress bar, retry x3]
          └── Log: OK / SKIPPED / FAILED / NO_URL
        └── Print summary report
```

---

## 10. Logging & Output

Logs written simultaneously to terminal and `download_log.txt`.

**Format:** `timestamp  LEVEL     message`

**Sample output:**
```
============================================================
  Econ Engineering Video Downloader  v1.2.0
============================================================
Input       : list.csv
Output      : E:\Vimeo_Archive
Python      : 3.11.0
Filename Key: Video ID (primary key)
------------------------------------------------------------
requests : OK  (v2.31.0)
openpyxl : OK  (v3.1.2)
tqdm     : OK  (v4.66.1)
yt-dlp   : OK  (v2024.3.10)
------------------------------------------------------------
Found 5000 video(s) to process.
--- (1 / 5000) ---
START [ID=1156438752] PWC Delhi Energy Transition
  URL      : https://vimeo.com/1156438752
  Saving as: 1156438752.mp4
OK    [ID=1156438752]
--- (2 / 5000) ---
SKIP  [ID=1156438752] already on disk: 1156438752.mp4
============================================================
DOWNLOAD COMPLETE — SUMMARY
  Total: 5000 | Downloaded: 4950 | Skipped: 45 | Failed: 4 | No URL: 1
  Output : E:\Vimeo_Archive
  Log    : download_log.txt
============================================================
```

