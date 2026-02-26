# YouTube Downloader PRO v5

A high-performance, parallel YouTube downloader supporting:

-   🎵 Audio (320 kbps MP3)
-   🎬 Highest-quality video (4K if available)
-   🔁 Resume support
-   ⏩ Skip existing files
-   📊 Real-time progress bars
-   🧾 CSV batch downloads
-   🔗 Single standalone link downloads
-   🧠 CLI + hardcoded configuration
-   🗂 Optional structured logging

------------------------------------------------------------------------

## Installation

### 1. Install Python dependencies

``` bash
pip install yt-dlp tqdm
```

### 2. Install FFmpeg (Required)

FFmpeg is required for: - Audio conversion (MP3) - Merging video + audio
streams

Verify installation:

``` bash
ffmpeg -version
```

------------------------------------------------------------------------

## Usage

### Download from CSV

``` bash
python downloader.py --csv bachata.csv --mode audio
```

### Download Single Link

``` bash
python downloader.py --link https://youtube.com/xxxxx --mode video
```

### Enable Logging

``` bash
python downloader.py --logs
```

Log files are saved automatically in:

    logs/download_run_YYYYMMDD_HHMMSS.log

------------------------------------------------------------------------

## CLI Arguments

  Argument      Description
  ------------- ------------------------------
  `--csv`       CSV file input
  `--link`      Single standalone link
  `--mode`      `audio`, `video`, or `both`
  `--workers`   Number of parallel downloads
  `--folder`    Output folder
  `--logs`      Enable logging

------------------------------------------------------------------------

## CSV Format

Supported flexible headers:

-   Title: `title`, `song`, `name`, `track`
-   Artist: `artist`, `artists`, `singer`
-   Link: `link`, `url`, `youtube`, `youtube_link`, `yt_link`
-   Number: `number`, `id`, `index`, `#`

Only title or link is required.

------------------------------------------------------------------------

## How Highest Quality Works

Video mode uses:

    bestvideo+bestaudio/best

This ensures: - Highest available resolution - Best audio stream -
Automatic merging via FFmpeg

------------------------------------------------------------------------

## License

Personal License
