import os
import csv
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import yt_dlp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

# ==================== HARD-CODED DEFAULT CONFIG ====================
DEFAULT_CSV_FILE = "bachata.csv"
DEFAULT_DOWNLOAD_FOLDER = "Downloads"
DEFAULT_MAX_WORKERS = 4
DEFAULT_MODE = "audio"  # audio | video | both
DEFAULT_ENABLE_LOGS = False
# ===================================================================

print_lock = threading.Lock()
progress_lock = threading.Lock()
progress_positions = {}
position_counter = 0


# ==================== UTILITIES ====================

def sanitize_filename(filename):
    for c in '<>:"/\\|?*':
        filename = filename.replace(c, '')
    return filename.strip('. ')[:200]


def setup_logging(enable_logs):
    if not enable_logs:
        return None

    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"download_run_{timestamp}.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return log_file


def log(msg):
    logging.info(msg)


# ==================== PROGRESS BAR ====================

def create_progress_hook(desc, position):

    pbar = tqdm(
        total=100,
        desc=desc[:30],
        position=position,
        leave=True,
        unit="%"
    )

    def hook(d):
        if d['status'] == 'downloading':
            percent = d.get('_percent_str', '0%').strip().replace('%', '')
            try:
                percent = float(percent)
                pbar.n = percent
                pbar.refresh()
            except:
                pass

        elif d['status'] == 'finished':
            pbar.n = 100
            pbar.refresh()
            pbar.close()

    return hook


# ==================== YDL OPTIONS ====================

def build_ydl_options(mode, output_template, progress_hook):
    base_opts = {
        'outtmpl': output_template,
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'continuedl': True,
        'overwrites': False,
        'progress_hooks': [progress_hook],
    }

    if mode == "audio":
        base_opts.update({
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',
            }],
        })

    elif mode == "video":
        base_opts.update({
            'format': 'bestvideo+bestaudio/best',
            'merge_output_format': 'mp4',
        })

    return base_opts


# ==================== DOWNLOAD ====================

def download_single(query, filename_base, folder, mode, number=None):
    global position_counter

    with progress_lock:
        position = position_counter
        position_counter += 1

    status = f"[{number}] " if number else ""
    desc = f"{status}{filename_base}"

    try:
        log(f"{status}Downloading: {filename_base}")

        if mode in ["audio", "both"]:
            hook = create_progress_hook(desc + " (Audio)", position)
            audio_out = str(folder / f"{filename_base}.%(ext)s")
            opts = build_ydl_options("audio", audio_out, hook)
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([query])

        if mode in ["video", "both"]:
            hook = create_progress_hook(desc + " (Video)", position)
            video_out = str(folder / f"{filename_base}.%(ext)s")
            opts = build_ydl_options("video", video_out, hook)
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([query])

        log(f"{status}✓ Complete")
        return True, None

    except Exception as e:
        log(f"{status}✗ Failed - {str(e)}")
        print(f"{status}✗ Failed: {filename_base}")
        return False, str(e)


# ==================== CSV PARSER ====================

def parse_csv_row(row):
    headers = row.keys()
    song_data = {}

    for key in ['number', 'num', 'index', 'id', '#']:
        if key in headers:
            song_data['number'] = row.get(key, '').strip()
            break

    for key in ['title', 'song', 'name', 'track']:
        if key in headers:
            song_data['title'] = row.get(key, '').strip()
            break

    for key in ['artist', 'artists', 'by', 'singer']:
        if key in headers:
            song_data['artist'] = row.get(key, '').strip()
            break

    for key in ['link', 'url', 'youtube', 'youtube_link', 'yt_link']:
        if key in headers:
            song_data['link'] = row.get(key, '').strip()
            break

    return song_data


# ==================== MAIN ====================

def main():

    parser = argparse.ArgumentParser(description="YouTube Downloader PRO v5")
    parser.add_argument("--csv", help="CSV file input")
    parser.add_argument("--link", help="Single standalone link")
    parser.add_argument("--mode", choices=["audio", "video", "both"])
    parser.add_argument("--workers", type=int)
    parser.add_argument("--folder", help="Download folder")
    parser.add_argument("--logs", action="store_true")

    args = parser.parse_args()

    csv_file = args.csv or DEFAULT_CSV_FILE
    single_link = args.link
    mode = args.mode or DEFAULT_MODE
    max_workers = args.workers or DEFAULT_MAX_WORKERS
    download_folder = args.folder or DEFAULT_DOWNLOAD_FOLDER
    enable_logs = args.logs or DEFAULT_ENABLE_LOGS

    log_file = setup_logging(enable_logs)

    base = Path(os.getcwd()) / download_folder
    base.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("YouTube Downloader PRO v5 (Realtime Progress)")
    print("=" * 70)
    print(f"Mode: {mode}")
    print(f"Download Folder: {base}")
    print(f"Workers: {max_workers}")
    print(f"Logging: {'Enabled' if enable_logs else 'Disabled'}")
    if log_file:
        print(f"Log File: {log_file}")
    print("=" * 70)

    # -------- Single Link Mode --------
    if single_link:
        filename_base = sanitize_filename("Single_Download")
        download_single(single_link, filename_base, base, mode)
        return

    # -------- CSV Mode --------
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_file}")
        return

    songs = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_lower = {k.lower(): v for k, v in row.items()}
            song = parse_csv_row(row_lower)
            if song.get("title") or song.get("link"):
                songs.append(song)

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for song in songs:
            query = song.get("link") or f"ytsearch1:{song.get('title')} {song.get('artist', '')}"
            filename_base = sanitize_filename(
                f"{song.get('number','')} - {song.get('title','')}"
            )
            futures.append(
                executor.submit(
                    download_single,
                    query,
                    filename_base,
                    base,
                    mode,
                    song.get("number")
                )
            )

        for future in as_completed(futures):
            ok, _ = future.result()
            if ok:
                success += 1
            else:
                failed += 1

    print("\n" + "=" * 70)
    print("DOWNLOAD COMPLETE")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print("=" * 70)


if __name__ == "__main__":
    main()
