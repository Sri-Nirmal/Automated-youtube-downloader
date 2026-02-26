import os
import csv
from pathlib import Path
import yt_dlp

def sanitize_filename(filename):
    for c in '<>:"/\\|?*':
        filename = filename.replace(c, '')
    return filename.strip('. ')[:200]

def create_folders(base):
    folders = {'salsa': base / "Salsa", 'bachata': base / "Bachata", 'other': base / "Other"}
    for f in folders.values():
        f.mkdir(parents=True, exist_ok=True)
    return folders

def categorize(title, artist):
    text = (title + " " + artist).lower()
    if any(k in text for k in ['bachata', 'romeo santos', 'aventura', 'prince royce', 'monchy', 'alexandra', 'frank reyes', 'zacarias', 'xtreme']):
        return 'bachata'
    if any(k in text for k in ['salsa', 'timbalive', 'marc anthony', 'celia cruz', 'sonora', 'joe arroyo', 'havana', 'pupy', 'casino', 'fruko', 'gran combo', 'willie', 'issac', 'manolito', 'maykel']):
        return 'salsa'
    return 'other'

def download(title, artist, folder, num):
    query = f"{title} {artist}"
    outfile = str(folder / f"{num:03d} - {sanitize_filename(title)} - {sanitize_filename(artist)}.%(ext)s")
    opts = {'format': 'bestaudio/best', 'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '320'}], 'outtmpl': outfile, 'quiet': True, 'no_warnings': True, 'default_search': 'ytsearch1', 'nocheckcertificate': True}
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            print(f"[{num}/160] {title} - {artist}")
            ydl.download([query])
            return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

def main():
    print("="*60)
    print("YouTube Music Downloader")
    print("="*60)
    base = Path(os.getcwd())
    folders = create_folders(base)
    csv_file = base / "songs.csv"
    if not csv_file.exists():
        print(f"ERROR: songs.csv not found in {base}")
        return
    success = 0
    failed = 0
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            num = int(row['number'])
            title = row['title']
            artist = row['artist']
            cat = categorize(title, artist)
            if download(title, artist, folders[cat], num):
                success += 1
            else:
                failed += 1
    print("\n" + "="*60)
    print(f"COMPLETE: {success} downloaded, {failed} failed")
    print("="*60)

if __name__ == "__main__":
    main()
