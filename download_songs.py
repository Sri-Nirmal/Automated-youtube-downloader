import os
import csv
from pathlib import Path
import yt_dlp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

print_lock = threading.Lock()

def sanitize_filename(filename):
    for c in '<>:"/\\|?*':
        filename = filename.replace(c, '')
    return filename.strip('. ')[:200]

def create_folders(base):
    folders = {'salsa': base / "Salsa", 'bachata': base / "Bachata", 'reggaeton': base / "Reggaeton", 'other': base / "Other"}
    for f in folders.values():
        f.mkdir(parents=True, exist_ok=True)
    return folders

def categorize(title, artist):
    text = (title + " " + artist).lower()
    
    # Bachata keywords - more specific
    bachata_artists = ['romeo santos', 'aventura', 'prince royce', 'monchy', 'frank reyes', 
                       'zacarias ferreira', 'xtreme', 'domenic marte', 'daniel santacruz',
                       'yoskar sarante', 'yiyo sarante', 'mickey then', 'luis miguel del amargue']
    if 'bachata' in text or any(artist in text for artist in bachata_artists):
        return 'bachata'
    
    # Salsa keywords - more specific
    salsa_artists = ['marc anthony', 'celia cruz', 'joe arroyo', 'willie colon', 'issac delgado',
                     'sonora carruseles', 'fruko', 'gran combo', 'yuri buenaventura', 
                     'johnny pacheco', 'pupy', 'maykel blanco', 'van van', 'timbalive',
                     'manolito', 'trabuco', 'adalberto alvarez', 'alexander abreu', 
                     'havana d primera', 'elito reve', 'lazarito valdes', 'bamboleo',
                     'aymee nuviola', 'masiel malaga']
    if 'salsa' in text or 'casino' in text or any(artist in text for artist in salsa_artists):
        return 'salsa'
    
    # Reggaeton keywords
    reggaeton_artists = ['daddy yankee', 'ozuna', 'bad bunny', 'j balvin', 'nicky jam',
                         'maluma', 'anuel', 'karol g', 'becky g', 'cnco', 'wisin',
                         'don omar', 'arcangel', 'farruko', 'zion', 'lennox']
    if 'reggaeton' in text or any(artist in text for artist in reggaeton_artists):
        return 'reggaeton'
    
    return 'other'

def download(title, artist, folder, num):
    query = f"{title} {artist}"
    outfile = str(folder / f"{num:03d} - {sanitize_filename(title)} - {sanitize_filename(artist)}.%(ext)s")
    opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '320'}],
        'outtmpl': outfile,
        'quiet': True,
        'no_warnings': True,
        'default_search': 'ytsearch1',
        'nocheckcertificate': True,
        'ignoreerrors': False,
    }
    
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            with print_lock:
                print(f"[{num}/160] Downloading: {title} - {artist}")
            ydl.download([query])
            with print_lock:
                print(f"[{num}/160] ✓ Complete: {title}")
            return (num, True, None)
    except Exception as e:
        with print_lock:
            print(f"[{num}/160] ✗ Failed: {title} - {str(e)}")
        return (num, False, str(e))

def download_task(row, folders):
    num = int(row['number'])
    title = row['title']
    artist = row['artist']
    cat = categorize(title, artist)
    return download(title, artist, folders[cat], num)

def main():
    print("="*70)
    print("YouTube Music Downloader - Social Dance Mix (Parallel Mode)")
    print("="*70)
    
    base = Path(os.getcwd())
    folders = create_folders(base)
    csv_file = base / "songs.csv"
    
    if not csv_file.exists():
        print(f"ERROR: songs.csv not found in {base}")
        return
    
    # Read all songs
    songs = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        songs = list(reader)
    
    print(f"\nTotal songs to download: {len(songs)}")
    print(f"Using {min(4, os.cpu_count() or 2)} parallel downloads")
    print("="*70 + "\n")
    
    success = 0
    failed = 0
    failed_songs = []
    
    # Use ThreadPoolExecutor for parallel downloads
    # Limit to 4 concurrent downloads to avoid rate limiting
    max_workers = min(4, os.cpu_count() or 2)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_task, song, folders) for song in songs]
        
        for future in as_completed(futures):
            num, ok, error = future.result()
            if ok:
                success += 1
            else:
                failed += 1
                failed_songs.append((num, error))
    
    print("\n" + "="*70)
    print(f"DOWNLOAD COMPLETE!")
    print(f"Success: {success} | Failed: {failed}")
    print("="*70)
    
    if failed_songs:
        print("\nFailed downloads:")
        for num, error in failed_songs:
            print(f"  Song #{num}: {error}")

if __name__ == "__main__":
    main()
