import os
import csv
from pathlib import Path
import yt_dlp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ==================== CONFIGURATION ====================
CSV_FILE = "bachata.csv"                    # CSV filename
DOWNLOAD_FOLDER = "Bachata"       # Base download folder
MAX_WORKERS = 8                           # Number of parallel downloads
ENABLE_CATEGORIZATION = False              # Set to False to skip categorization
# =======================================================

print_lock = threading.Lock()

def sanitize_filename(filename):
    for c in '<>:"/\\|?*':
        filename = filename.replace(c, '')
    return filename.strip('. ')[:200]

def create_folders(base, enable_cat):
    if enable_cat:
        folders = {
            'salsa': base / "Salsa",
            'bachata': base / "Bachata",
            'reggaeton': base / "Reggaeton",
            'kizomba': base / "Kizomba_Zouk",
            'other': base / "Other"
        }
    else:
        folders = {'all': base}
    
    for f in folders.values():
        f.mkdir(parents=True, exist_ok=True)
    return folders

def categorize(title, artist):
    if not ENABLE_CATEGORIZATION:
        return 'all'
    
    text = (title + " " + artist).lower()
    
    bachata_kw = ['romeo santos', 'aventura', 'prince royce', 'monchy', 'frank reyes', 
                  'zacarias ferreira', 'xtreme', 'domenic marte', 'yoskar sarante', 
                  'mickey then', 'bachata']
    if any(k in text for k in bachata_kw):
        return 'bachata'
    
    salsa_kw = ['marc anthony', 'celia cruz', 'joe arroyo', 'willie colon', 'issac delgado',
                'sonora', 'fruko', 'gran combo', 'johnny pacheco', 'pupy', 'maykel blanco',
                'van van', 'timbalive', 'manolito', 'trabuco', 'adalberto alvarez',
                'alexander abreu', 'havana', 'elito reve', 'lazarito', 'bamboleo',
                'aymee nuviola', 'masiel malaga', 'salsa', 'casino']
    if any(k in text for k in salsa_kw):
        return 'salsa'
    
    reggaeton_kw = ['daddy yankee', 'ozuna', 'bad bunny', 'j balvin', 'nicky jam',
                    'maluma', 'anuel', 'karol g', 'becky g', 'cnco', 'wisin', 'don omar',
                    'arcangel', 'farruko', 'reggaeton', 'luis fonsi', 'despacito']
    if any(k in text for k in reggaeton_kw):
        return 'reggaeton'
    
    kizomba_kw = ['kizomba', 'zouk', 'badoxa', 'tarraxinha', 'pinto picasso', 'mayinbito',
                  'dj faze', 'dj husky', 'chris paradise', 'jalil lopez', 'lola jane']
    if any(k in text for k in kizomba_kw):
        return 'kizomba'
    
    return 'other'

def download(song_data, folder):
    title = song_data.get('title', 'Unknown')
    artist = song_data.get('artist', '')
    link = song_data.get('link', '')
    num = song_data.get('number', '')
    
    # Determine what to download
    if link:
        query = link  # Direct YouTube link
        display_name = f"{title} - {artist}" if artist else title
    elif title and artist:
        query = f"ytsearch1:{title} {artist}"
        display_name = f"{title} - {artist}"
    elif title:
        query = f"ytsearch1:{title}"
        display_name = title
    else:
        return (num, False, "No title or link provided")
    
    # Create filename
    if num:
        filename_base = f"{num} - {sanitize_filename(title)}"
    else:
        filename_base = sanitize_filename(title)
    
    if artist:
        filename_base += f" - {sanitize_filename(artist)}"
    
    outfile = str(folder / f"{filename_base}.%(ext)s")
    
    opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '320',
        }],
        'outtmpl': outfile,
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
    }
    
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            with print_lock:
                status = f"[{num}] " if num else ""
                print(f"{status}Downloading: {display_name}")
            ydl.download([query])
            with print_lock:
                print(f"{status}✓ Complete: {display_name}")
            return (num, True, None)
    except Exception as e:
        with print_lock:
            status = f"[{num}] " if num else ""
            print(f"{status}✗ Failed: {display_name} - {str(e)}")
        return (num, False, str(e))

def download_task(song_data, folders):
    title = song_data.get('title', '')
    artist = song_data.get('artist', '')
    cat = categorize(title, artist)
    return download(song_data, folders[cat])

def parse_csv_row(row, headers):
    """Intelligently parse CSV row with flexible column names"""
    song_data = {}
    
    # Try to find number/index
    for key in ['number', 'num', 'index', 'id', '#']:
        if key in headers:
            song_data['number'] = row.get(key, '').strip()
            break
    
    # Try to find title
    for key in ['title', 'song', 'name', 'track']:
        if key in headers:
            song_data['title'] = row.get(key, '').strip()
            break
    
    # Try to find artist
    for key in ['artist', 'artists', 'by', 'singer']:
        if key in headers:
            song_data['artist'] = row.get(key, '').strip()
            break
    
    # Try to find YouTube link
    for key in ['link', 'url', 'youtube', 'youtube_link', 'yt_link']:
        if key in headers:
            song_data['link'] = row.get(key, '').strip()
            break
    
    return song_data

def main():
    print("="*70)
    print("YouTube Music Downloader - Flexible & Parallel")
    print("="*70)
    
    base = Path(os.getcwd()) / DOWNLOAD_FOLDER
    folders = create_folders(base, ENABLE_CATEGORIZATION)
    csv_file = Path(os.getcwd()) / CSV_FILE
    
    if not csv_file.exists():
        print(f"ERROR: {CSV_FILE} not found in {os.getcwd()}")
        return
    
    # Read all songs
    songs = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = [h.lower() for h in reader.fieldnames]
        
        for row in reader:
            # Convert headers to lowercase for case-insensitive matching
            row_lower = {k.lower(): v for k, v in row.items()}
            song_data = parse_csv_row(row_lower, headers)
            
            # Only add if we have at least title or link
            if song_data.get('title') or song_data.get('link'):
                songs.append(song_data)
    
    if not songs:
        print("ERROR: No valid songs found in CSV!")
        return
    
    print(f"\nCSV File: {CSV_FILE}")
    print(f"Download Location: {base}")
    print(f"Total songs: {len(songs)}")
    print(f"Parallel downloads: {MAX_WORKERS}")
    print(f"Categorization: {'Enabled' if ENABLE_CATEGORIZATION else 'Disabled'}")
    print("="*70 + "\n")
    
    success = 0
    failed = 0
    failed_songs = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
        for num, error in failed_songs[:10]:  # Show first 10
            print(f"  #{num}: {error}")
        if len(failed_songs) > 10:
            print(f"  ... and {len(failed_songs) - 10} more")

if __name__ == "__main__":
    main()
