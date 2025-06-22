import requests
from bs4 import BeautifulSoup
import json
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from tqdm import tqdm
import time
from github import Github
import os
from datetime import datetime
import csv

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open('Outputs/All_Outputs.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

# Thread-safe list for storing movie data
class ThreadSafeList:
    def __init__(self):
        self.items = []
        self.lock = threading.Lock()
    
    def extend(self, items):
        with self.lock:
            self.items.extend(items)
    
    def __len__(self):
        return len(self.items)

def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session

def process_film(session, film_url, progress_tracker, list_number=None):
    retries = 3
    for attempt in range(retries):
        try:
            film_response = session.get(f"https://letterboxd.com{film_url}", timeout=10)
            film_response.raise_for_status()
            film_soup = BeautifulSoup(film_response.content, 'html.parser')
            
            og_title = film_soup.find('meta', property='og:title')
            if og_title:
                title_text = og_title['content']
                
                # Extract year and title
                year = ''
                if '(' in title_text and ')' in title_text:
                    year = title_text[title_text.rindex('(')+1:title_text.rindex(')')]
                    title = title_text[:title_text.rindex('(')].strip()
                else:
                    title = title_text
                
                film_poster_div = film_soup.find('div', class_='film-poster')
                film_id = film_poster_div.get('data-film-id') if film_poster_div else "Unknown"
                
                current = progress_tracker.increment()
                print_to_csv(f"✅ {title_text} - Added ({current}/{progress_tracker.total_films})")
                return {'ListNumber': list_number, 'Title': title, 'Year': year, 'ID': film_id} if list_number is not None else {'Title': title, 'Year': year, 'ID': film_id}
            
            break
        except Exception as e:
            print_to_csv(f"❌ Error processing film {film_url}, attempt {attempt + 1}/{retries}: {e}")
            sleep(1)
    return None

def process_page(session, url, max_films, progress_tracker):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Updated selector
        film_list = soup.find('ul', class_='poster-list')

        if not film_list:
            print_to_csv("Film list not found on page.")
        
        temp_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            film_items = film_list.find_all('li', class_='poster-container')
            for li in film_items:
                film_poster = li.find('div', class_='film-poster')
                if not film_poster:
                    print_to_csv("Film poster not found for one item; skipping.")
                    continue
                    
                film_url = film_poster.get('data-target-link')
                list_number_tag = li.find('p', class_='list-number')
                
                # Only get list_number if the tag exists; otherwise, it is unranked
                list_number = int(list_number_tag.text.strip()) if list_number_tag else None
                # uncomment for more details print_to_csv(f"Processing film URL: {film_url}, List Number: {list_number}")
                
                # Process film regardless of whether there's a list number
                if film_url:
                    futures.append(executor.submit(process_film, session, film_url, progress_tracker, list_number))
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    temp_data.append(result)
                    # uncomment for more details print_to_csv(f"Processed film: {result}")
        
        has_next = bool(soup.find('a', class_='next'))
        return has_next, temp_data
    except Exception as e:
        print_to_csv(f"Error processing page {url}: {e}")
        return False, []

def get_list_size(session, base_url):
    try:
        response = session.get(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get count from meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            content = meta_desc.get('content', '')
            if 'A list of ' in content and ' films' in content:
                # Remove commas before converting to int
                number_str = content.split('A list of ')[1].split(' films')[0]
                return int(number_str.replace(',', ''))
        
        # Fallback to calculating from page count if meta description fails
        film_list = soup.find('ul', class_='poster-list') or \
                   soup.find('div', class_='poster-list') # Added div as fallback
        
        films_per_page = len(film_list.find_all('li', class_='poster-container')) if film_list else 0
        pagination = soup.find_all('li', class_='paginate-page')
        total_pages = int(pagination[-1].text) if pagination else 1
        
        return films_per_page * total_pages
    except Exception as e:
        print_to_csv(f"Error getting list size: {e}")
        return 0

class ProgressTracker:
    def __init__(self, total_films):
        self.total_films = total_films
        self.current_count = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
    
    def increment(self):
        with self.lock:
            self.current_count += 1
            return self.current_count
    
    def get_elapsed_time(self):
        return time.time() - self.start_time

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def update_github_file(filename, file_content):
    """
    Updates or creates a file in the GitHub repository.
    """
    try:
        # Initialize Github with your access token
        g = Github("")
        
        # Get the repository
        repo = g.get_repo("bigbadraj/Letterboxd-List-JSONs")
        
        # Get just the filename without path
        base_filename = os.path.basename(filename)
        
        try:
            # Try to get existing file
            contents = repo.get_contents(base_filename)
            # If file exists, update it
            repo.update_file(
                contents.path,
                f"Updated {base_filename} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                file_content,
                contents.sha
            )
            print_to_csv(f"✅ Successfully updated {base_filename} on GitHub")
        except Exception:
            # If file doesn't exist, create it
            repo.create_file(
                base_filename,
                f"Added {base_filename} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                file_content
            )
            print_to_csv(f"✅ Successfully created {base_filename} on GitHub")
            
    except Exception as e:
        print_to_csv(f"❌ Error updating GitHub: {str(e)}")

def main():
    print_to_csv("Updating All Common Lists")

    # Define the lists of URLs to process
    lists_to_process = [
        {"url": "https://letterboxd.com/slinkyman/list/letterboxds-top-250-highest-rated-short-films/"},
        {"url": "https://letterboxd.com/slinkyman/list/letterboxds-top-250-highest-rated-narrative/"},
        {"url": "https://letterboxd.com/louferrigno/list/the-anti-letterboxd-250/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-2500-most-popular-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-2500-highest-rated-narrative-feature/"},
        {"url": "https://letterboxd.com/darrencb/list/letterboxds-top-250-horror-films/"},
        {"url": "https://letterboxd.com/lifeasfiction/list/letterboxd-100-animation/"},
        {"url": "https://letterboxd.com/dave/list/imdb-top-250/"},
        {"url": "https://letterboxd.com/jack/list/official-top-250-documentary-films/"},
        {"url": "https://letterboxd.com/matthew/list/all-time-worldwide-box-office/"},
        {"url": "https://letterboxd.com/jack/list/women-directors-the-official-top-250-narrative/"},
        {"url": "https://letterboxd.com/jack/list/black-directors-the-official-top-100-narrative/"},
        {"url": "https://letterboxd.com/jack/list/official-top-250-films-with-the-most-fans/"},
        {"url": "https://letterboxd.com/offensivename/list/top-100-concert-films-digital-albums/"},
        {"url": "https://letterboxd.com/dave/list/letterboxd-top-250-films-history-collected/"},
        {"url": "https://letterboxd.com/thisisdrew/list/the-most-controversial-films-on-letterboxd/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-things-on-letterboxd/"},
        {"url": "https://letterboxd.com/ben_macdonald/list/guillermo-del-toros-twitter-film-recommendations/"},
        {"url": "https://letterboxd.com/bigbadraj/list/highest-grossing-movies-of-all-time-adjusted/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-grossing-movies-of-all-time-1/"},
        {"url": "https://letterboxd.com/imthelizardking/list/rotten-tomatoes-300-best-movies-of-all-time/"},
        {"url": "https://letterboxd.com/browsehorror/list/horror-movies-everyone-should-watch-at-least/"},
        {"url": "https://letterboxd.com/fcbarcelona/list/movies-everyone-should-watch-at-least-once/"},
        {"url": "https://letterboxd.com/prof_ratigan/list/top-5000-films-of-all-time-calculated/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-movie-ive-seen-ranked/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-100-highest-rated-stand-up-comedy-specials/"},
        {"url": "https://letterboxd.com/andregps/list/letterboxd-four-favorites-interviews/"},
        {"url": "https://letterboxd.com/mattheweg/list/the-top-rated-movie-of-every-year-by-letterboxd/"},
        {"url": "https://letterboxd.com/rileyaust/list/movies-where-a-5-star-rating-is-most-common/"},
        {"url": "https://letterboxd.com/jonny5244/list/billion-dollar-movies/"},
        {"url": "https://letterboxd.com/desdemoor/list/letterboxd-113-highest-rated-19th-century/"},
        {"url": "https://letterboxd.com/offensivename/list/official-top-50-narrative-feature-films-under/"},
        {"url": "https://letterboxd.com/stateofhailey/list/letterboxds-top-250-romantic-comedy-films/"},
        {"url": "https://letterboxd.com/jumpy/list/letterboxds-official-top-250-anime-tv-miniseries/"},
        {"url": "https://letterboxd.com/jbutts15/list/the-complete-criterion-collection/"},
        {"url": "https://letterboxd.com/flanaganfilm/list/flanagans-favorites-my-top-100/"},
        {"url": "https://letterboxd.com/zishi/list/four-greatest-films-of-each-year-according/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-action-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-adventure-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-animation-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-comedy-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-crime-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-drama-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-family-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-fantasy-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-music-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-romantic-comedy-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-romance-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-science-fiction-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-thriller-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-war-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-western-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-100-g-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-pg-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-pg-13-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-r-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-20-nc-17-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-nr-rated-narrative-feature-films/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-north-american-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-south-american-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-south-american-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-european-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-100-highest-rated-african-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-asian-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-75-highest-rated-australian-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-250-highest-rated-films-of-90-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-250-highest-rated-films-of-120-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-150-highest-rated-films-of-180-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-20-highest-rated-films-of-240-minutes/"},
        {"url": "https://letterboxd.com/arhodes/list/list-of-box-office-number-one-films-in-the/"},
        {"url": "https://letterboxd.com/arhodes/list/biggest-box-office-bombs-adjusted-for-inflation/"},
        {"url": "https://letterboxd.com/arhodes/list/highest-grossing-film-by-year-of-release/"},
        {"url": "https://letterboxd.com/arhodes/list/most-popular-film-for-every-year-on-letterboxd/"},
        {"url": "https://letterboxd.com/arhodes/list/most-expensive-films-adjusted-for-inflation/"},
        {"url": "https://letterboxd.com/arhodes/list/most-expensive-films-unadjusted-for-inflation/"},
        {"url": "https://letterboxd.com/blackkfoxx/list/top-250-movies-by-unweighted-rating/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-highest-rated-horror-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-action-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-adventure-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-animation-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-comedy-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-crime-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-drama-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-family-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-fantasy-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-history-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-horror-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-music-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-mystery-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-romance-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-science-fiction-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-thriller-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-western-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-war-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-200-most-popular-g-rated-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-pg-rated-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-pg-13-rated-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-r-rated-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-25-most-popular-nc-17-rated-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-nr-rated-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-north-american-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-100-most-popular-south-american-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-european-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-250-most-popular-asian-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-20-most-popular-african-narrative-feature/"},
        {"url": "https://letterboxd.com/bigbadraj/list/top-150-most-popular-australian-narrative/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-250-most-popular-films-of-90-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-250-most-popular-films-of-120-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-75-most-popular-films-of-180-minutes/"},
        {"url": "https://letterboxd.com/bigbadraj/list/the-top-5-most-popular-films-of-240-minutes/"},
    ]
    
    # Calculate total films across all relevant lists
    session = create_session()
    total_films = sum(get_list_size(session, list_info['url']) for list_info in lists_to_process)
    progress_tracker = ProgressTracker(total_films)
        
    for i, list_info in enumerate(lists_to_process, 1):
        print_to_csv(f"\nProcessing list {i}/{len(lists_to_process)}")
        base_url = list_info['url']
        list_name = base_url.rstrip('/').split('/')[-1]
        output_json = fr'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\JSONs\film_titles_{list_name}.json'
        print_to_csv(f"URL: {base_url}")
        process_single_list(base_url, output_json, progress_tracker=progress_tracker, update_github=True)
        print_to_csv(f"Completed list {i}/{len(lists_to_process)}")

def process_single_list(base_url, output_json, progress_tracker, max_films=None, update_github=True):
    session = create_session()
    all_data = ThreadSafeList()
    current_page = 1
    
    # Get total number of pages first
    response = session.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    pagination = soup.find_all('li', class_='paginate-page')
    total_pages = int(pagination[-1].text) if pagination else 1
    
    with tqdm(
        total=total_pages, 
        desc="Processing pages", 
        unit=" pages",
        bar_format="{desc}: {percentage:3.0f}% |{bar}| {n_fmt}/{total_fmt} pages"
    ) as pbar:
        while True:
            page_url = f"{base_url}page/{current_page}/" if current_page > 1 else base_url
            print_to_csv(f"\n{f' Page {current_page}/{total_pages} ':=^100}")
            has_next, page_data = process_page(session, page_url, max_films, progress_tracker)
            
            if page_data:
                all_data.extend(page_data)
            
            # Calculate overall progress
            total_time = progress_tracker.get_elapsed_time()
            current_movies_per_second = progress_tracker.current_count / total_time if total_time > 0 else 0
            estimated_total_time = progress_tracker.total_films / current_movies_per_second if current_movies_per_second > 0 else 0
            time_remaining = estimated_total_time - total_time if estimated_total_time > 0 else 0
            
            print_to_csv(f"{f'Overall Progress: {progress_tracker.current_count}/{progress_tracker.total_films} films':^100}")
            print_to_csv(f"{f'Elapsed Time: {format_time(total_time)} | Estimated Time Remaining: {format_time(time_remaining)}':^100}")
            print_to_csv(f"{f'Processing Speed: {current_movies_per_second:.2f} movies/second':^100}")
            
            pbar.update(1)
            
            if not has_next or (max_films and len(all_data) >= max_films):
                break
                
            current_page += 1
            sleep(1)

    # Before saving to JSON, sort the data if it contains ListNumber
    final_data = all_data.items
    if any('ListNumber' in item for item in final_data):
        final_data = sorted(final_data, key=lambda x: x.get('ListNumber', float('inf')))

    # Save to JSON file
    with open(output_json, 'w', encoding='utf-8') as f:
        json_content = json.dumps(final_data, ensure_ascii=False, indent=2)
        f.write(json_content)
        
    # Update GitHub repository
    if update_github:
        update_github_file(output_json, json_content)
    
    print_to_csv(f"\nSaved {len(all_data)} films to {output_json}")
    print_to_csv(f"Total time elapsed: {format_time(total_time)}")
    print_to_csv(f"Processing speed: {current_movies_per_second:.2f} movies/second")

if __name__ == "__main__":
    main()