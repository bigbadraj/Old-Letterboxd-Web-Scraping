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
        
        # Try both ranked and unranked list classes
        film_list = soup.find('ul', class_='js-list-entries poster-list -p125 -grid film-list') or \
                   soup.find('ul', class_='poster-list -p125 -grid film-list')
        
        if not film_list:
            print_to_csv("Film list not found on page.")
            return False, []
        
        temp_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for li in film_list.find_all('li', class_='poster-container'):
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
        film_list = soup.find('ul', class_='js-list-entries poster-list -p125 -grid film-list') or \
                   soup.find('ul', class_='poster-list -p125 -grid film-list')
        
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
        g = Github("YOUR API KEY HERE")
        
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
    print_to_csv("Rare Lists are being Updated")
    
    choice = 4
    
    expanded_lists_to_process = [
        {"url": "https://letterboxd.com/bigbadraj/list/every-new-york-film-critics-circle-best-film/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-national-society-of-film-critics-best/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-national-board-of-review-best-film/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-los-angeles-film-critics-association/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-producers-guild-of-america-best-theatrical/"},
        {"url": "https://letterboxd.com/elmiko_/list/directors-guild-of-america-award-winners/"},
        {"url": "https://letterboxd.com/bigbadraj/list/screen-actors-guild-outstanding-performance/"},
        {"url": "https://letterboxd.com/bigbadraj/list/gotham-awards-best-feature-winners/"},
        {"url": "https://letterboxd.com/yuriaso/list/razzie-worst-picture/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-annie-best-animated-feature-winner/"},
        {"url": "https://letterboxd.com/ruthalula/list/critics-choice-winners/"},
        {"url": "https://letterboxd.com/vedant_vashi13/list/list-of-all-winners-for-the-independent-spirit/"},
        {"url": "https://letterboxd.com/bigbadraj/list/saturn-award-winners-for-best-horror-science/"},
        {"url": "https://letterboxd.com/harmenyolo/list/tiff-peoples-choice-award-winners/"},
        {"url": "https://letterboxd.com/peterstanley/list/berlin-international-film-festival-golden/"},
        {"url": "https://letterboxd.com/cinelove/list/sundance-grand-jury-prize-winners/"},
        {"url": "https://letterboxd.com/cinelove/list/golden-lion-winners/"},
        {"url": "https://letterboxd.com/samuelelliott/list/every-oscar-nominee-ever/"},
        {"url": "https://letterboxd.com/floorman/list/every-oscar-winner-ever-1/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-bafta-best-film-winner/"},
        {"url": "https://letterboxd.com/bigbadraj/list/golden-globes-winners-for-best-drama/"},
        {"url": "https://letterboxd.com/bigbadraj/list/golden-globe-winners-for-best-comedy-musical/"},
        {"url": "https://letterboxd.com/floorman/list/oscar-winners-best-picture/"},
        {"url": "https://letterboxd.com/brsan/list/cannes-palme-dor-winners/"},
        {"url": "https://letterboxd.com/elvisisking/list/the-complete-library-of-congress-national/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-film-to-win-10-or-oscars/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-film-to-win-7-or-oscars/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-film-to-win-5-or-oscars/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-film-to-win-3-or-oscars/"},
        {"url": "https://letterboxd.com/bigbadraj/list/250-highest-grossing-movies-of-all-time/"},
        {"url": "https://letterboxd.com/peterstanley/list/1001-movies-you-must-see-before-you-die/"},
        {"url": "https://letterboxd.com/dvideostor/list/roger-eberts-great-movies/"},
        {"url": "https://letterboxd.com/crew/list/edgar-wrights-1000-favorite-movies/"},
        {"url": "https://letterboxd.com/francisfcoppola/list/movies-that-i-highly-recommend/"},
        {"url": "https://letterboxd.com/george808/list/films-where-andrew-garfield-goes-up-against/"},
        {"url": "https://letterboxd.com/michaelj/list/martin-scorseses-film-school/"},
        {"url": "https://letterboxd.com/bigbadraj/list/every-writers-guild-of-america-best-screenplay/"},
        {"url": "https://letterboxd.com/flanaganfilm/list/mike-flanagans-recommended-gateway-horror/"},
        {"url": "https://letterboxd.com/crew/list/most-fans-per-viewer-on-letterboxd-2024/"},
        {"url": "https://letterboxd.com/lesaladino/list/every-movie-referenced-watched-in-gilmore/"},
        {"url": "https://letterboxd.com/tintinabello/list/movies-where-the-protagonist-witnesses-a/"},

    ]

    # Calculate total films across all relevant lists
    session = create_session()
    lists_to_handle = expanded_lists_to_process
    total_films = sum(get_list_size(session, list_info['url']) for list_info in lists_to_handle)
    progress_tracker = ProgressTracker(total_films)
        
    for i, list_info in enumerate(lists_to_handle, 1):
        print_to_csv(f"\nProcessing list {i}/{len(lists_to_handle)}")
        base_url = list_info['url']
        list_name = base_url.rstrip('/').split('/')[-1]
        output_json = fr'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\JSONs\film_titles_{list_name}.json'
        print_to_csv(f"URL: {base_url}")
        process_single_list(base_url, output_json, progress_tracker=progress_tracker, update_github=True)
        print_to_csv(f"Completed list {i}/{len(lists_to_handle)}")

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