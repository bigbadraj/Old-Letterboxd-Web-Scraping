import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading

# Thread-safe list for storing movie data
class ThreadSafeList:
    def __init__(self):
        self.items = []
        self.lock = threading.Lock()
    
    def append(self, item):
        with self.lock:
            self.items.append(item)
    
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
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session

def process_film(session, film_url, movies_data):
    try:
        film_response = session.get(f"https://letterboxd.com{film_url}", timeout=10)
        film_response.raise_for_status()
        film_soup = BeautifulSoup(film_response.content, 'html.parser')
        
        og_title = film_soup.find('meta', property='og:title')
        if og_title:
            title_text = og_title['content']
            
            year = ''
            if '(' in title_text and ')' in title_text:
                year = title_text[title_text.rindex('(')+1:title_text.rindex(')')]
                title = title_text[:title_text.rindex('(')].strip()
            else:
                title = title_text
            
            movies_data.append({'Title': title, 'Year': year})
            
            if len(movies_data) % 10 == 0:
                print(f'Scraped {len(movies_data)} titles. Latest: {title}')
            
            sleep(0.05)  # Reduced sleep time
    except Exception as e:
        print(f"Error processing film {film_url}: {e}")

def process_page(session, url, movies_data, max_films):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        film_list = soup.find('ul', class_='js-list-entries poster-list -p125 -grid film-list')
        
        if not film_list:
            return False
            
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for li in film_list.find_all('li', class_='poster-container'):
                if max_films and len(movies_data) >= max_films:
                    return False
                    
                film_url = li.find('div', class_='film-poster').get('data-target-link')
                if film_url:
                    futures.append(
                        executor.submit(process_film, session, film_url, movies_data)
                    )
            
            for future in as_completed(futures):
                future.result()
        
        return bool(soup.find('a', class_='next'))
    except Exception as e:
        print(f"Error processing page {url}: {e}")
        return False

def main():
    # Configure these as needed
    base_url = "https://letterboxd.com/bigbadraj/list/every-movie-ive-seen-ranked/"
    max_films = None  # Set to None for no limit
    
    if base_url[-1] != '/':
        base_url += '/'
    
    session = create_session()
    movies_data = ThreadSafeList()
    page = 1
    
    while True:
        url = f'{base_url}page/{page}/'
        has_next = process_page(session, url, movies_data, max_films)
        
        if not has_next or (max_films and len(movies_data) >= max_films):
            break
            
        page += 1
    
    df = pd.DataFrame(movies_data.items)
    output_csv = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs\film_titles.csv'
    df.to_csv(output_csv, index=False)
    print(f"\nScraping complete. {len(movies_data)} films saved to {output_csv}")

if __name__ == "__main__":
    main()