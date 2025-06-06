# Import necessary libraries
import time
import random
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import requests
import re
import csv
import locale
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import unicodedata
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open('Outputs/All_Outputs.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

# Configure locale and constants
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
MAX_MOVIES = 5 # Currently using 7000
MAX_MOVIES_2500 = 2500
CHUNK_SIZE = 1900

# Category display names for statistics
category_display_names = {
    'director_counts': 'directors',
    'actor_counts': 'actors',
    'decade_counts': 'decades',
    'genre_counts': 'genres',
    'studio_counts': 'studios',
    'language_counts': 'languages',
    'country_counts': 'countries'
}

def get_ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return str(n) + suffix

# Configure settings
MIN_RATING_COUNT = 1000
MIN_RUNTIME = 40
MAX_RETRIES = 25
RETRY_DELAY = 15

# File paths
BASE_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs'
LIST_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping'
BLACKLIST_PATH = os.path.join(LIST_DIR, 'blacklist.xlsx')
WHITELIST_PATH = os.path.join(LIST_DIR, 'whitelist.xlsx')
INCOMPLETE_STATS_WHITELIST_PATH = os.path.join(LIST_DIR, 'Incomplete_Stats_Whitelist.xlsx')
ZERO_REVIEWS_PATH = os.path.join(LIST_DIR, 'Zero_Reviews.xlsx')  # Add new path

# TMDb API key
TMDB_API_KEY = 'Key'

# Filtering criteria
FILTER_KEYWORDS = {
    'concert film', 'miniseries',
    'live performance', 'filmed theater', 'live theater', 
    'stand-up comedy', 'edited from tv series'
}

FILTER_GENRES = {'Documentary'}

# Initialize stats for MAX_MOVIES_2500
max_movies_2500_stats = {
    'film_data': [],
    'director_counts': defaultdict(int),
    'actor_counts': defaultdict(int),
    'decade_counts': defaultdict(int),
    'genre_counts': defaultdict(int),
    'studio_counts': defaultdict(int),
    'language_counts': defaultdict(int),
    'country_counts': defaultdict(int)
}

@dataclass
class MovieData:
    title: str
    year: str
    tmdb_id: Optional[str] = None
    rating_count: int = 0
    runtime: int = 0
    keywords: List[str] = None
    genres: List[str] = None

class RequestsSession:
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.session.get(url, **kwargs)

def normalize_text(text):
    return unicodedata.normalize('NFKC', str(text)).strip()

class MovieProcessor:
    def __init__(self):
        self.session = RequestsSession()
        self.whitelist = None
        self.whitelist_lookup = {}
        self.incomplete_stats_whitelist = None
        self.incomplete_stats_lookup = {}
        self.zero_reviews = None
        self.zero_reviews_lookup = {}
        self.load_whitelist()
        self.load_incomplete_stats_whitelist()
        self.load_zero_reviews()
        
        # Update blacklist loading to include the Link column
        self.blacklist = pd.read_excel(BLACKLIST_PATH, header=0, names=['Title', 'Year', 'Reason', 'Link'], usecols=[0, 1, 2, 3])
        
        # Normalize titles and years in blacklist
        self.blacklist['Title'] = self.blacklist['Title'].apply(normalize_text)
        self.blacklist['Year'] = self.blacklist['Year'].astype(str).str.strip()
        # Fill empty links with empty string instead of None
        self.blacklist['Link'] = self.blacklist['Link'].fillna('')
        
        self.added_movies: Set[Tuple[str, str]] = set()
        self.film_data: List[Dict] = []
        self.rejected_data: List[List] = []
        self.unfiltered_approved: List[List] = []
        self.unfiltered_denied: List[List] = []
        
        # Statistics tracking
        self.director_counts: Dict[str, int] = {}
        self.actor_counts: Dict[str, int] = {}
        self.decade_counts: Dict[str, int] = {}
        self.genre_counts: Dict[str, int] = {}
        self.studio_counts: Dict[str, int] = {}
        self.language_counts: Dict[str, int] = {}
        self.country_counts: Dict[str, int] = {}
        self.rating_counts: Dict[str, int] = {}

    def load_whitelist(self):
        """Load and initialize the whitelist data."""
        try:
            # Read whitelist with explicit string type for Year column and include Information and Link columns
            self.whitelist = pd.read_excel(WHITELIST_PATH, header=0, names=['Title', 'Year', 'Information', 'Link'], dtype={'Year': str})
            
            # Normalize the data
            self.whitelist['Title'] = self.whitelist['Title'].apply(normalize_text)
            self.whitelist['Year'] = self.whitelist['Year'].astype(str).str.strip()
            # Fill empty links with empty string instead of None
            self.whitelist['Link'] = self.whitelist['Link'].fillna('')
            
            # Create a lookup dictionary for faster matching
            self.whitelist_lookup = {}
            for idx, row in self.whitelist.iterrows():
                key = f"{row['Title'].lower()}_{row['Year']}"
                try:
                    # Handle null/empty Information values by treating them as empty dictionaries
                    if pd.isna(row['Information']) or row['Information'] == '':
                        info = {}
                    else:
                        info = json.loads(row['Information']) if isinstance(row['Information'], str) else row['Information']
                    self.whitelist_lookup[key] = (info, idx, row['Link'])
                except (json.JSONDecodeError, TypeError):
                    # If there's any error parsing, treat it as an empty dictionary
                    info = {}
                    self.whitelist_lookup[key] = (info, idx, row['Link'])
                    continue
                
        except FileNotFoundError:
            print_to_csv("whitelist.xlsx not found. Creating new file.")
            self.whitelist = pd.DataFrame(columns=['Title', 'Year', 'Information', 'Link'])
            self.whitelist.to_excel(WHITELIST_PATH, index=False)

    def load_incomplete_stats_whitelist(self):
        """Load and initialize the incomplete stats whitelist data."""
        try:
            # Read incomplete stats whitelist with explicit string type for Year column
            self.incomplete_stats_whitelist = pd.read_excel(INCOMPLETE_STATS_WHITELIST_PATH, 
                                                          header=0, 
                                                          names=['Title', 'Year'], 
                                                          dtype={'Year': str})
            
            # Normalize the data
            self.incomplete_stats_whitelist['Title'] = self.incomplete_stats_whitelist['Title'].apply(normalize_text)
            self.incomplete_stats_whitelist['Year'] = self.incomplete_stats_whitelist['Year'].astype(str).str.strip()
            
            # Create a lookup dictionary for faster matching
            self.incomplete_stats_lookup = {}
            for _, row in self.incomplete_stats_whitelist.iterrows():
                key = f"{row['Title'].lower()}_{row['Year']}"
                self.incomplete_stats_lookup[key] = True
                
        except FileNotFoundError:
            print_to_csv("Incomplete_Stats_Whitelist.xlsx not found. Creating new file.")
            self.incomplete_stats_whitelist = pd.DataFrame(columns=['Title', 'Year'])
            self.incomplete_stats_whitelist.to_excel(INCOMPLETE_STATS_WHITELIST_PATH, index=False)

    def load_zero_reviews(self):
        """Load and initialize the zero reviews data."""
        try:
            # Check if file exists
            if os.path.exists(ZERO_REVIEWS_PATH):
                # Read zero reviews with explicit string type for Year column
                self.zero_reviews = pd.read_excel(ZERO_REVIEWS_PATH, header=0, names=['Title', 'Year', 'Blank', 'Link'], dtype={'Year': str})
                
                # Normalize the data
                self.zero_reviews['Title'] = self.zero_reviews['Title'].apply(normalize_text)
                self.zero_reviews['Year'] = self.zero_reviews['Year'].astype(str).str.strip()
                self.zero_reviews['Link'] = self.zero_reviews['Link'].fillna('')
                self.zero_reviews['Blank'] = ''  # Ensure Blank column is empty
                            
                # Create a lookup dictionary for faster matching
                self.zero_reviews_lookup = {}
                for idx, row in self.zero_reviews.iterrows():
                    key = f"{row['Title'].lower()}_{row['Year']}"
                    self.zero_reviews_lookup[key] = (row['Link'], idx)
                    
            else:
                self.zero_reviews = pd.DataFrame(columns=['Title', 'Year', 'Blank', 'Link'])
                self.zero_reviews.to_excel(ZERO_REVIEWS_PATH, index=False)
                
        except Exception as e:
            print_to_csv(f"ERROR loading zero reviews: {str(e)}")
            print_to_csv(f"ERROR type: {type(e)}")
            print_to_csv(f"ERROR details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            raise  # Re-raise the exception to see the full traceback

    def process_whitelist_info(self, info: Dict):
        """Process information from whitelist and update statistics."""
        if not isinstance(info, dict):
            print_to_csv("❌ Info is not a dictionary, skipping")
            return

        # Create film_data entry
        film_data = {
            'Title': info.get('Title'),
            'Year': info.get('Year'),
            'tmdbID': info.get('tmdbID')
        }

        # Add to film data
        self.film_data.append(film_data)

        # Process MAX_MOVIES_2500 using centralized function
        if add_to_max_movies_2500(info.get('Title'), info.get('Year'), info.get('tmdbID')):
            self.update_max_movies_2500_statistics(info.get('Title'), info.get('Year'), info.get('tmdbID'))

    def update_whitelist(self, film_title: str, release_year: str, movie_data: Dict, film_url: str = None) -> bool:
        """Update the whitelist with new movie data."""
        try:
            key = f"{film_title.lower()}_{release_year}"
            
            if key in self.whitelist_lookup:
                # Update existing entry
                _, row_idx, existing_url = self.whitelist_lookup[key]
                self.whitelist.at[row_idx, 'Information'] = json.dumps(movie_data)
                # Only update link if it's currently blank and we have a new URL
                if film_url and (not existing_url or existing_url == ''):
                    self.whitelist.at[row_idx, 'Link'] = film_url
                    print_to_csv(f"🔗 Added link to whitelist for {film_title}")
                self.whitelist_lookup[key] = (movie_data, row_idx, film_url or existing_url)
            else:
                # Add new entry
                new_row = pd.DataFrame([{
                    'Title': film_title,
                    'Year': release_year,
                    'Information': json.dumps(movie_data),
                    'Link': film_url or ''
                }])
                self.whitelist = pd.concat([self.whitelist, new_row], ignore_index=True)
                self.whitelist_lookup[key] = (movie_data, len(self.whitelist) - 1, film_url or '')
                if film_url:
                    print_to_csv(f"🔗 Added link to whitelist for {film_title}")
            
            # Save to Excel
            self.whitelist.to_excel(WHITELIST_PATH, index=False)
            self.load_whitelist()  # Reload to ensure consistency
            return True
            
        except Exception as e:
            print_to_csv(f"Error updating whitelist: {str(e)}")
            return False

    def get_whitelist_data(self, film_title: str, release_year: str = None, film_url: str = None) -> Optional[Tuple[Dict, int]]:
        """Get the whitelist data for a movie if it exists."""
        
        # If we have a URL, check for URL match first
        if film_url:
            for key, value in self.whitelist_lookup.items():
                if isinstance(value, tuple) and len(value) == 3:
                    info, row_idx, url = value
                    if url == film_url:
                        return info, row_idx
        
        # If no URL match or no URL provided, try title-only match
        matches = []
        for key, value in self.whitelist_lookup.items():
            title = key.split('_')[0]
            if title == film_title.lower():
                # Handle case where value might not have all three elements
                if isinstance(value, tuple):
                    if len(value) == 3:
                        info, row_idx, url = value
                    elif len(value) == 2:
                        info, row_idx = value
                        url = ''
                    else:
                        continue
                else:
                    continue
                matches.append((info, row_idx, url))

        if len(matches) == 1:
            return matches[0][0], matches[0][1]
        elif len(matches) > 1:
            # If we have multiple matches and a film_url, check for URL match first
            if film_url:
                for info, row_idx, url in matches:
                    if url == film_url:
                        return info, row_idx
            
                # If no URL match, try to get release year from the page
                try:
                    # Use requests session instead of Selenium for year extraction
                    response = self.session.get(film_url)
                    # Use regex to find the year in the page source
                    match = re.search(r'<meta property="og:title" content="[^"]*\((\d{4})\)"', response.text)
                    if match:
                        scraped_year = match.group(1)
                        # Now try exact match with title and scraped year
                        key = f"{film_title.lower()}_{scraped_year}"
                        if key in self.whitelist_lookup:
                            value = self.whitelist_lookup[key]
                            if isinstance(value, tuple):
                                if len(value) == 3:
                                    info, row_idx, _ = value
                                elif len(value) == 2:
                                    info, row_idx = value
                                else:
                                    return None, None
                            else:
                                return None, None
                            return info, row_idx
                except Exception as e:
                    print_to_csv(f"DEBUG: Error scraping release year: {str(e)}")
            return None, None
        
        return None, None

    def fetch_tmdb_details(self, tmdb_id: str) -> Tuple[List[str], List[str]]:
        movie_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=keywords"
        response = self.session.get(movie_url)

        if response.status_code == 200:
            movie_data = response.json()
            keywords = [keyword['name'] for keyword in movie_data['keywords']['keywords']]
            genre_elements = movie_data['genres']
            genres = [genre['name'] for genre in genre_elements]
            return keywords, genres
        else:
            if response.status_code == 401:
                print_to_csv("Check your API key.")
            return [], []

    def add_to_blacklist(self, film_title: str, release_year: str, reason: str, film_url: str = None) -> None:
        if not any((film_title.lower() == str(row['Title']).lower() and 
                   release_year == row['Year']) for _, row in self.blacklist.iterrows()):
            # Create a new row as a DataFrame
            new_row = pd.DataFrame([[film_title, release_year, reason, film_url]], 
                                 columns=['Title', 'Year', 'Reason', 'Link'])
            # Append to existing blacklist
            self.blacklist = pd.concat([self.blacklist, new_row], ignore_index=True)
            # Save back to Excel
            self.blacklist.to_excel(BLACKLIST_PATH, index=False)
            print_to_csv(f"⚫ {film_title} ({release_year}) added to blacklist {reason}")

    def is_whitelisted(self, film_title: str, release_year: str) -> bool:
        """Check if a movie is in the whitelist using the lookup dictionary."""
        key = f"{film_title.lower()}_{release_year}"
        return key in self.whitelist_lookup

    def extract_runtime(self, driver, film_title: str) -> Optional[int]:
        try:
            runtime_element = driver.find_element(By.CSS_SELECTOR, 'p.text-link.text-footer')
            runtime_text = runtime_element.text
            match = re.search(r'(\d+)\s*mins', runtime_text)
            if match:
                runtime = int(match.group(1))
                return runtime
        except Exception:
            pass
        
        print_to_csv(f"⚠️ No runtime found. Skipping {film_title}.")
        return None

    def update_max_movies_2500_statistics(self, film_title: str, release_year: str, tmdb_id: str):
        """Update statistics for MAX_MOVIES_2500."""
        # Get the movie info from whitelist
        movie_info, _ = self.get_whitelist_data(film_title, release_year)
        if not movie_info:
            return

        # Update directors
        for director in movie_info.get('Directors', []):
            max_movies_2500_stats['director_counts'][director] += 1

        # Update actors
        for actor in movie_info.get('Actors', []):
            max_movies_2500_stats['actor_counts'][actor] += 1

        # Update decade
        decade = movie_info.get('Decade')
        if decade:
            max_movies_2500_stats['decade_counts'][decade] += 1

        # Update genres
        for genre in movie_info.get('Genres', []):
            max_movies_2500_stats['genre_counts'][genre] += 1

        # Update studios
        for studio in movie_info.get('Studios', []):
            max_movies_2500_stats['studio_counts'][studio] += 1

        # Update languages
        for language in movie_info.get('Languages', []):
            max_movies_2500_stats['language_counts'][language] += 1

        # Update countries
        for country in movie_info.get('Countries', []):
            max_movies_2500_stats['country_counts'][country] += 1

    def is_blacklisted(self, film_title: str, release_year: str = None, film_url: str = None, driver = None) -> bool:
        """Check if a movie is in the blacklist using a lookup dictionary."""
        # If we have a URL, check for URL match first
        if film_url:
            for _, row in self.blacklist.iterrows():
                if row['Link'] == film_url:
                    return True
        
        # If no URL match or no URL provided, try title matching
        normalized_title = normalize_text(film_title).lower()
        
        # Find all matching titles in blacklist
        matching_entries = self.blacklist[
            self.blacklist['Title'].apply(normalize_text).str.lower() == normalized_title
        ]
        
        if matching_entries.empty:
            return False
            
        # If we have a URL but no direct match, check year match
        if film_url:
            for _, row in matching_entries.iterrows():
                if row['Link'] == '':  # If link is empty, check year match
                    # Get release year from movie page if not provided
                    if not release_year and driver:  # Make sure we have a driver
                        print_to_csv("Getting release year from movie page...")
                        driver.get(film_url)  # Use the passed driver parameter
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                        )
                        time.sleep(random.uniform(1.0, 1.5))
                        
                        meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
                        if meta_tag:
                            release_year_content = meta_tag.get_attribute('content')
                            if '(' in release_year_content and ')' in release_year_content:
                                release_year = release_year_content.split('(')[-1].strip(')')
                                print_to_csv(f"Found release year: {release_year}")
                
                    # Check if years match
                    if release_year and str(row['Year']).strip() == str(release_year).strip():
                        # Update the blacklist with the link
                        self.blacklist.loc[row.name, 'Link'] = film_url
                        self.blacklist.to_excel(BLACKLIST_PATH, index=False)
                        print_to_csv(f"🔗 Added link to blacklist for {film_title}")
                        return True
        
        # If no URL or no match found, check year if available
        if release_year:
            for _, row in matching_entries.iterrows():
                if str(row['Year']).strip() == str(release_year).strip():
                    return True
        
        return False

    def save_refreshed_data(self, film_title: str, release_year: str, tmdb_id: str) -> None:
        """Save information about movies that had their data reconstructed."""
        try:
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(BASE_DIR + '/Refreshed_Data.csv')
            
            with open(BASE_DIR + '/Refreshed_Data.csv', mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(['Title', 'Year', 'tmdbID'])
                writer.writerow([film_title, release_year, tmdb_id])
        except Exception as e:
            print_to_csv(f"Error saving refreshed data: {str(e)}")

    def is_incomplete_stats_whitelisted(self, film_title: str, release_year: str) -> bool:
        """Check if a movie is in the incomplete stats whitelist."""
        key = f"{film_title.lower()}_{release_year}"
        return key in self.incomplete_stats_lookup

    def add_to_zero_reviews(self, film_title: str, release_year: str, film_url: str):
        """Add a movie to the zero reviews list."""
        try:
            # Check if movie is already in zero_reviews by URL
            if any(self.zero_reviews['Link'] == film_url):
                return
             
            # Create a new row as a DataFrame
            new_row = pd.DataFrame([[film_title, release_year, '', film_url]], 
                                 columns=['Title', 'Year', 'Blank', 'Link'])
            
            # Append to existing zero reviews
            self.zero_reviews = pd.concat([self.zero_reviews, new_row], ignore_index=True)
                        
            # Save back to Excel immediately
            try:
                self.zero_reviews.to_excel(ZERO_REVIEWS_PATH, index=False)
            except Exception as excel_error:
                print_to_csv(f"ERROR saving to Excel: {str(excel_error)}")
                print_to_csv(f"ERROR type: {type(excel_error)}")
                raise
            
        except Exception as e:
            print_to_csv(f"ERROR adding to zero reviews: {str(e)}")
            print_to_csv(f"ERROR type: {type(e)}")
            print_to_csv(f"ERROR details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            raise  # Re-raise the exception to see the full traceback

    def is_zero_reviews(self, film_title: str, release_year: str, film_url: str) -> bool:
        """Check if a movie is in the zero reviews list."""
        try:
            # Check if the URL exists in the zero_reviews DataFrame
            if film_url:
                result = any(self.zero_reviews['Link'] == film_url)
                if result:
                    # 1 in 15 chance to remove the entry after finding it
                    if random.random() < (1/15):
                        # Find the index of the row to remove
                        idx_to_remove = self.zero_reviews[self.zero_reviews['Link'] == film_url].index[0]
                        # Remove the row
                        self.zero_reviews = self.zero_reviews.drop(idx_to_remove)
                        # Save the updated DataFrame
                        self.zero_reviews.to_excel(ZERO_REVIEWS_PATH, index=False)
                        print_to_csv(f"🗑️ Removed {film_title} from zero reviews list")
                return result
            
            return False
        except Exception as e:
            print_to_csv(f"ERROR checking zero reviews: {str(e)}")
            return False

def setup_webdriver() -> webdriver.Firefox:
    options = Options()
    options.headless = True
    options.set_preference("permissions.default.image", 2)  # Disable images
    options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", "false")
    options.set_preference("browser.display.use_document_fonts", 0)
    options.set_preference("browser.display.document_color_use", 2)
    
    # Add these preferences to prevent random downloads
    options.set_preference("browser.download.folderList", 2)  # Use custom download location
    options.set_preference("browser.download.manager.showWhenStarting", False)  # Don't show download manager
    options.set_preference("browser.download.dir", os.path.join(BASE_DIR, "downloads"))  # Set download directory
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/html,text/plain")  # Don't ask to save HTML files
    options.set_preference("browser.helperApps.alwaysAsk.force", False)  # Don't force asking
    options.set_preference("browser.download.manager.alertOnEXEOpen", False)  # Don't alert on exe downloads
    options.set_preference("browser.download.manager.focusWhenStarting", False)  # Don't focus download manager
    options.set_preference("browser.download.manager.useWindow", False)  # Don't use window for downloads
    options.set_preference("browser.download.manager.showAlertOnComplete", False)  # Don't show alert when complete
    options.set_preference("browser.download.manager.closeWhenDone", True)  # Close download manager when done
    
    # Add these optimizations:
    options.set_preference("javascript.enabled", True)  # Keep JS enabled but optimize
    options.set_preference("network.http.connection-timeout", 30)  # Reduce timeout
    options.set_preference("network.http.max-connections-per-server", 10)  # Limit connections
    options.set_preference("browser.cache.disk.enable", True)  # Enable disk cache
    options.set_preference("browser.cache.memory.enable", True)  # Enable memory cache
    
    service = Service()
    return webdriver.Firefox(service=service, options=options)

def format_time(seconds):
    """Format seconds into hours, minutes, seconds string"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def extract_mpaa_rating(driver) -> Optional[str]:
    """Extract the MPAA rating from the movie's page if the country is USA."""
    try:
        # Wait for the page to be fully loaded
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.release-country-list')))
        
        # Use JavaScript to extract all country data
        js_script = """
        const countries = Array.from(document.querySelectorAll('.release-country'));
        return countries.map(country => {
            const name = country.querySelector('.name')?.textContent?.trim() || '';
            const rating = country.querySelector('.release-certification-badge .label')?.textContent?.trim() || '';
            return { name, rating };
        });
        """
        
        country_data = driver.execute_script(js_script)
        
        # Process the country data
        for data in country_data:
            name = data['name']
            rating = data['rating']
            
            if name == "USA" and rating:
                # Handle special cases
                if rating.upper() in ['NR', 'NOT RATED', 'UNRATED']:
                    return 'NR'
                
                # Map common rating formats to MPAA ratings
                rating_map = {
                    'R': 'R',
                    'PG-13': 'PG-13',
                    'PG': 'PG',
                    'G': 'G',
                    'NC-17': 'NC-17',
                    'X': 'NC-17',  # Historical rating
                    'M': 'PG',     # Historical rating
                    'GP': 'PG',    # Historical rating
                }
                
                if rating in rating_map:
                    return rating_map[rating]
        
        return None
        
    except Exception as e:
        print_to_csv(f"Error extracting MPAA rating: {str(e)}")
        return None

def add_to_max_movies_2500(film_title: str, release_year: str, tmdb_id: str) -> bool:
    """
    Centralized function to add a movie to max_movies_2500_stats if it's not already present.
    Returns True if the movie was added, False if it was already present or if we've reached the limit.
    """
    # Check if movie already exists
    if any(movie['Title'] == film_title and movie['Year'] == release_year 
           for movie in max_movies_2500_stats['film_data']):
        return False
        
    # Check if we've reached the limit
    if len(max_movies_2500_stats['film_data']) >= MAX_MOVIES_2500:
        return False
        
    # Add the movie
    max_movies_2500_stats['film_data'].append({
        'Title': film_title,
        'Year': release_year,
        'tmdbID': tmdb_id
    })
    return True

class LetterboxdScraper:
    def __init__(self, genre=None, sort_type=None):
        self.driver = setup_webdriver()
        self.processor = MovieProcessor()
        self.genre = genre
        self.sort_type = sort_type
        self.base_url = f'https://letterboxd.com/films/genre/{genre}/by/{sort_type}/' if genre and sort_type else None
        self.total_titles = 0
        self.processed_titles = 0
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()
        self.top_movies_count = 0  # Track the number of movies added to the top 2500 list
        print_to_csv("Initialized Letterboxd Scraper.")

    def process_movie_data(self, info, film_title=None, film_url=None):
        """Process movie data from the whitelist."""
        try:            
            # If we have a complete info dict, process it directly without loading the page
            if isinstance(info, dict):
                film_title = info.get('Title')
                release_year = info.get('Year')
                
                # Get whitelist data to check if we have a link
                whitelist_info, row_idx = self.processor.get_whitelist_data(film_title, release_year, film_url)
                
                # Check if we need to update a blank link
                if film_url:
                    # Get the current link from whitelist
                    key = f"{film_title.lower()}_{release_year}"
                    if key in self.processor.whitelist_lookup:
                        _, _, existing_url = self.processor.whitelist_lookup[key]
                        # Only proceed with link update if current link is blank
                        if not existing_url or existing_url == '':
                            try:
                                # Load the movie page to verify it's the correct movie
                                self.driver.get(film_url)
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                                )
                                time.sleep(random.uniform(1.0, 1.5))
                                
                                # Extract release year from the page
                                meta_tag = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
                                content = meta_tag.get_attribute('content')
                                if content and '(' in content and ')' in content:
                                    page_year = content.split('(')[-1].split(')')[0].strip()
                                    # If years match, update the whitelist with the link
                                    if page_year == release_year:
                                        self.processor.update_whitelist(film_title, release_year, info, film_url)
                                    else:
                                        print_to_csv(f"⚠️ Year mismatch for {film_title}: expected {release_year}, found {page_year}")
                                        return False
                            except Exception as e:
                                print_to_csv(f"Error verifying movie link: {str(e)}")
                                return False
                
                # Check if movie is in incomplete stats whitelist
                if self.processor.is_incomplete_stats_whitelisted(film_title, release_year):
                    # Process the whitelist information directly, regardless of completeness
                    if info and info != {}:  # Only process if there's at least some data
                        self.processor.process_whitelist_info(info)
                        self.valid_movies_count += 1
                        print_to_csv(f"✅ Processed whitelist data for {info.get('Title')} ({self.valid_movies_count}/{MAX_MOVIES})")
                        return True
                    return False

                # If not in incomplete stats whitelist, check for completeness
                if all([
                    info.get('Title'),
                    info.get('Year'),
                    info.get('Runtime'),
                    info.get('RatingCount'),
                    info.get('Languages'),
                    info.get('Countries'),
                    info.get('Directors'),
                    info.get('Genres'),
                    info.get('Studios'),
                    info.get('Actors')
                ]):
                    # Process the whitelist information directly
                    self.processor.process_whitelist_info(info)
                    self.valid_movies_count += 1
                    print_to_csv(f"✅ Processed whitelist data for {info.get('Title')} ({self.valid_movies_count}/{MAX_MOVIES})")
                    
                    # 2% chance to clear the whitelist data
                    if random.random() < 0.02:
                        self.processor.update_whitelist(film_title, release_year, {}, film_url)
                        print_to_csv(f"🤓 Random data audit scheduled for {film_title} ({release_year})")
                    
                    return True

            # If info is incomplete, clear the Information cell and let normal scraping handle it
            if isinstance(info, dict):
                film_title = info.get('Title')
                release_year = info.get('Year')
                tmdb_id = info.get('tmdbID')
                
                # Save the movie info to Refreshed_Data.csv
                if all([film_title, release_year, tmdb_id]):
                    self.processor.save_refreshed_data(film_title, release_year, tmdb_id)
                
                # Clear the Information cell in whitelist
                if self.processor.is_whitelisted(film_title, release_year):
                    self.processor.update_whitelist(film_title, release_year, {}, film_url)
                    print_to_csv(f"🔄 Cleared incomplete data for {film_title}, will rescrape")
                
                # Construct URL if not provided
                if not film_url:
                    film_url = f"https://letterboxd.com/film/{film_title.lower().replace(' ', '-')}/"
                
                # Let the normal scraping flow handle it
                max_retries = 20
                for retry in range(max_retries):
                    try:
                        self.driver.get(film_url)
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                        )
                        time.sleep(random.uniform(1.0, 1.5))

                        # Extract release year
                        meta_tag = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                        )
                        content = meta_tag.get_attribute('content')
                        if content and '(' in content and ')' in content:
                            release_year = content.split('(')[-1].split(')')[0].strip()
                        else:
                            print_to_csv(f"❌ Could not extract release year for {film_title}")
                            if retry < max_retries - 1:
                                print_to_csv(f"Retrying... (Attempt {retry + 1}/{max_retries})")
                                time.sleep(2)
                                continue
                            return False

                        # Get fresh data and update whitelist
                        movie_data = self.update_statistics_for_movie(film_title, release_year, info.get('tmdbID'), self.driver, film_url)
                        if movie_data:
                            # Update whitelist with fresh data
                            if self.processor.update_whitelist(film_title, release_year, movie_data, film_url):
                                # Process through output channels
                                self.processor.process_whitelist_info(movie_data)
                                
                                # Process MAX_MOVIES_2500 using centralized function
                                if add_to_max_movies_2500(film_title, release_year, movie_data.get('tmdbID')):
                                    self.processor.update_max_movies_2500_statistics(film_title, release_year, movie_data.get('tmdbID'))
                                
                                return True
                            else:
                                print_to_csv(f"❌ Failed to update whitelist for {film_title}")
                                if retry < max_retries - 1:
                                    print_to_csv(f"Retrying... (Attempt {retry + 1}/{max_retries})")
                                    time.sleep(2)
                                    continue
                                return False
                        else:
                            if retry < max_retries - 1:
                                print_to_csv(f"Retrying... (Attempt {retry + 1}/{max_retries})")
                                time.sleep(2)
                                continue
                            # On final retry, check if we should add to incomplete stats whitelist
                            try:
                                current_year = datetime.now().year
                                movie_year = int(release_year)
                                rating_count = 0
                                page_source = self.driver.page_source
                                match = re.search(r'ratingCount":(\d+)', page_source)
                                if match:
                                    rating_count = int(match.group(1))
                                
                                # Check if movie meets criteria for incomplete stats whitelist
                                if (current_year - movie_year > 5 and 
                                    rating_count > 50000 and 
                                    not self.processor.is_incomplete_stats_whitelisted(film_title, release_year)):
                                    # Add to incomplete stats whitelist
                                    new_row = pd.DataFrame([{
                                        'Title': film_title,
                                        'Year': release_year
                                    }])
                                    self.processor.incomplete_stats_whitelist = pd.concat(
                                        [self.processor.incomplete_stats_whitelist, new_row], 
                                        ignore_index=True
                                    )
                                    self.processor.incomplete_stats_whitelist.to_excel(
                                        INCOMPLETE_STATS_WHITELIST_PATH, 
                                        index=False
                                    )
                                    # Update lookup dictionary
                                    key = f"{film_title.lower()}_{release_year}"
                                    self.processor.incomplete_stats_lookup[key] = True
                                    print_to_csv(f"📝 Added {film_title} ({release_year}) to incomplete stats whitelist")
                            except Exception as e:
                                print_to_csv(f"Error checking incomplete stats whitelist criteria: {str(e)}")
                            return False

                    except Exception as e:
                        print_to_csv(f"❌ Error rescraping {film_title}: {str(e)}")
                        if retry < max_retries - 1:
                            print_to_csv(f"Retrying... (Attempt {retry + 1}/{max_retries})")
                            time.sleep(2)
                            continue
                        return False

                return False

            # If info is not a dictionary or we have film_url, we need to scrape the data
            if not isinstance(info, dict):
                if not film_url:
                    print_to_csv(f"❌ Need to scrape data for {film_title} but no URL provided")
                    return
                
                try:
                    # Add retry mechanism for individual movie pages
                    max_retries = 20
                    for retry in range(max_retries):
                        try:
                            self.driver.get(film_url)
                            # Wait for page to load
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                            )
                            time.sleep(random.uniform(1.0, 1.5))
                            break
                        except Exception as e:
                            if retry == max_retries - 1:
                                print_to_csv(f"❌ Error loading movie page for {film_title}: {str(e)}")
                                return
                            print_to_csv(f"Retry {retry + 1}/{max_retries} loading movie page for {film_title}")
                            time.sleep(2)
                except Exception as e:
                    print_to_csv(f"❌ Error loading movie page for {film_title}: {str(e)}")
                    return

                # Extract release year
                try:
                    meta_tag = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                    )
                    content = meta_tag.get_attribute('content')
                    if content and '(' in content and ')' in content:
                        release_year = content.split('(')[-1].split(')')[0].strip()
                    else:
                        print_to_csv(f"❌ Could not extract release year for {film_title}")
                        return False
                except Exception as e:
                    print_to_csv(f"❌ Error extracting release year for {film_title}: {str(e)}")
                    return

                # Check if movie is in whitelist first
                whitelist_info, _ = self.processor.get_whitelist_data(film_title, release_year, film_url)
                    
                # Extract rating count
                rating_count = 0
                try:
                    page_source = self.driver.page_source
                    match = re.search(r'ratingCount":(\d+)', page_source)
                    if match:
                        rating_count = int(match.group(1))
                except Exception as e:
                    print_to_csv(f"Error extracting rating count: {str(e)}")

                # Check 1: Rating count minimum
                if rating_count < MIN_RATING_COUNT:
                    print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                    self.processor.rejected_data.append([film_title, release_year, None, 'Insufficient ratings (< 1000)'])
                    return

                # If movie is in whitelist but has insufficient ratings, reject it
                if whitelist_info and rating_count < MIN_RATING_COUNT:
                    print_to_csv(f"❌ {film_title} ({release_year}) was not added due to insufficient ratings: {rating_count} ratings.")
                    self.processor.rejected_data.append([film_title, release_year, None, 'Insufficient ratings (< 1000)'])
                    return

                # If movie is in whitelist and has sufficient ratings, process it
                if whitelist_info:
                    self.process_movie_data(whitelist_info, film_title, film_url)
                    return

                # Extract TMDb ID
                try:
                    tmdb_id = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, 'body'))
                    ).get_attribute('data-tmdb-id')
                except Exception as e:
                    tmdb_id = None
                    print_to_csv(f"TMDb ID not found: {e}")

                # Update statistics and collect data
                self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver, film_url)
                
                # Extract runtime
                runtime = None
                try:
                    runtime_element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                    )
                    runtime_text = runtime_element.text
                    match = re.search(r'(\d+)\s*min(?:s)?', runtime_text)
                    if match:
                        runtime = int(match.group(1))
                except Exception as e:
                    print_to_csv(f"Error extracting runtime: {str(e)}")

                # Extract languages
                movie_languages = set()
                try:                    
                    headings = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details h3')
                    
                    for heading in headings:
                        span = heading.find_element(By.TAG_NAME, 'span')
                        heading_text = span.get_attribute('textContent').strip() if span else heading.get_attribute('textContent').strip()
                        
                        if any(lang in heading_text for lang in ["Language", "Primary Language", "Languages", "Primary Languages"]):
                            try:
                                sluglist = heading.find_element(By.XPATH, "following-sibling::div[contains(@class, 'text-sluglist')]")
                                
                                if sluglist:
                                    p_tag = sluglist.find_element(By.TAG_NAME, 'p')
                                    language_elements = p_tag.find_elements(By.CSS_SELECTOR, 'a.text-slug[href*="/films/language/"]')
                                    
                                    for language in language_elements:
                                        language_name = language.get_attribute('textContent').strip()
                                        if language_name:
                                            movie_languages.add(language_name)
                            except Exception:
                                pass
                except Exception:
                    pass

                # Extract MPAA rating
                mpaa_rating = None
                try:
                    mpaa_rating = extract_mpaa_rating(self.driver)
                except Exception as e:
                    print_to_csv(f"Error extracting MPAA rating: {str(e)}")

                # Extract directors
                movie_directors = []
                try:
                    director_elements = self.driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
                    for director in director_elements:
                        director_name = director.text.strip()
                        if director_name:
                            movie_directors.append(director_name)
                except Exception:
                    pass

                # Extract actors
                movie_actors = []
                try:
                    actor_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
                    for actor in actor_elements:
                        actor_name = actor.text.strip()
                        if actor_name:
                            movie_actors.append(actor_name)
                except Exception:
                    pass

                # Extract genres
                movie_genres = []
                try:
                    genre_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug[href*="/films/genre/"]')
                    for genre in genre_elements:
                        genre_name = genre.get_attribute('textContent').strip()
                        if genre_name and not any(char in genre_name for char in ['…', 'Show All']):
                            movie_genres.append(genre_name)
                except Exception:
                    pass

                # Extract studios
                movie_studios = []
                try:
                    studio_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/studio/"]')
                    for studio in studio_elements:
                        studio_name = studio.get_attribute('textContent').strip()
                        if studio_name:
                            movie_studios.append(studio_name)
                except Exception:
                    pass

                # Extract countries
                movie_countries = []
                try:
                    country_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]')
                    for country in country_elements:
                        country_name = country.get_attribute('textContent').strip()
                        if country_name:
                            movie_countries.append(country_name)
                            self.processor.country_counts[country_name] = self.processor.country_counts.get(country_name, 0) + 1
                except Exception as e:
                    print_to_csv(f"Error extracting countries: {str(e)}")
                    movie_countries = []

                # Create movie data dictionary
                movie_data = {
                    'Title': film_title,
                    'Year': release_year,
                    'tmdbID': tmdb_id,
                    'MPAA': mpaa_rating,
                    'Runtime': runtime,
                    'RatingCount': rating_count,
                    'Languages': list(movie_languages),
                    'Countries': movie_countries,
                    'Decade': (int(release_year) // 10) * 10,
                    'Directors': movie_directors,
                    'Genres': movie_genres,
                    'Studios': movie_studios,
                    'Actors': movie_actors
                }

            # If we have valid info dict, process it normally
            if isinstance(info, dict):
                film_title = info.get('Title')
                release_year = info.get('Year')
                tmdb_id = info.get('tmdbID')
                
                if not all([film_title, release_year]):
                    return

                # Process the whitelist information
                self.processor.process_whitelist_info(info)
                                
                # Process MAX_MOVIES_2500 using centralized function
                if add_to_max_movies_2500(film_title, release_year, tmdb_id):
                    self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)
                
                # Add to unfiltered_approved if not already in whitelist
                if not self.processor.is_whitelisted(film_title, release_year):
                    if not any(film_title.lower() == movie[0].lower() and release_year == movie[1] for movie in self.processor.unfiltered_approved):
                        self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id, film_url])
                
                return True
            
        except Exception as e:
            print_to_csv(f"Error processing movie data: {str(e)}")
            print_to_csv(f"Error type: {type(e)}")
            print_to_csv(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            # Don't raise the exception, just continue

    def scrape_movies(self):
        seen_titles = set()  # <-- Add this at the start of the method

        while self.valid_movies_count < MAX_MOVIES:
            # Construct the URL for the current page
            url = f'{self.base_url}page/{self.page_number}/'
            print_to_csv(f"\nLoading page {self.page_number}: {url}")
            
            # Send a GET request to the URL with retry mechanism
            page_retries = 20
            for retry in range(page_retries):
                try:
                    self.driver.get(url)
                    # Wait for the page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.react-component.poster'))
                    )
                    break
                except Exception as e:
                    if retry == page_retries - 1:
                        print_to_csv(f"❌ Failed to load page after {page_retries} attempts: {str(e)}")
                        self.save_results()  # Save progress before exiting
                        raise Exception(f"Failed to load page after {page_retries} attempts: {str(e)}")
                    print_to_csv(f"Retry {retry + 1}/{page_retries} loading page {self.page_number}: {str(e)}")
                    time.sleep(2)
            
            time.sleep(random.uniform(1.0, 1.5))
                    
            # Find all film containers with retry mechanism
            film_containers = []
            container_retries = 25  # Maximum number of retries
            for retry in range(container_retries):
                try:
                    film_containers = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.react-component.poster'))
                    )
                    if len(film_containers) == 72:  # Check for exactly 72 containers
                        break
                    else:
                        print_to_csv(f"Found only {len(film_containers)} containers, retrying... (Attempt {retry + 1}/{container_retries})")
                        time.sleep(5)  # Wait longer between retries
                        self.driver.refresh()  # Refresh the page
                        time.sleep(2)  # Wait for refresh
                except Exception as e:
                    if retry == container_retries - 1:
                        print_to_csv(f"❌ Failed to find all 72 film containers after {container_retries} attempts: {str(e)}")
                        self.save_results()  # Save progress before exiting
                        raise Exception(f"Failed to find all 72 film containers after {container_retries} attempts: {str(e)}")
                    print_to_csv(f"Retry {retry + 1}/{container_retries} finding film containers: {str(e)}")
                    time.sleep(5)
                    self.driver.refresh()
                    time.sleep(2)
            
            if len(film_containers) != 72:
                print_to_csv(f"❌ Failed to find all 72 film containers after {container_retries} attempts")
                self.save_results()  # Save progress before exiting
                raise Exception(f"Failed to find all 72 film containers after {container_retries} attempts")

            print_to_csv(f"\n{f' Page {self.page_number} ':=^100}")

            # First collect all film data from the page
            film_data_list = []
            for container in film_containers:
                try:
                    # Get the anchor element first
                    anchor = container.find_element(By.CSS_SELECTOR, 'a')
                    film_url = anchor.get_attribute('href')
                    
                    # Get the film name from the data attribute
                    film_title = container.get_attribute('data-film-name')
                    
                    if film_title and film_url:
                        # Extract year from title if possible
                        release_year = None
                        if '(' in film_title and ')' in film_title:
                            release_year = film_title.split('(')[-1].split(')')[0].strip()
                        
                        # Just check if title exists in blacklist, don't try to get release year yet
                        is_blacklisted = self.processor.is_blacklisted(film_title, release_year, film_url, None)  # Pass None as driver
                        film_data_list.append({
                            'title': film_title,
                            'url': film_url,
                            'is_blacklisted': is_blacklisted,
                            'release_year': release_year
                        })
                    else:
                        print_to_csv(f"Missing data for movie - Title: {film_title}, URL: {film_url}")
                except Exception as e:
                    print_to_csv(f"Error collecting film data: {str(e)}")
                    continue

            print_to_csv(f"Collected {len(film_data_list)} movies from page {self.page_number}")
            
            if not film_data_list:
                print_to_csv("No valid film data collected. Moving to next page...")
                self.page_number += 1
                continue

            # Now process each film one by one
            for film_data in film_data_list:
                if self.valid_movies_count >= MAX_MOVIES:
                    print_to_csv(f"\nReached the target of {MAX_MOVIES} successful movies. Stopping scraping.")
                    return

                film_title = film_data['title']
                film_url = film_data['url']
                release_year = film_data['release_year']

                # If we've seen this title before, require title+year match
                if film_title.lower() in seen_titles:
                    whitelist_info, _ = self.processor.get_whitelist_data(film_title, release_year, film_url)
                else:
                    whitelist_info, _ = self.processor.get_whitelist_data(film_title, film_url=film_url)

                # After processing, add the title to seen_titles
                seen_titles.add(film_title.lower())

                # Increment total_titles for each movie we process, including blacklisted ones
                self.total_titles += 1
                
                # Check if movie is in zero reviews list
                if self.processor.is_zero_reviews(film_title, release_year, film_url):
                    print_to_csv(f"📊 {film_title} is in zero reviews list. Skipping.")
                    continue
                
                # Handle blacklisted movies first
                if film_data['is_blacklisted']:
                    print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
                    self.processor.rejected_data.append([film_title, release_year, None, 'Blacklisted'])
                    continue
                
                # First check for exact matches in whitelist
                if whitelist_info:
                    self.process_movie_data(whitelist_info, film_title, film_url)
                    continue
                                
                # Get initial movie data without full scrape
                movie_retries = 20  # Maximum number of retries for individual movie pages
                for retry in range(movie_retries):
                    try:
                        self.driver.get(film_url)
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                        )
                        time.sleep(random.uniform(1.0, 1.5))
                        
                        # Extract basic info needed for checks
                        meta_tag = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
                        release_year = None
                        if meta_tag:
                            release_year_content = meta_tag.get_attribute('content')
                            release_year = release_year_content.split('(')[-1].strip(')')
                        
                        # Extract rating count
                        rating_count = 0
                        try:
                            page_source = self.driver.page_source
                            match = re.search(r'ratingCount":(\d+)', page_source)
                            if match:
                                rating_count = int(match.group(1))
                        except Exception as e:
                            print_to_csv(f"Error extracting rating count: {str(e)}")

                        # Check if movie has zero reviews
                        if rating_count == 0:
                            print_to_csv(f"📊 {film_title} has no reviews. Adding to zero reviews list.")
                            self.processor.add_to_zero_reviews(film_title, release_year, film_url)
                            self.processor.rejected_data.append([film_title, release_year, None, 'Zero reviews'])
                            break  # Break out of retry loop and continue to next movie
                        
                        # Check 1: Rating count minimum
                        if rating_count < MIN_RATING_COUNT:
                            print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                            self.processor.rejected_data.append([film_title, release_year, None, 'Insufficient ratings (< 1000)'])
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Check 2: Blacklist
                        if self.processor.is_blacklisted(film_title, release_year, film_url, self.driver):
                            print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
                            self.processor.rejected_data.append([film_title, release_year, None, 'Blacklisted'])
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Check 3: Runtime
                        try:
                            runtime_element = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                            )
                            runtime_text = runtime_element.text
                            match = re.search(r'(\d+)\s*min(?:s)?', runtime_text)
                            if match:
                                runtime = int(match.group(1))
                            else:
                                runtime = None
                        except Exception as e:
                            runtime = None
                            print_to_csv(f"Error extracting runtime for {film_title}: {str(e)}")

                        if runtime is None:
                            print_to_csv(f"⚠️ {film_title} skipped due to missing runtime")
                            if retry < movie_retries - 1:
                                print_to_csv(f"Retrying... (Attempt {retry + 1}/{movie_retries})")
                                time.sleep(2)
                                continue
                            break
                            
                        if runtime < MIN_RUNTIME:
                            print_to_csv(f"❌ {film_title} was not added due to a short runtime of {runtime} minutes.")
                            self.processor.rejected_data.append([film_title, release_year, None, f'Short runtime of {runtime} minutes'])
                            self.processor.add_to_blacklist(film_title, release_year, f'Short runtime of {runtime} minutes')
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Check 4: TMDB ID
                        try:
                            body_tag = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, 'body'))
                            )
                            tmdb_id = body_tag.get_attribute('data-tmdb-id')
                        except Exception as e:
                            tmdb_id = None
                            print_to_csv(f"Error extracting TMDB ID for {film_title}: {str(e)}")
                        
                        if not tmdb_id:
                            print_to_csv(f"❌ {film_title} was not added due to missing TMDB ID.")
                            self.processor.rejected_data.append([film_title, release_year, None, 'Missing TMDB ID'])
                            self.processor.unfiltered_denied.append([film_title, release_year, None, film_url])
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Check 5: Keywords and Genres
                        keywords, genres = self.processor.fetch_tmdb_details(tmdb_id)
                        
                        # Check keywords
                        matching_keywords = [k for k in FILTER_KEYWORDS if k in keywords]
                        if matching_keywords:
                            rejection_reason = f"due to being a {', '.join(matching_keywords)}."
                            print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                            self.processor.rejected_data.append([film_title, release_year, None, rejection_reason])
                            self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Check genres
                        matching_genres = [g for g in FILTER_GENRES if g in genres]
                        if matching_genres:
                            rejection_reason = f"due to being a {', '.join(matching_genres)}."
                            print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                            self.processor.rejected_data.append([film_title, release_year, None, rejection_reason])
                            self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                            break  # Break out of retry loop since this is a permanent rejection
                        
                        # Now do the full scrape and process the movie
                        movie_data = {
                            'Title': film_title,
                            'Year': release_year,
                            'tmdbID': tmdb_id,
                            'Runtime': runtime,
                            'RatingCount': rating_count
                        }

                        # Extract directors
                        try:
                            director_elements = self.driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
                            movie_data['Directors'] = [director.text.strip() for director in director_elements if director.text.strip()]
                        except Exception as e:
                            print_to_csv(f"Error extracting directors: {str(e)}")
                            movie_data['Directors'] = []

                        # Extract actors
                        try:
                            actor_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
                            movie_data['Actors'] = [actor.text.strip() for actor in actor_elements if actor.text.strip()]
                        except Exception as e:
                            print_to_csv(f"Error extracting actors: {str(e)}")
                            movie_data['Actors'] = []

                        # Extract genres
                        try:
                            genre_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug[href*="/films/genre/"]')
                            movie_data['Genres'] = [genre.get_attribute('textContent').strip() for genre in genre_elements 
                                                  if genre.get_attribute('textContent').strip() and 
                                                  not any(char in genre.get_attribute('textContent').strip() for char in ['…', 'Show All'])]
                        except Exception as e:
                            print_to_csv(f"Error extracting genres: {str(e)}")
                            movie_data['Genres'] = []

                        # Extract studios
                        try:
                            studio_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/studio/"]')
                            movie_data['Studios'] = [studio.get_attribute('textContent').strip() for studio in studio_elements if studio.get_attribute('textContent').strip()]
                        except Exception as e:
                            print_to_csv(f"Error extracting studios: {str(e)}")
                            movie_data['Studios'] = []

                        # Extract languages
                        movie_languages = set()
                        try:
                            headings = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details h3')
                            for heading in headings:
                                span = heading.find_element(By.TAG_NAME, 'span')
                                heading_text = span.get_attribute('textContent').strip() if span else heading.get_attribute('textContent').strip()
                                
                                if any(lang in heading_text for lang in ["Language", "Primary Language", "Languages", "Primary Languages"]):
                                    try:
                                        sluglist = heading.find_element(By.XPATH, "following-sibling::div[contains(@class, 'text-sluglist')]")
                                        if sluglist:
                                            p_tag = sluglist.find_element(By.TAG_NAME, 'p')
                                            language_elements = p_tag.find_elements(By.CSS_SELECTOR, 'a.text-slug[href*="/films/language/"]')
                                            for language in language_elements:
                                                language_name = language.get_attribute('textContent').strip()
                                                if language_name:
                                                    movie_languages.add(language_name)
                                                    self.processor.language_counts[language_name] = self.processor.language_counts.get(language_name, 0) + 1
                                    except Exception:
                                        pass
                        except Exception as e:
                            print_to_csv(f"Error extracting languages: {str(e)}")
                        movie_data['Languages'] = list(movie_languages)

                        # Extract countries
                        try:
                            country_elements = self.driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]')
                            movie_data['Countries'] = [country.get_attribute('textContent').strip() for country in country_elements if country.get_attribute('textContent').strip()]
                        except Exception as e:
                            print_to_csv(f"Error extracting countries: {str(e)}")
                            movie_data['Countries'] = []

                        # Add decade
                        movie_data['Decade'] = (int(release_year) // 10) * 10

                        # Add to unfiltered_approved
                        if not any(film_title.lower() == movie[0].lower() and release_year == movie[1] for movie in self.processor.unfiltered_approved):
                            # Only add to unfiltered_approved if the movie is not in the whitelist
                            if not self.processor.is_whitelisted(film_title, release_year):
                                self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id, film_url])
                                self.valid_movies_count += 1  # Increment the count since it's an approved movie
                                print_to_csv(f"✅ Successfully approved {film_title} ({self.valid_movies_count}/{MAX_MOVIES})")
                                                                                             
                                # Process MAX_MOVIES_2500
                                if add_to_max_movies_2500(film_title, release_year, tmdb_id):
                                    self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)

                        # Update statistics
                        self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver, film_url)
                        break  # Successfully processed the movie, break out of retry loop

                    except Exception as e:
                        print_to_csv(f"❌ Error processing {film_title}: {str(e)}")
                        if retry < movie_retries - 1:
                            print_to_csv(f"Retrying... (Attempt {retry + 1}/{movie_retries})")
                            time.sleep(2)
                            continue
                        raise Exception(f"Failed to process {film_title} after {movie_retries} attempts")

            self.page_number += 1
            time.sleep(random.uniform(1.0, 1.5))

        # If we reach here, we've successfully completed scraping
        return

    def process_approved_movie(self, film_title: str, release_year: str, tmdb_id: str, film_url: str, approval_type: str):
        if self.valid_movies_count >= MAX_MOVIES:
            return

        # Add to max_movies_2500_stats
        if add_to_max_movies_2500(film_title, release_year, tmdb_id):
            self.update_max_movies_2500_statistics(film_title, release_year, tmdb_id, self.driver)

        # Update statistics
        self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver, film_url)

        # Increment valid movies count
        self.valid_movies_count += 1

    def update_max_movies_2500_statistics(self, film_title: str, release_year: str, tmdb_id: str, driver):
        """Update statistics for MAX_MOVIES_2500."""
        # Get movie details from TMDb
        genres, keywords = self.processor.fetch_tmdb_details(tmdb_id)
        
        # Update genre counts
        for genre in genres:
            max_movies_2500_stats['genre_counts'][genre] = max_movies_2500_stats['genre_counts'].get(genre, 0) + 1
        
        # Update keyword counts
        for keyword in keywords:
            max_movies_2500_stats['keyword_counts'][keyword] = max_movies_2500_stats['keyword_counts'].get(keyword, 0) + 1
        
        # Update decade counts
        decade = f"{release_year[:3]}0s"
        max_movies_2500_stats['decade_counts'][decade] = max_movies_2500_stats['decade_counts'].get(decade, 0) + 1
        
        # Update director counts
        for heading in driver.find_elements(By.CSS_SELECTOR, '#tab-details h3'):
            if "Director" in heading.text or "Directors" in heading.text:
                sluglist = heading.find_element(By.CSS_SELECTOR, '.text-sluglist')
                for director in sluglist.find_elements(By.CSS_SELECTOR, 'a.text-slug'):
                    director_name = director.text.strip()
                    if director_name:
                        max_movies_2500_stats['director_counts'][director_name] = max_movies_2500_stats['director_counts'].get(director_name, 0) + 1
        
        # Update actor counts
        for heading in driver.find_elements(By.CSS_SELECTOR, '#tab-details h3'):
            if "Cast" in heading.text:
                sluglist = heading.find_element(By.CSS_SELECTOR, '.text-sluglist')
                for actor in sluglist.find_elements(By.CSS_SELECTOR, 'a.text-slug'):
                    actor_name = actor.text.strip()
                    if actor_name:
                        max_movies_2500_stats['actor_counts'][actor_name] = max_movies_2500_stats['actor_counts'].get(actor_name, 0) + 1
        
        # Update studio counts
        for heading in driver.find_elements(By.CSS_SELECTOR, '#tab-details h3'):
            if "Studio" in heading.text or "Studios" in heading.text:
                sluglist = heading.find_element(By.CSS_SELECTOR, '.text-sluglist')
                for studio in sluglist.find_elements(By.CSS_SELECTOR, 'a.text-slug'):
                    studio_name = studio.text.strip()
                    if studio_name:
                        max_movies_2500_stats['studio_counts'][studio_name] = max_movies_2500_stats['studio_counts'].get(studio_name, 0) + 1
        
        # Update language counts
        for heading in driver.find_elements(By.CSS_SELECTOR, '#tab-details h3'):
            if "Language" in heading.text or "Languages" in heading.text:
                sluglist = heading.find_element(By.CSS_SELECTOR, '.text-sluglist')
                for language in sluglist.find_elements(By.CSS_SELECTOR, 'a.text-slug'):
                    language_name = language.text.strip()
                    if language_name:
                        max_movies_2500_stats['language_counts'][language_name] = max_movies_2500_stats['language_counts'].get(language_name, 0) + 1
        
        # Update country counts
        for heading in driver.find_elements(By.CSS_SELECTOR, '#tab-details h3'):
            if "Country" in heading.text or "Countries" in heading.text:
                sluglist = heading.find_element(By.CSS_SELECTOR, '.text-sluglist')
                for country in sluglist.find_elements(By.CSS_SELECTOR, 'a.text-slug'):
                    country_name = country.text.strip()
                    if country_name:
                        max_movies_2500_stats['country_counts'][country_name] = max_movies_2500_stats['country_counts'].get(country_name, 0) + 1

    def save_max_movies_2500_results(self):
        """Save results for MAX_MOVIES_2500."""
        # Save movie data to CSV
        df = pd.DataFrame(max_movies_2500_stats['film_data'])
        output_path = os.path.join(BASE_DIR, f'top_250_{self.genre}_{self.sort_type}.csv')
        df.to_csv(output_path, index=False, encoding='utf-8')

        # Save statistics
        stats_path = os.path.join(BASE_DIR, f'stats_top_250_{self.genre}_{self.sort_type}.txt')
        with open(stats_path, mode='w', encoding='utf-8') as file:
            # Format the genre name for display
            formatted_genre = self.genre.capitalize()
            if formatted_genre == "Science-fiction":
                formatted_genre = "Science Fiction"
            elif formatted_genre == "Animation":
                formatted_genre = "Animated"

            # Write header
            if self.sort_type == "popular":
                file.write(f"<strong>The Top {len(max_movies_2500_stats['film_data'])} Most Popular {formatted_genre} Narrative Feature Films on Letterboxd</strong>\n\n")
            else:
                file.write(f"<strong>The Top {len(max_movies_2500_stats['film_data'])} Highest Rated {formatted_genre} Narrative Feature Films on Letterboxd</strong>\n\n")
            
            # Write last updated date
            current_date = datetime.now()
            formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"
            file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
            file.write("<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>\n\n")
            
            # Write eligibility criteria
            file.write("<strong>Film eligibility criteria:</strong>\n")
            file.write("-- Must have a minimum of 1,000 reviews on Letterboxd.\n")
            file.write("-- Cannot be a short film (minimum 40 minutes).\n")
            file.write("-- Cannot be a television miniseries.\n")
            file.write("-- Cannot be a compilation of short serials.\n")
            file.write("-- Cannot be a documentary.\n")
            file.write("-- Cannot be a non-narrative project (paint drying for 10 hours, a timelapse of the construction of a building, abstract images, etc).\n")
            file.write("-- Cannot be a recording of a live performance (stand-up specials, recordings of live theater, concert films, etc).\n")
            file.write("-- Cannot be a television special episode, though feature film spin-offs from television shows are allowed.\n")
            file.write("-- Feature film spin-offs from television shows must contain original material, not just recap or compilation of existing material.\n")
            file.write("-- Entries that have scores inflated because they share a name with a popular television show are removed, as I notice them.\n\n")
            
            # Write top 10 statistics for this category
            for category_name, counts in max_movies_2500_stats.items():
                if category_name != 'film_data':
                    display_name = category_display_names.get(category_name, category_name.replace('_counts', ''))
                    file.write(f"<strong>The ten most appearing {display_name}:</strong>\n")
                    for item, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                        file.write(f"{item}: {count}\n")
                    file.write("\n")
            file.write("If you notice any movies you believe should/should not be included just let me know!")

    def save_results(self):
        """Save all results to files"""
        # Save unfiltered approved data (append mode)
        approved_path = os.path.join(BASE_DIR, 'unfiltered_approved.csv')
        with open(approved_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_approved:
                writer.writerow(movie + [f"{self.genre.capitalize()}_{self.sort_type}"])  # Append genre and sort type

        # Save unfiltered denied data (append mode)
        denied_path = os.path.join(BASE_DIR, 'unfiltered_denied.csv')
        with open(denied_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_denied:
                writer.writerow(movie + [f"{self.genre.capitalize()}_{self.sort_type}"])  # Append genre and sort type

        # Save MAX_MOVIES_2500 results
        self.save_max_movies_2500_results()

    def log_error_to_csv(self, error_message: str):
        """Log error messages to update_results.csv."""
        error_path = os.path.join(BASE_DIR, 'update_results.csv')
        with open(error_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Error Type', 'Error Message'])
            writer.writerow([type(error_message).__name__, error_message])  # Write the error type and message

    def update_statistics_for_movie(self, film_title: str, release_year: str, tmdb_id: str, driver, film_url: str = None):
        """Update statistics for the given movie."""
        try:
            # Extract directors
            movie_directors = []
            try:
                director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
                for director in director_elements:
                    director_name = director.text.strip()
                    if director_name:
                        movie_directors.append(director_name)
                        self.processor.director_counts[director_name] = self.processor.director_counts.get(director_name, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting directors: {str(e)}")

            # Extract actors
            movie_actors = []
            try:
                actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
                for actor in actor_elements:
                    actor_name = actor.text.strip()
                    if actor_name:
                        movie_actors.append(actor_name)
                        self.processor.actor_counts[actor_name] = self.processor.actor_counts.get(actor_name, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting actors: {str(e)}")

            # Extract decade
            try:
                decade = (int(release_year) // 10) * 10
                self.processor.decade_counts[decade] = self.processor.decade_counts.get(decade, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting decade: {str(e)}")

            # Extract genres
            movie_genres = []
            try:
                genre_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug[href*="/films/genre/"]')
                for genre in genre_elements:
                    genre_name = genre.get_attribute('textContent').strip()
                    if genre_name and not any(char in genre_name for char in ['…', 'Show All']):
                        movie_genres.append(genre_name)
                        self.processor.genre_counts[genre_name] = self.processor.genre_counts.get(genre_name, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting genres: {str(e)}")

            # Extract studios
            movie_studios = []
            try:
                studio_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/studio/"]')
                for studio in studio_elements:
                    studio_name = studio.get_attribute('textContent').strip()
                    if studio_name:
                        movie_studios.append(studio_name)
                        self.processor.studio_counts[studio_name] = self.processor.studio_counts.get(studio_name, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting studios: {str(e)}")

            # Extract languages
            movie_languages = set()
            try:
                headings = driver.find_elements(By.CSS_SELECTOR, '#tab-details h3')
                for heading in headings:
                    span = heading.find_element(By.TAG_NAME, 'span')
                    heading_text = span.get_attribute('textContent').strip() if span else heading.get_attribute('textContent').strip()
                    
                    if any(lang in heading_text for lang in ["Language", "Primary Language", "Languages", "Primary Languages"]):
                        try:
                            sluglist = heading.find_element(By.XPATH, "following-sibling::div[contains(@class, 'text-sluglist')]")
                            if sluglist:
                                p_tag = sluglist.find_element(By.TAG_NAME, 'p')
                                language_elements = p_tag.find_elements(By.CSS_SELECTOR, 'a.text-slug[href*="/films/language/"]')
                                for language in language_elements:
                                    language_name = language.get_attribute('textContent').strip()
                                    if language_name:
                                        movie_languages.add(language_name)
                                        self.processor.language_counts[language_name] = self.processor.language_counts.get(language_name, 0) + 1
                        except Exception:
                            pass
            except Exception as e:
                print_to_csv(f"Error extracting languages: {str(e)}")

            # Extract rating count
            rating_count = 0
            try:
                page_source = self.driver.page_source
                match = re.search(r'ratingCount":(\d+)', page_source)
                if match:
                    rating_count = int(match.group(1))
            except Exception as e:
                print_to_csv(f"Error extracting rating count: {str(e)}")

            # Extract countries
            movie_countries = []
            try:
                country_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]')
                for country in country_elements:
                    country_name = country.get_attribute('textContent').strip()
                    if country_name:
                        movie_countries.append(country_name)
                        self.processor.country_counts[country_name] = self.processor.country_counts.get(country_name, 0) + 1
            except Exception as e:
                print_to_csv(f"Error extracting countries: {str(e)}")

            # Extract runtime
            runtime = None
            try:
                runtime_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                )
                runtime_text = runtime_element.text
                match = re.search(r'(\d+)\s*min(?:s)?', runtime_text)
                if match:
                    runtime = int(match.group(1))
            except Exception as e:
                print_to_csv(f"Error extracting runtime: {str(e)}")

            # Create movie data dictionary
            movie_data = {
                'Title': film_title,
                'Year': release_year,
                'tmdbID': tmdb_id,
                'MPAA': extract_mpaa_rating(self.driver),
                'Runtime': runtime,
                'RatingCount': rating_count,
                'Languages': list(movie_languages),
                'Countries': movie_countries,
                'Decade': (int(release_year) // 10) * 10,
                'Directors': movie_directors,
                'Genres': movie_genres,
                'Studios': movie_studios,
                'Actors': movie_actors
            }

            # Only update whitelist if the movie is already in it
            if self.processor.is_whitelisted(film_title, release_year):
                if self.processor.update_whitelist(film_title, release_year, movie_data, film_url):
                    print_to_csv(f"📝 Successfully updated whitelist data for {film_title}")
                    # Process through all output channels
                    self.processor.process_whitelist_info(movie_data)
                    
                    # Process MAX_MOVIES_2500 using centralized function
                    if add_to_max_movies_2500(film_title, release_year, tmdb_id):
                        self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)
                    
                    self.valid_movies_count += 1
                    print_to_csv(f"✅ Processed whitelist data for {film_title} ({self.valid_movies_count}/{MAX_MOVIES})")
                    return movie_data

        except Exception as e:
            print_to_csv(f"Error in update_statistics_for_movie: {str(e)}")
            return None
    
def main():
    genres = ["action", "adventure", "animation", "comedy", "crime", "drama", "family", "fantasy", "history", "horror", "music", "mystery", "romance", "science-fiction", "thriller", "war", "western"]
    start_time = time.time()
    
    for genre in genres:
        for sort_type in ["rating", "popular"]:
            scraper = None
            try:
                print_to_csv(f"\n{'Starting New Genre/Sort Type':=^100}")
                print_to_csv(f"Genre: {genre.capitalize()}")
                print_to_csv(f"Sort Type: {sort_type.capitalize()}")
                
                # Reset max_movies_2500_stats for each new genre/sort type combination
                global max_movies_2500_stats
                max_movies_2500_stats = {
                    'film_data': [],
                    'director_counts': defaultdict(int),
                    'actor_counts': defaultdict(int),
                    'decade_counts': defaultdict(int),
                    'genre_counts': defaultdict(int),
                    'studio_counts': defaultdict(int),
                    'language_counts': defaultdict(int),
                    'country_counts': defaultdict(int)
                }
                
                scraper = LetterboxdScraper(genre=genre, sort_type=sort_type)
                scraper.scrape_movies()
                scraper.save_results()

                # Format execution time
                execution_time = time.time() - start_time
                print_to_csv(f"\n{'Execution Summary':=^100}")
                print_to_csv(f"Total execution time: {format_time(execution_time)}")
                print_to_csv(f"Average processing speed: {scraper.valid_movies_count / execution_time:.2f} movies/second")

            except Exception as e:
                print_to_csv(f"\n{'Error':=^100}")
                print_to_csv(f"❌ An error occurred during execution: {e}")
            finally:
                if scraper is not None:
                    try:
                        scraper.driver.quit()
                    except:
                        pass

if __name__ == "__main__":
    main()