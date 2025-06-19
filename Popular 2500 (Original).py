# Import necessary libraries
import time
import random
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import requests
from bs4 import BeautifulSoup
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

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open('Outputs/All_Outputs.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

# Configure locale and constants
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
MAX_MOVIES = 7000 # Currently using 7000
MAX_MOVIES_2500 = 2500
MAX_MOVIES_MPAA = 250
MAX_MOVIES_RUNTIME = 250
MAX_MOVIES_CONTINENT = 250

# Configure settings
MIN_RATING_COUNT = 1000
MIN_RUNTIME = 40
MAX_RETRIES = 25
RETRY_DELAY = 15
CHUNK_SIZE = 1900

# Configure specific maxes
MAX_180 = 75
MAX_240 = 5
MAX_MOVIES_G = 200
MAX_MOVIES_NC17 = 25
MAX_MOVIES_AFRICA = 20
MAX_MOVIES_OCEANIA = 150
MAX_MOVIES_SOUTH_AMERICA = 100

# File paths
BASE_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs'
LIST_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping'
BLACKLIST_PATH = os.path.join(LIST_DIR, 'blacklist.xlsx')
WHITELIST_PATH = os.path.join(LIST_DIR, 'whitelist.xlsx')

# TMDb API key
TMDB_API_KEY = ''

# Filtering criteria
FILTER_KEYWORDS = {
    'concert film', 'miniseries',
    'live performance', 'filmed theater', 'live theater', 
    'stand-up comedy', 'edited from tv series'
}

FILTER_GENRES = {'Documentary'}

# Add new constants for MPAA ratings
MPAA_RATINGS = ['G', 'PG', 'PG-13', 'R', 'NC-17', 'NR']
mpaa_stats = {rating: {'film_data': [], 'director_counts': defaultdict(int), 'actor_counts': defaultdict(int), 
                       'decade_counts': defaultdict(int), 'genre_counts': defaultdict(int), 
                       'studio_counts': defaultdict(int), 'language_counts': defaultdict(int), 
                       'country_counts': defaultdict(int)} for rating in MPAA_RATINGS}

# Add new constants for runtime categories
RUNTIME_CATEGORIES = {
    '90_Minutes_or_Less': [],
    '120_Minutes_or_Less': [],
    '180_Minutes_or_Greater': [],
    '240_Minutes_or_Greater': []
}

runtime_stats = {
    '90_Minutes_or_Less': {'film_data': [], 'director_counts': defaultdict(int), 'actor_counts': defaultdict(int), 
                     'decade_counts': defaultdict(int), 'genre_counts': defaultdict(int), 
                     'studio_counts': defaultdict(int), 'language_counts': defaultdict(int), 
                     'country_counts': defaultdict(int)},
    '120_Minutes_or_Less': {'film_data': [], 'director_counts': defaultdict(int), 'actor_counts': defaultdict(int), 
                      'decade_counts': defaultdict(int), 'genre_counts': defaultdict(int), 
                      'studio_counts': defaultdict(int), 'language_counts': defaultdict(int), 
                      'country_counts': defaultdict(int)},
    '180_Minutes_or_Greater': {'film_data': [], 'director_counts': defaultdict(int), 'actor_counts': defaultdict(int), 
                         'decade_counts': defaultdict(int), 'genre_counts': defaultdict(int), 
                         'studio_counts': defaultdict(int), 'language_counts': defaultdict(int), 
                         'country_counts': defaultdict(int)},
    '240_Minutes_or_Greater': {'film_data': [], 'director_counts': defaultdict(int), 'actor_counts': defaultdict(int), 
                         'decade_counts': defaultdict(int), 'genre_counts': defaultdict(int), 
                         'studio_counts': defaultdict(int), 'language_counts': defaultdict(int), 
                         'country_counts': defaultdict(int)}
}

# Define continents and their associated countries in a case-insensitive manner
CONTINENTS_COUNTRIES = {
    'Africa': ['Algeria', 'Angola', 'Benin', 'Botswana', 'Burkina Faso', 'Burundi', 'Cabo Verde', 'Cameroon', 'Central African Republic', 'Chad', 'Comoros', 'Congo, Democratic Republic of the', 'Congo, Republic of the', 'Djibouti', 'Egypt', 'Equatorial Guinea', 'Eritrea', 'Eswatini', 'Ethiopia', 'Gabon', 'Gambia', 'Ghana', 'Guinea', 'Guinea-Bissau', 'Kenya', 'Lesotho', 'Liberia', 'Libya', 'Madagascar', 'Malawi', 'Mali', 'Mauritania', 'Mauritius', 'Morocco', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Rwanda', 'Sao Tome and Principe', 'Senegal', 'Seychelles', 'Sierra Leone', 'Somalia', 'South Africa', 'South Sudan', 'Sudan', 'Tanzania', 'Togo', 'Tunisia', 'Uganda', 'Zambia', 'Zimbabwe'],
    'Asia': ['State of Palestine', 'Hong Kong', 'Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan', 'Brunei', 'Cambodia', 'China', 'Cyprus', 'Georgia', 'India', 'Indonesia', 'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Lebanon', 'Malaysia', 'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 'North Korea', 'Oman', 'Pakistan', 'Palestine', 'Philippines', 'Qatar', 'Russia', 'Saudi Arabia', 'Singapore', 'South Korea', 'Sri Lanka', 'Syrian Arab Republic', 'Taiwan', 'Tajikistan', 'Thailand', 'Timor-Leste', 'Turkey', 'Turkmenistan', 'United Arab Emirates', 'Uzbekistan', 'Vietnam', 'Yemen'],
    'Europe': ['East Germany', 'North Macedonia', 'Yugoslavia', 'Serbia and Montenegro', 'Czechoslovakia', 'Czechia', 'USSR', 'Albania', 'Latvia', 'Andorra', 'Liechtenstein', 'Armenia', 'Lithuania', 'Austria', 'Luxembourg', 'Azerbaijan', 'Malta', 'Belarus', 'Moldova', 'Belgium', 'Monaco', 'Bosnia and Herzegovina', 'Montenegro', 'Bulgaria', 'Netherlands', 'Croatia', 'Norway', 'Cyprus', 'Poland', 'Czech Republic', 'Portugal', 'Denmark', 'Romania', 'Estonia', 'Russia', 'Finland', 'San Marino', 'Former Yugoslav Republic of Macedonia', 'Serbia', 'France', 'Slovakia', 'Georgia', 'Slovenia', 'Germany', 'Spain', 'Greece', 'Sweden', 'Hungary', 'Switzerland', 'Iceland', 'Ireland', 'Turkey', 'Italy', 'Ukraine', 'Kosovo', 'UK'],
    'North America': ['Cuba', 'The Bahamas', 'Bermuda', 'Canada', 'The Caribbean', 'Clipperton Island', 'Greenland', 'Mexico', 'Saint Pierre and Miquelon', 'Turks and Caicos Islands', 'USA', 'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama'],
    'Oceania': ['Australia', 'Fiji', 'Kiribati', 'Marshall Islands', 'Micronesia', 'Nauru', 'New Zealand', 'Palau', 'Papua New Guinea', 'Samoa', 'Solomon Islands', 'Tonga', 'Tuvalu', 'Vanuatu'],
    'South America': ['Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Bolivarian Republic of Venezuela', 'The Falkland Islands', 'South Georgia and the South Sandwich Islands', 'French Guiana'],
}

# Initialize continent stats with additional counts
continent_stats = {
    continent: {
        'film_data': [],
        'country_counts': defaultdict(int),
        'director_counts': defaultdict(int),
        'actor_counts': defaultdict(int),
        'decade_counts': defaultdict(int),
        'genre_counts': defaultdict(int),
        'studio_counts': defaultdict(int),
        'language_counts': defaultdict(int)
    } for continent in CONTINENTS_COUNTRIES.keys()
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
        self.whitelist = pd.read_excel(WHITELIST_PATH, header=0, names=['Title', 'Year'], usecols=[0,1])
        # Update blacklist loading to include the Link column
        self.blacklist = pd.read_excel(BLACKLIST_PATH, header=0, names=['Title', 'Year', 'Reason', 'Link'], usecols=[0, 1, 2, 3])
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

        # Normalize titles and years in whitelist and blacklist
        self.whitelist['Title'] = self.whitelist['Title'].apply(normalize_text)
        self.whitelist['Year'] = self.whitelist['Year'].astype(str).str.strip()
        self.blacklist['Title'] = self.blacklist['Title'].apply(normalize_text)
        self.blacklist['Year'] = self.blacklist['Year'].astype(str).str.strip()
        # Fill empty links with empty string instead of None
        self.blacklist['Link'] = self.blacklist['Link'].fillna('')

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
        film_title = normalize_text(film_title)
        release_year = str(release_year).strip()
        return any(
            (film_title.lower() == str(row['Title']).lower() and release_year == row['Year'])
            for _, row in self.whitelist.iterrows()
        )

    def is_blacklisted(self, film_title: str, release_year: str = None, film_url: str = None) -> bool:
        """Check if a movie is in the blacklist using a lookup dictionary."""
        # Normalize the title for comparison
        normalized_title = normalize_text(film_title).lower()
        
        # Find all matching titles in blacklist
        matching_entries = self.blacklist[
            self.blacklist['Title'].apply(normalize_text).str.lower() == normalized_title
        ]
        
        if matching_entries.empty:
            return False
            
        # If we have a URL, check for exact match first
        if film_url:
            for _, row in matching_entries.iterrows():
                if row['Link'] == film_url:
                    return True
                elif row['Link'] == '':  # If link is empty, check year match
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

    def extract_runtime(self, soup: BeautifulSoup, film_title: str) -> Optional[int]:
        runtime_tag = soup.find('p', class_='text-link text-footer')
        if runtime_tag:
            # Use a regular expression to find the runtime in minutes
            match = re.search(r'(\d+)\s*mins', runtime_tag.text)
            if match:
                runtime = int(match.group(1))
                # print_to_csv(f"Extracted runtime: {runtime} minutes")
                return runtime
        
        # Print message and return None if no runtime is found
        print_to_csv(f"⚠️ No runtime found. Skipping {film_title}.")
        return None  # Updated to return None instead of raising an exception


def setup_webdriver() -> webdriver.Firefox:
    options = Options()
    options.headless = True
    options.set_preference("permissions.default.image", 2)  # Disable images
    options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", "false")
    options.set_preference("browser.display.use_document_fonts", 0)
    options.set_preference("browser.display.document_color_use", 2)
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
    try:
        country_elements = driver.find_elements(By.CSS_SELECTOR, '.release-country-list .release-country')
        for country in country_elements:
            country_name = country.find_element(By.CSS_SELECTOR, '.name').text.strip()
            if country_name == "USA":
                rating_element = country.find_element(By.CSS_SELECTOR, '.release-certification-badge .label')
                if rating_element:
                    return rating_element.text.strip()
        return None
    except Exception:
        return None

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

class LetterboxdScraper:
    def __init__(self):
        self.driver = setup_webdriver()
        self.processor = MovieProcessor()
        self.base_url = 'https://letterboxd.com/films/by/popular/'
        self.total_titles = 0
        self.processed_titles = 0
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()
        self.unknown_continent_films = []  # Initialize the list for unknown continent films
        self.top_movies_count = 0  # Track the number of movies added to the top 2500 list
        print_to_csv("Initialized Letterboxd Scraper.")

    def scrape_movies(self):
        try:
            while self.valid_movies_count < MAX_MOVIES:
                url = f'{self.base_url}page/{self.page_number}/'
                retries = 0
                success = False

                # Page loading with retries
                while retries < MAX_RETRIES and not success:
                    try:
                        print_to_csv(f'Loading page {self.page_number}...')
                        print_to_csv(f'Sending GET request to: {url}')
                        self.driver.get(url)
                        print_to_csv(f'Received response. Parsing HTML content...')
                        time.sleep(random.uniform(1.0, 1.5))
                        
                        film_containers = self.driver.find_elements(By.CSS_SELECTOR, 'div.react-component.poster')
                        if len(film_containers) == 72:
                            success = True
                        else:
                            print_to_csv(f"Unexpected number of film containers ({len(film_containers)}). Retrying...")
                            retries += 1
                            time.sleep(RETRY_DELAY)
                    
                    except Exception as e:
                        print_to_csv(f"Connection error: {e}. Retrying in {RETRY_DELAY} seconds...")
                        retries += 1
                        time.sleep(RETRY_DELAY)

                if not success:
                    error_message = f"Failed to retrieve the expected number of film containers after {MAX_RETRIES} attempts."
                    print_to_csv(f"❌ {error_message}")
                    # Log the specific error to update_results.csv
                    self.log_error_to_csv(error_message)
                    break  # Exit the loop if the expected number of film containers is not found

                print_to_csv(f"\n{f' Page {self.page_number} ':=^100}")

                # Collect all film URLs and titles first
                film_data_list = []
                for container in film_containers:
                    film_title = container.get_attribute('data-film-name')
                    film_url = container.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
                    film_data_list.append((film_title, film_url))

                # Then, process each film
                for film_title, film_url in film_data_list:
                    if self.valid_movies_count >= MAX_MOVIES:
                        print_to_csv(f"\nReached the target of {MAX_MOVIES} successful movies. Stopping scraping.")
                        return

                    self.driver.get(film_url)
                    time.sleep(random.uniform(1.0, 1.5))

                    # --- Extract Release Year ---
                    try:
                        meta_tag = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
                        content = meta_tag.get_attribute('content')
                        # Extract the year from the content string
                        if content and '(' in content and ')' in content:
                            release_year = content.split('(')[-1].split(')')[0].strip()
                            print(f"Release year: {release_year}")
                        else:
                            release_year = None
                            print("Release year not found in og:title content.")
                    except Exception as e:
                        release_year = None
                        print(f"Release year not found: {e}")

                    # --- Extract TMDb ID ---
                    try:
                        tmdb_id = self.driver.find_element(By.TAG_NAME, 'body').get_attribute('data-tmdb-id')
                        print(f"TMDb ID: {tmdb_id}")
                    except Exception as e:
                        tmdb_id = None
                        print(f"TMDb ID not found: {e}")

                    # --- Extract Rating Count ---
                    rating_count = 0
                    try:
                        page_source = self.driver.page_source
                        match = re.search(r'ratingCount":(\d+)', page_source)
                        if match:
                            rating_count = int(match.group(1))
                            print(f"Rating count: {rating_count}")
                        else:
                            print("Rating count not found in page source.")
                    except Exception as e:
                        print(f"Error extracting rating count: {e}")

                    # --- Extract Runtime ---
                    try:
                        runtime_text = self.driver.find_element(By.CSS_SELECTOR, 'p.text-link.text-footer').text
                        match = re.search(r'(\d+)\s*mins', runtime_text)
                        if match:
                            runtime = int(match.group(1))
                            print(f"Runtime: {runtime} minutes")
                        else:
                            runtime = None
                            print("Runtime not found in text-footer.")
                    except Exception as e:
                        runtime = None
                        print(f"Runtime not found: {e}")

                    self.total_titles += 1

                    # Check 1: Rating count minimum
                    if rating_count < MIN_RATING_COUNT:
                        print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Insufficient ratings (< 1000)'])
                        continue

                    # Check 2: Whitelist
                    if self.processor.is_whitelisted(film_title, release_year):
                        movie_identifier = (film_title.lower(), release_year)
                        if movie_identifier not in self.processor.added_movies:
                            # Process the approved movie
                            self.process_approved_movie(film_title, release_year, tmdb_id, "whitelisted")
                            print_to_csv(f"✅ {film_title} was added due to being whitelisted ({self.valid_movies_count + 1}/{MAX_MOVIES})")  # Updated reference
                            self.valid_movies_count += 1  # Increment count for whitelisted movies
                            continue  # Skip to the next movie

                    # Check 3: Blacklist
                    if self.processor.is_blacklisted(film_title, release_year):
                        print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Blacklisted'])
                        continue

                    # Check 4: Runtime
                    if runtime is None:
                        continue  # Skip this film entirely if runtime is None
                    if runtime < MIN_RUNTIME:
                        print_to_csv(f"❌ {film_title} was not added due to a short runtime of {runtime} minutes.")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Short runtime'])
                        self.processor.add_to_blacklist(film_title, release_year, 'Short runtime')
                        continue

                    # Check 5: TMDB ID
                    if not tmdb_id:
                        print_to_csv(f"❌ {film_title} was not added due to missing TMDB ID.")
                        self.processor.rejected_data.append([film_title, release_year, None, 'Missing TMDB ID'])
                        self.processor.unfiltered_denied.append([film_title, release_year, None])
                        continue

                    # Check 6: Keywords and Genres
                    if tmdb_id:
                        keywords, genres = self.processor.fetch_tmdb_details(tmdb_id)
                        
                        # Check keywords
                        matching_keywords = [k for k in FILTER_KEYWORDS if k in keywords]
                        if matching_keywords:
                            rejection_reason = f"Due to being a {', '.join(matching_keywords)}."
                            print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                            self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                            self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                            continue

                        # Check genres
                        matching_genres = [g for g in FILTER_GENRES if g in genres]
                        if matching_genres:
                            rejection_reason = f"Due to being a {', '.join(matching_genres)}."
                            print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                            self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                            self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                            continue

                    # If we reach here, the movie is approved
                    if len(self.processor.film_data) < MAX_MOVIES:  # Only add to filtered films if under MAX_MOVIES
                        self.process_approved_movie(film_title, release_year, tmdb_id, "approved")
                        print_to_csv(f"✅ {film_title} was approved ({self.valid_movies_count + 1}/{MAX_MOVIES})")  # Updated reference
                        self.valid_movies_count += 1  # Increment count for approved movies

                    # Add to unfiltered_approved regardless of list sizes
                    if not self.processor.is_whitelisted(film_title, release_year):
                        # Check if the movie is already in the unfiltered_approved list
                        if not any(film_title.lower() == movie[0].lower() and release_year == movie[1] for movie in self.processor.unfiltered_approved):
                            self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id])

                    # Log rejection reasons for blacklisted films
                    if self.processor.is_blacklisted(film_title, release_year):
                        print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Blacklisted'])
                        continue

                elapsed_time = time.time() - self.start_time
                movies_per_second = self.valid_movies_count / elapsed_time if elapsed_time > 0 else 0
                estimated_total_time = MAX_MOVIES / movies_per_second if movies_per_second > 0 else 0
                time_remaining = estimated_total_time - elapsed_time if estimated_total_time > 0 else 0
                print_to_csv(f"\n{f'Overall Progress: {self.valid_movies_count}/{MAX_MOVIES} films':^100}")  # Updated reference
                print_to_csv(f"{f'Elapsed Time: {format_time(elapsed_time)} | Estimated Time Remaining: {format_time(time_remaining)}':^100}")
                print_to_csv(f"{f'Processing Speed: {movies_per_second:.2f} movies/second':^100}")

                print_to_csv(f"\n{f'Completed Page {self.page_number}':=^100}")
                self.page_number += 1

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            print_to_csv(f"❌ {error_message}")
            self.log_error_to_csv(error_message)

    def process_approved_movie(self, film_title: str, release_year: str, tmdb_id: str, approval_type: str):
        if self.valid_movies_count >= MAX_MOVIES:
            return

        movie_identifier = (film_title.lower(), release_year)
        if movie_identifier in self.processor.added_movies:
            return

        # Add the movie to the filtered films data only if we haven't reached MAX_MOVIES
        if len(self.processor.film_data) < MAX_MOVIES:
            self.processor.film_data.append({
                'Title': film_title,
                'Year': release_year,
                'tmdbID': tmdb_id
            })
            self.processor.added_movies.add((film_title.lower(), release_year))

        # NEW: Add the movie to MAX_MOVIES_2500 stats if we haven't reached the threshold
        if len(max_movies_2500_stats['film_data']) < MAX_MOVIES_2500:
            max_movies_2500_stats['film_data'].append({
                'Title': film_title,
                'Year': release_year,
                'tmdbID': tmdb_id
            })
            self.update_max_movies_2500_statistics(film_title, release_year, tmdb_id, self.driver)

        # Extract runtime using Selenium
        runtime = None
        try:
            runtime_text = self.driver.find_element(By.CSS_SELECTOR, 'p.text-link.text-footer').text
            match = re.search(r'(\d+)\s*mins', runtime_text)
            if match:
                runtime = int(match.group(1))
        except Exception:
            runtime = None

        self.process_runtime_category(film_title, release_year, tmdb_id, runtime, self.driver)
        self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver)

    def update_max_movies_2500_statistics(self, film_title: str, release_year: str, tmdb_id: str, driver):
        """Update statistics for the given movie for MAX_MOVIES_2500."""
        # Find the movie in film_data
        movie_data = next((movie for movie in max_movies_2500_stats['film_data'] 
                          if movie['Title'] == film_title and movie['Year'] == release_year), None)
        
        if not movie_data:
            return

        # Directors
        try:
            director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
            for director in director_elements:
                director_name = director.text.strip()
                if director_name:
                    max_movies_2500_stats['director_counts'][director_name] += 1
        except Exception:
            pass

        # Actors
        try:
            actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
            for actor in actor_elements:
                actor_name = actor.text.strip()
                if actor_name:
                    max_movies_2500_stats['actor_counts'][actor_name] += 1
        except Exception:
            pass

        # Decade
        try:
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            content = meta_tag.get_attribute('content')
            if content and '(' in content and ')' in content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                max_movies_2500_stats['decade_counts'][decade] += 1
        except Exception:
            pass

        # Genres - Only get main genres, not microgenres
        try:
            genre_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug[href*="/films/genre/"]')
            genres = []
            for genre in genre_elements:
                genre_name = genre.get_attribute('textContent').strip()
                if genre_name and not any(char in genre_name for char in ['…', 'Show All']):
                    genres.append(genre_name)
                    max_movies_2500_stats['genre_counts'][genre_name] += 1
            movie_data['Genres'] = genres
        except Exception:
            pass

        # Studios
        try:
            studio_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/studio/"]')
            studios = []
            for studio in studio_elements:
                studio_name = studio.get_attribute('textContent').strip()
                if studio_name:
                    studios.append(studio_name)
                    max_movies_2500_stats['studio_counts'][studio_name] += 1
            movie_data['Studios'] = studios
        except Exception:
            pass

        # Languages
        try:
            language_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/films/language/"]')
            languages = []
            for language in language_elements:
                language_name = language.get_attribute('textContent').strip()
                if language_name:
                    languages.append(language_name)
                    max_movies_2500_stats['language_counts'][language_name] += 1
            movie_data['Languages'] = languages
        except Exception:
            pass

        # Countries
        try:
            country_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]')
            countries = []
            for country in country_elements:
                country_name = country.get_attribute('textContent').strip()
                if country_name:
                    countries.append(country_name)
                    max_movies_2500_stats['country_counts'][country_name] += 1
            movie_data['Countries'] = countries
        except Exception:
            pass

    def process_runtime_category(self, film_title: str, release_year: str, tmdb_id: str, runtime: int, driver):
        """Process the runtime category for a movie and extract all its metadata"""
        categories = []  # Initialize a list to hold categories

        if runtime < 91:
            categories.append('90_Minutes_or_Less')
        if runtime < 121:
            categories.append('120_Minutes_or_Less')
        if runtime > 179:
            categories.append('180_Minutes_or_Greater')
        if runtime > 239:
            categories.append('240_Minutes_or_Greater')

        if not categories:
            return  # Not in any category we care about

        # Add the movie to the film_data for each category
        for category in categories:
            runtime_stats[category]['film_data'].append({
                'Title': film_title,
                'Year': release_year,
                'tmdbID': tmdb_id
            })

            # Now check if we should update statistics
            max_movies_limit = (
                MAX_180 if category == '180_Minutes_or_Greater' else
                MAX_240 if category == '240_Minutes_or_Greater' else
                MAX_MOVIES_RUNTIME
            )
            if len(runtime_stats[category]['film_data']) <= max_movies_limit:
                self.update_runtime_statistics(film_title, release_year, tmdb_id, driver, category)

    def update_runtime_statistics(self, film_title: str, release_year: str, tmdb_id: str, driver, category: str):
        """Update statistics for the given runtime category."""
        if (len(runtime_stats[category]['film_data']) - 1) >= MAX_MOVIES_RUNTIME:
            return  # Skip updating if we already have enough movies

        # Extract directors
        try:
            director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
            for director in director_elements:
                director_name = director.text.strip()
                if director_name:
                    runtime_stats[category]['director_counts'][director_name] += 1
        except Exception:
            pass

        # Extract actors without roles
        try:
            actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
            for actor in actor_elements:
                actor_name = actor.text.strip()
                if actor_name:
                    runtime_stats[category]['actor_counts'][actor_name] += 1
        except Exception:
            pass

        # Extract decades
        try:
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            content = meta_tag.get_attribute('content')
            if content and '(' in content and ')' in content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                runtime_stats[category]['decade_counts'][decade] += 1
        except Exception:
            pass

        # Extract genres
        try:
            print(f"\nExtracting genres for {film_title}...")
            # Try multiple possible selectors for genres
            genre_selectors = [
                '#tab-genres .text-sluglist a.text-slug',
                '.text-sluglist a.text-slug[href*="/films/genre/"]',
                'a[href*="/films/genre/"]'
            ]
            
            genres = []
            for selector in genre_selectors:
                genre_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if genre_elements:
                    print(f"\nFound {len(genre_elements)} genre elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(genre_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    genres = [el.get_attribute('textContent').strip() for el in genre_elements if el.get_attribute('textContent').strip()]
                    if genres:
                        break
            
            print(f"\nExtracted genres: {genres}")
            for genre in genres:
                runtime_stats[category]['genre_counts'][genre] += 1
            print(f"Updated genre counts: {dict(runtime_stats[category]['genre_counts'])}")
        except Exception as e:
            print(f"Error extracting genres: {str(e)}")

        # Extract studios
        try:
            print(f"\nExtracting studios for {film_title}...")
            # Try multiple possible selectors for studios
            studio_selectors = [
                '#tab-details .text-sluglist a.text-slug[href*="/studio/"]',
                'a[href*="/studio/"]',
                '.text-sluglist a.text-slug[href*="/studio/"]'
            ]
            
            studios = []
            for selector in studio_selectors:
                studio_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if studio_elements:
                    print(f"\nFound {len(studio_elements)} studio elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(studio_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    studios = [el.get_attribute('textContent').strip() for el in studio_elements if el.get_attribute('textContent').strip()]
                    if studios:
                        break
            
            print(f"\nExtracted studios: {studios}")
            for studio in studios:
                runtime_stats[category]['studio_counts'][studio] += 1
            print(f"Updated studio counts: {dict(runtime_stats[category]['studio_counts'])}")
        except Exception as e:
            print(f"Error extracting studios: {str(e)}")

        # Extract languages
        try:
            language_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for language in language_elements:
                language_name = language.text.strip()
                if language_name:
                    runtime_stats[category]['language_counts'][language_name] += 1
        except Exception:
            pass

        # Extract countries
        try:
            print(f"\nExtracting countries for {film_title}...")
            # Try multiple possible selectors for countries
            country_selectors = [
                '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]',
                'a[href*="/films/country/"]',
                '.text-sluglist a.text-slug[href*="/films/country/"]'
            ]
            
            countries = []
            for selector in country_selectors:
                country_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if country_elements:
                    print(f"\nFound {len(country_elements)} country elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(country_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    countries = [el.get_attribute('textContent').strip() for el in country_elements if el.get_attribute('textContent').strip()]
                    if countries:
                        break
            
            print(f"\nExtracted countries: {countries}")
            for country in countries:
                # Check if the country belongs to any continent
                for continent, country_list in CONTINENTS_COUNTRIES.items():
                    if country in country_list:
                        if len(continent_stats[continent]['film_data']) < MAX_MOVIES_CONTINENT:
                            continent_stats[continent]['film_data'].append({
                                'Title': film_title,
                                'Year': release_year,
                                'tmdbID': tmdb_id
                            })
                            self.update_continent_statistics(continent, driver)
                        break
                runtime_stats[category]['country_counts'][country] += 1
            print(f"Updated country counts: {dict(runtime_stats[category]['country_counts'])}")
        except Exception as e:
            print(f"Error extracting countries: {str(e)}")

    def update_statistics_for_movie(self, film_title: str, release_year: str, tmdb_id: str, driver):
        """Update statistics for the given movie."""
        # Extract directors
        try:
            director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
            for director in director_elements:
                director_name = director.text.strip()
                if director_name:
                    self.processor.director_counts[director_name] = self.processor.director_counts.get(director_name, 0) + 1
        except Exception:
            pass

        # Extract actors without roles
        try:
            actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
            for actor in actor_elements:
                actor_name = actor.text.strip()
                if actor_name:
                    self.processor.actor_counts[actor_name] = self.processor.actor_counts.get(actor_name, 0) + 1
        except Exception:
            pass

        # Extract decades
        try:
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            content = meta_tag.get_attribute('content')
            if content and '(' in content and ')' in content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                self.processor.decade_counts[decade] = self.processor.decade_counts.get(decade, 0) + 1
        except Exception:
            pass

        # Extract genres with debug prints
        try:
            print(f"\nExtracting genres for {film_title}...")
            # Try multiple possible selectors for genres
            genre_selectors = [
                '#tab-genres .text-sluglist a.text-slug',
                '.text-sluglist a.text-slug[href*="/films/genre/"]',
                'a[href*="/films/genre/"]'
            ]
            
            genres = []
            for selector in genre_selectors:
                genre_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if genre_elements:
                    print(f"\nFound {len(genre_elements)} genre elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(genre_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    genres = [el.get_attribute('textContent').strip() for el in genre_elements if el.get_attribute('textContent').strip()]
                    if genres:
                        break
            
            print(f"\nExtracted genres: {genres}")
            for genre in genres:
                self.processor.genre_counts[genre] = self.processor.genre_counts.get(genre, 0) + 1
            print(f"Updated genre counts: {dict(self.processor.genre_counts)}")
        except Exception as e:
            print(f"Error extracting genres: {str(e)}")

        # Extract studios with debug prints
        try:
            print(f"\nExtracting studios for {film_title}...")
            # Try multiple possible selectors for studios
            studio_selectors = [
                '#tab-details .text-sluglist a.text-slug[href*="/studio/"]',
                'a[href*="/studio/"]',
                '.text-sluglist a.text-slug[href*="/studio/"]'
            ]
            
            studios = []
            for selector in studio_selectors:
                studio_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if studio_elements:
                    print(f"\nFound {len(studio_elements)} studio elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(studio_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    studios = [el.get_attribute('textContent').strip() for el in studio_elements if el.get_attribute('textContent').strip()]
                    if studios:
                        break
            
            print(f"\nExtracted studios: {studios}")
            for studio in studios:
                self.processor.studio_counts[studio] = self.processor.studio_counts.get(studio, 0) + 1
            print(f"Updated studio counts: {dict(self.processor.studio_counts)}")
        except Exception as e:
            print(f"Error extracting studios: {str(e)}")

        # Extract languages
        try:
            language_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for language in language_elements:
                language_name = language.text.strip()
                if language_name:
                    self.processor.language_counts[language_name] = self.processor.language_counts.get(language_name, 0) + 1
        except Exception:
            pass

        # Extract countries with debug prints
        try:
            print(f"\nExtracting countries for {film_title}...")
            # Try multiple possible selectors for countries
            country_selectors = [
                '#tab-details .text-sluglist a.text-slug[href*="/films/country/"]',
                'a[href*="/films/country/"]',
                '.text-sluglist a.text-slug[href*="/films/country/"]'
            ]
            
            countries = []
            for selector in country_selectors:
                country_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if country_elements:
                    print(f"\nFound {len(country_elements)} country elements with selector: {selector}")
                    print("Raw elements found:")
                    for idx, el in enumerate(country_elements):
                        print(f"Element {idx + 1}:")
                        print(f"  Text: {el.get_attribute('textContent')}")
                        print(f"  HTML: {el.get_attribute('outerHTML')}")
                        print(f"  Href: {el.get_attribute('href')}")
                        print(f"  Class: {el.get_attribute('class')}")
                    
                    countries = [el.get_attribute('textContent').strip() for el in country_elements if el.get_attribute('textContent').strip()]
                    if countries:
                        break
            
            print(f"\nExtracted countries: {countries}")
            for country in countries:
                # Check if the country belongs to any continent
                for continent, country_list in CONTINENTS_COUNTRIES.items():
                    if country in country_list:
                        if len(continent_stats[continent]['film_data']) < MAX_MOVIES_CONTINENT:
                            continent_stats[continent]['film_data'].append({
                                'Title': film_title,
                                'Year': release_year,
                                'tmdbID': tmdb_id
                            })
                            self.update_continent_statistics(continent, driver)
                        break
                self.processor.country_counts[country] = self.processor.country_counts.get(country, 0) + 1
            print(f"Updated country counts: {dict(self.processor.country_counts)}")
        except Exception as e:
            print(f"Error extracting countries: {str(e)}")

        # Check if the release year is before 1968 for MPAA ratings
        release_date_str = f"01 {release_year}"  # Assuming the release date is the first of the year for comparison
        release_date = datetime.strptime(release_date_str, "%d %Y")  # Convert to datetime object
        cutoff_date = datetime(1968, 11, 1)  # Define the cutoff date

        if release_date < cutoff_date:
            mpaa_rating = None  # Do not assign an MPAA rating if the release date is before November 1, 1968
        else:
            mpaa_rating = extract_mpaa_rating(driver)

        if mpaa_rating in MPAA_RATINGS:
            # Add to the corresponding MPAA rating list regardless of MAX_MOVIES
            mpaa_stats[mpaa_rating]['film_data'].append({
                'Title': film_title,
                'Year': release_year,
                'tmdbID': tmdb_id
            })
            # Update counts for statistics
            self.update_statistics(mpaa_rating, self.driver)

    def update_continent_statistics(self, continent: str, driver):
        """Update statistics for the given continent."""
        # Determine the max limit based on the continent
        max_movies_limit = (
            MAX_MOVIES_AFRICA if continent == 'Africa' else
            MAX_MOVIES_OCEANIA if continent == 'Oceania' else
            MAX_MOVIES_SOUTH_AMERICA if continent == 'South America' else
            MAX_MOVIES_CONTINENT
        )

        # Check if the movie is in the top max_movies_limit
        if len(continent_stats[continent]['film_data']) > max_movies_limit:
            return  # Skip updating if we already have enough movies

        # Extract directors
        try:
            director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
            for director in director_elements:
                director_name = director.text.strip()
                if director_name:
                    continent_stats[continent]['director_counts'][director_name] += 1
        except Exception:
            pass

        # Extract actors without roles
        try:
            actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
            for actor in actor_elements:
                actor_name = actor.text.strip()
                if actor_name:
                    continent_stats[continent]['actor_counts'][actor_name] += 1
        except Exception:
            pass

        # Extract decades
        try:
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            content = meta_tag.get_attribute('content')
            if content and '(' in content and ')' in content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                continent_stats[continent]['decade_counts'][decade] += 1
        except Exception:
            pass

        # Extract genres
        try:
            genre_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug')
            genres = [el.text.strip() for el in genre_elements]
            for genre in genres:
                continent_stats[continent]['genre_counts'][genre] += 1
        except Exception:
            pass

        # Extract studios
        try:
            studio_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for studio in studio_elements:
                studio_name = studio.text.strip()
                if studio_name:
                    continent_stats[continent]['studio_counts'][studio_name] += 1
        except Exception:
            pass

        # Extract languages
        try:
            language_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for language in language_elements:
                language_name = language.text.strip()
                if language_name:
                    continent_stats[continent]['language_counts'][language_name] += 1
        except Exception:
            pass

        # Extract countries
        try:
            country_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for country in country_elements:
                country_name = country.text.strip()
                if country_name:
                    continent_stats[continent]['country_counts'][country_name] += 1
        except Exception:
            pass

    def update_statistics(self, mpaa_rating: str, driver):
        """Update statistics for the given MPAA rating."""
        # Determine the max limit based on the MPAA rating
        max_movies_limit = (
            MAX_MOVIES_G if mpaa_rating == 'G' else
            MAX_MOVIES_NC17 if mpaa_rating == 'NC-17' else
            MAX_MOVIES_MPAA
        )

        # Check if the movie is in the top max_movies_limit
        if len(mpaa_stats[mpaa_rating]['film_data']) > max_movies_limit:
            return  # Skip updating if we already have enough movies

        # Extract directors
        try:
            director_elements = driver.find_elements(By.CSS_SELECTOR, 'span.directorlist a.contributor')
            for director in director_elements:
                director_name = director.text.strip()
                if director_name:
                    mpaa_stats[mpaa_rating]['director_counts'][director_name] += 1
        except Exception:
            pass

        # Extract actors without roles
        try:
            actor_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-cast .text-sluglist a.text-slug.tooltip')
            for actor in actor_elements:
                actor_name = actor.text.strip()
                if actor_name:
                    mpaa_stats[mpaa_rating]['actor_counts'][actor_name] += 1
        except Exception:
            pass

        # Extract decades
        try:
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            content = meta_tag.get_attribute('content')
            if content and '(' in content and ')' in content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                mpaa_stats[mpaa_rating]['decade_counts'][decade] += 1
        except Exception:
            pass

        # Extract genres
        try:
            genre_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-genres .text-sluglist a.text-slug')
            genres = [el.text.strip() for el in genre_elements]
            for genre in genres:
                mpaa_stats[mpaa_rating]['genre_counts'][genre] += 1
        except Exception:
            pass

        # Extract studios
        try:
            studio_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for studio in studio_elements:
                studio_name = studio.text.strip()
                if studio_name:
                    mpaa_stats[mpaa_rating]['studio_counts'][studio_name] += 1
        except Exception:
            pass

        # Extract languages
        try:
            language_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for language in language_elements:
                language_name = language.text.strip()
                if language_name:
                    mpaa_stats[mpaa_rating]['language_counts'][language_name] += 1
        except Exception:
            pass

        # Extract countries
        try:
            country_elements = driver.find_elements(By.CSS_SELECTOR, '#tab-details .text-sluglist a.text-slug')
            for country in country_elements:
                country_name = country.text.strip()
                if country_name:
                    mpaa_stats[mpaa_rating]['country_counts'][country_name] += 1
        except Exception:
            pass

    def save_max_movies_2500_results(self):
        """Save results for MAX_MOVIES_2500."""
        # Save movie data in chunks
        num_chunks = (len(max_movies_2500_stats['film_data']) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(num_chunks):
            start_idx = i * CHUNK_SIZE
            end_idx = min((i + 1) * CHUNK_SIZE, len(max_movies_2500_stats['film_data']))
            chunk_df = pd.DataFrame(max_movies_2500_stats['film_data'][start_idx:end_idx])
            # Only include Title, Year, and tmdbID in the output
            chunk_df = chunk_df[['Title', 'Year', 'tmdbID']]
            output_path = os.path.join(BASE_DIR, f'rating_filtered_movie_titles{i+1}.csv')
            chunk_df.to_csv(output_path, index=False, encoding='utf-8')

        def get_ordinal(n):
            if 10 <= n % 100 <= 20:
                suffix = 'th'
            else:
                suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
            return str(n) + suffix

        current_date = datetime.now()
        formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"

        # Save statistics for this rating
        stats_path = os.path.join(BASE_DIR, f'rating_filtered_titles.txt')
        with open(stats_path, mode='w', encoding='utf-8') as file:
            file.write(f"<strong>The Top {len(max_movies_2500_stats['film_data'])} Highest Rated Narrative Feature Films on Letterboxd.</strong>\n\n")
            file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
            file.write("<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly!</a>\n\n")
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

            # Mapping for display names
            category_display_names = {
                'director_counts': 'directors',
                'actor_counts': 'actors',
                'decade_counts': 'decades',
                'genre_counts': 'genres',
                'studio_counts': 'studios',
                'language_counts': 'languages',
                'country_counts': 'countries'
            }

            # Write top 10 statistics for this category
            for category_name, counts in max_movies_2500_stats.items():
                if category_name != 'film_data':
                    display_name = category_display_names.get(category_name, category_name.replace('_counts', ''))
                    file.write(f"<strong>The ten most appearing {display_name}:</strong>\n")
                    for item, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                        file.write(f"{item}: {count}\n")
                    file.write("\n")
            file.write("If you notice any movies you believe should/should not be included just let me know!")
            
    def save_continent_results(self):
        """Save results for each continent."""
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

        current_date = datetime.now()
        formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"

        for continent in CONTINENTS_COUNTRIES.keys():
            continent_data = continent_stats[continent]['film_data']
            if continent_data:
                # Determine the max limit based on the continent
                max_limit = (
                    MAX_MOVIES_AFRICA if continent == 'Africa' else
                    MAX_MOVIES_OCEANIA if continent == 'Oceania' else
                    MAX_MOVIES_SOUTH_AMERICA if continent == 'South America' else
                    MAX_MOVIES_CONTINENT
                )
                # Limit to top results
                top_data = continent_data[:int(max_limit)]  # Ensure it does not exceed the max
                if top_data:
                    # Save movie data in chunks
                    num_chunks = (len(top_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
                    for i in range(num_chunks):
                        start_idx = i * CHUNK_SIZE
                        end_idx = min((i + 1) * CHUNK_SIZE, len(top_data))
                        chunk_df = pd.DataFrame(top_data[start_idx:end_idx])
                        chunk_df = chunk_df[['Title', 'Year', 'tmdbID']]
                        output_path = os.path.join(BASE_DIR, f'{continent.replace(" ", "_").lower()}_top_movies.csv')
                        chunk_df.to_csv(output_path, index=False, encoding='utf-8')

                    # Save statistics for this continent
                    stats_path = os.path.join(BASE_DIR, f'stats_{continent.replace(" ", "_").lower()}_top_movies.txt')
                    with open(stats_path, mode='w', encoding='utf-8') as file:
                        file.write(f"<strong>The Top {len(top_data)} Highest Rated Films from {'Australia' if continent == 'Oceania' else continent}</strong>\n\n")
                        file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
                        file.write("<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>\n\n")
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

                        # Write top 10 statistics for this continent
                        for category_name, counts in continent_stats[continent].items():
                            if category_name != 'film_data':
                                display_name = category_display_names.get(category_name, category_name.replace('_', ' '))
                                file.write(f"<strong>The ten most appearing {display_name}:</strong>\n")
                                for item, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                                    file.write(f"{item}: {count}\n")
                                file.write("\n")
                        file.write("<strong>If you notice any movies you believe should/should not be included just let me know!</strong>")

                    # Ensure we only save up to MAX_MOVIES_CONTINENT in the film data
                    continent_stats[continent]['film_data'] = continent_stats[continent]['film_data'][:MAX_MOVIES_CONTINENT]

    def save_results(self):
        """Save all results to files"""

        # Save unfiltered approved data (append mode)
        approved_path = os.path.join(BASE_DIR, 'unfiltered_approved.csv')
        with open(approved_path, mode='a', newline='', encoding='utf-8') as file:  # Change to 'a' for append
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_approved:
                writer.writerow(movie + ["2500 Top"])  # Append "2500 Top" to each row

        # Save unfiltered denied data (append mode)
        denied_path = os.path.join(BASE_DIR, 'unfiltered_denied.csv')
        with open(denied_path, mode='a', newline='', encoding='utf-8') as file:  # Change to 'a' for append
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_denied:
                writer.writerow(movie + ["2500 Top"])  # Append "2500 Top" to each row

        # Save MPAA results
        self.save_mpaa_results()

        # Save runtime results
        self.save_runtime_results()

        # Save continent results
        self.save_continent_results()

        # Save unknown continent films
        self.save_unknown_continent_films()

        # Save MAX_MOVIES_2500 results
        self.save_max_movies_2500_results()

    def save_mpaa_results(self):
        """Save results for each MPAA rating."""
        # Add this mapping at the beginning of the save_mpaa_results and save_runtime_results methods
        category_display_names = {
            'director_counts': 'directors',
            'actor_counts': 'actors',
            'decade_counts': 'decades',
            'genre_counts': 'genres',
            'studio_counts': 'studios',
            'language_counts': 'languages',
            'country_counts': 'countries'
        }

        for rating in MPAA_RATINGS:
            rating_data = mpaa_stats[rating]['film_data']
            if rating_data:
                # Determine the max limit based on the MPAA rating
                max_limit = (
                    MAX_MOVIES_G if rating == 'G' else
                    MAX_MOVIES_NC17 if rating == 'NC-17' else
                    MAX_MOVIES_MPAA
                )
                # Limit to top results
                top_data = rating_data[:int(max_limit)]  # Ensure it does not exceed the max
                # Save movie data in chunks
                num_chunks = (len(top_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
                for i in range(num_chunks):
                    start_idx = i * CHUNK_SIZE
                    end_idx = min((i + 1) * CHUNK_SIZE, len(top_data))
                    chunk_df = pd.DataFrame(top_data[start_idx:end_idx])
                    chunk_df = chunk_df[['Title', 'Year', 'tmdbID']]
                    output_path = os.path.join(BASE_DIR, f'{rating.upper()}_top_movies.csv')
                    chunk_df.to_csv(output_path, index=False, encoding='utf-8')

                def get_ordinal(n):
                    if 10 <= n % 100 <= 20:
                        suffix = 'th'
                    else:
                        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
                    return str(n) + suffix

                current_date = datetime.now()
                formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"

                # Save statistics for this rating
                stats_path = os.path.join(BASE_DIR, f'stats_{rating.upper()}_top_movies.txt')
                with open(stats_path, mode='w', encoding='utf-8') as file:
                    file.write(f"<strong>The Top {len(top_data)} Highest Rated {rating} Rated Movies On Letterboxd</strong>\n\n")
                    file.write("<strong>Rating defined by MPAA. Films released before November 1, 1968 are not eligible as they predate the current MPAA rating system. (Unless there was a subsequent re-rating.)</strong>\n\n")
                    file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
                    file.write("<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>\n\n")
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
                    for category_name, counts in mpaa_stats[rating].items():
                        if category_name != 'film_data':
                            # Use the mapping for display names
                            display_name = category_display_names.get(category_name, category_name.replace('_', ' '))
                            file.write(f"<strong>The ten most appearing {display_name}:</strong>\n")
                            for item, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                                file.write(f"{item}: {count}\n")
                            file.write("\n")
                    file.write("<strong>If you notice any movies you believe should/should not be included just let me know!</strong>")


    def save_runtime_results(self):
        """Save results for each runtime category."""
        # Add this mapping at the beginning of the save_mpaa_results and save_runtime_results methods
        category_display_names = {
            'director_counts': 'directors',
            'actor_counts': 'actors',
            'decade_counts': 'decades',
            'genre_counts': 'genres',
            'studio_counts': 'studios',
            'language_counts': 'languages',
            'country_counts': 'countries'
        }

        for category in RUNTIME_CATEGORIES.keys():
            category_data = runtime_stats[category]['film_data']
            if category_data:
                # Determine the max limit based on the category
                max_limit = (
                    MAX_180 if category == '180_Minutes_or_Greater' else
                    MAX_240 if category == '240_Minutes_or_Greater' else
                    MAX_MOVIES_RUNTIME
                )
                # Limit to top results
                top_data = category_data[:int(max_limit)]  # Ensure it does not exceed the max
                # Save movie data in chunks
                num_chunks = (len(top_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
                for i in range(num_chunks):
                    start_idx = i * CHUNK_SIZE
                    end_idx = min((i + 1) * CHUNK_SIZE, len(top_data))
                    chunk_df = pd.DataFrame(top_data[start_idx:end_idx])
                    chunk_df = chunk_df[['Title', 'Year', 'tmdbID']]
                    output_path = os.path.join(BASE_DIR, f'{category}_top_movies.csv')
                    chunk_df.to_csv(output_path, index=False, encoding='utf-8')

                def get_ordinal(n):
                    if 10 <= n % 100 <= 20:
                        suffix = 'th'
                    else:
                        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
                    return str(n) + suffix

                current_date = datetime.now()
                formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"

                # Save statistics for this category
                stats_path = os.path.join(BASE_DIR, f'stats_{category}_top_movies.txt')
                with open(stats_path, mode='w', encoding='utf-8') as file:
                    file.write(f"<strong>The Top {len(top_data)} Highest Rated Films With a Runtime of {category.replace('_', ' ')}.</strong>\n\n")
                    file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
                    file.write("<a href=https://letterboxd.com/bigbadraj/list/the-official-list-index/> Check out more of the lists I update regularly! </a>\n\n")
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
                    
                    # Ensure to limit to top 10 for each category
                    for category_name, counts in runtime_stats[category].items():
                        if category_name != 'film_data':
                            # Use the mapping for display names
                            display_name = category_display_names.get(category_name, category_name.replace('_', ' '))
                            file.write(f"<strong>The ten most appearing {display_name}:</strong>\n")
                            for item, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]:  # Limit to top 10
                                file.write(f"{item}: {count}\n")
                            file.write("\n")
                    file.write("<strong>If you notice any movies you believe should/should not be included just let me know!</strong>")

    def save_unknown_continent_films(self):
        """Save films from unknown continents to a CSV file."""
        if self.unknown_continent_films:
            unknown_df = pd.DataFrame(self.unknown_continent_films)
            output_path = os.path.join(BASE_DIR, 'unknown_continent_films.csv')
            unknown_df.to_csv(output_path, index=False, encoding='utf-8')
            print_to_csv(f"Saved {len(self.unknown_continent_films)} films from unknown continents to {output_path}.")

    def log_error_to_csv(self, error_message: str):
        """Log error messages to update_results.csv."""
        error_path = os.path.join(BASE_DIR, 'update_results.csv')
        with open(error_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Error Type', 'Error Message'])
            writer.writerow([type(error_message).__name__, error_message])  # Write the error type and message

    
def main():
    start_time = time.time()
    try:
        scraper = LetterboxdScraper()
        scraper.scrape_movies()
        scraper.save_results()

        # Format final statistics
        print_to_csv(f"\n{'Final Statistics':=^100}")
        print_to_csv(f"{'Total movies scraped:':<30} {scraper.total_titles:>10}")
        print_to_csv(f"{'Total accepted:':<30} {scraper.valid_movies_count:>10}")
        print_to_csv(f"{'Total rejected:':<30} {len(scraper.processor.rejected_data):>10}")
        print_to_csv(f"{'Total unfiltered approved:':<30} {len(scraper.processor.unfiltered_approved):>10}")
        print_to_csv(f"{'Total unfiltered denied:':<30} {len(scraper.processor.unfiltered_denied):>10}")

        # Format execution time
        execution_time = time.time() - start_time
        print_to_csv(f"\n{'Execution Summary':=^100}")
        print_to_csv(f"Total execution time: {format_time(execution_time)}")
        print_to_csv(f"Average processing speed: {scraper.valid_movies_count / execution_time:.2f} movies/second")

    except Exception as e:
        print_to_csv(f"\n{'Error':=^100}")
        print_to_csv(f"❌ An error occurred during execution: {e}")
    finally:
        if 'scraper' in locals():
            try:
                scraper.driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()