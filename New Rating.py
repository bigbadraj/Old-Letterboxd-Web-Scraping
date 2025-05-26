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
MAX_MOVIES = 70 # Currently using 7000
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
MAX_180 = 150
MAX_240 = 25
MAX_MOVIES_G = 100
MAX_MOVIES_NC17 = 20
MAX_MOVIES_AFRICA = 100
MAX_MOVIES_OCEANIA = 75
MAX_MOVIES_SOUTH_AMERICA = 250

# File paths
BASE_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs'
LIST_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping'
BLACKLIST_PATH = os.path.join(LIST_DIR, 'blacklist.xlsx')
WHITELIST_PATH = os.path.join(LIST_DIR, 'whitelist.xlsx')
INCOMPLETE_STATS_WHITELIST_PATH = os.path.join(LIST_DIR, 'Incomplete_Stats_Whitelist.xlsx')

# TMDb API key
TMDB_API_KEY = 'KEY HERE'

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
        self.whitelist = None
        self.whitelist_lookup = {}
        self.incomplete_stats_whitelist = None
        self.incomplete_stats_lookup = {}
        self.load_whitelist()
        self.load_incomplete_stats_whitelist()
        
        self.blacklist = pd.read_excel(BLACKLIST_PATH, header=0, names=['Title', 'Year'], usecols=[0, 1])
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
        self.mpaa_counts: Dict[str, int] = {}

        # Normalize titles and years in blacklist
        self.blacklist['Title'] = self.blacklist['Title'].apply(normalize_text)
        self.blacklist['Year'] = self.blacklist['Year'].astype(str).str.strip()

    def load_whitelist(self):
        """Load and initialize the whitelist data."""
        try:
            # Read whitelist with explicit string type for Year column and include Information column
            self.whitelist = pd.read_excel(WHITELIST_PATH, header=0, names=['Title', 'Year', 'Information'], dtype={'Year': str})
            
            # Normalize the data
            self.whitelist['Title'] = self.whitelist['Title'].apply(normalize_text)
            self.whitelist['Year'] = self.whitelist['Year'].astype(str).str.strip()
            
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
                    self.whitelist_lookup[key] = (info, idx)
                except (json.JSONDecodeError, TypeError):
                    # If there's any error parsing, treat it as an empty dictionary
                    info = {}
                    self.whitelist_lookup[key] = (info, idx)
                    continue
                
        except FileNotFoundError:
            print_to_csv("whitelist.xlsx not found. Creating new file.")
            self.whitelist = pd.DataFrame(columns=['Title', 'Year', 'Information'])
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

        # Process runtime category if we have runtime info
        runtime = info.get('Runtime')
        if runtime:
            categories = []
            if runtime < 91:
                categories.append('90_Minutes_or_Less')
            if runtime < 121:
                categories.append('120_Minutes_or_Less')
            if runtime > 179:
                categories.append('180_Minutes_or_Greater')
            if runtime > 239:
                categories.append('240_Minutes_or_Greater')
            
            for category in categories:
                if add_to_runtime_stats(category, info.get('Title'), info.get('Year'), info.get('tmdbID')):
                    self.update_runtime_statistics(info.get('Title'), info.get('Year'), info.get('tmdbID'), None, category)

        # Process MPAA rating if we have it
        mpaa_rating = info.get('MPAA')
        if mpaa_rating and mpaa_rating in MPAA_RATINGS:
            if add_to_mpaa_stats(mpaa_rating, info.get('Title'), info.get('Year'), info.get('tmdbID')):
                self.update_statistics(mpaa_rating)

        # Process continent data if we have countries
        countries = info.get('Countries', [])
        for country in countries:
            for continent, country_list in CONTINENTS_COUNTRIES.items():
                if country in country_list:
                    if add_to_continent_stats(continent, info.get('Title'), info.get('Year'), info.get('tmdbID')):
                        self.update_continent_statistics(continent)
                    break

        # Process MAX_MOVIES_2500 using centralized function
        if add_to_max_movies_2500(info.get('Title'), info.get('Year'), info.get('tmdbID')):
            self.update_max_movies_2500_statistics(info.get('Title'), info.get('Year'), info.get('tmdbID'))

    def update_whitelist(self, film_title: str, release_year: str, movie_data: Dict) -> bool:
        """Update the whitelist with new movie data."""
        try:
            key = f"{film_title.lower()}_{release_year}"
            
            if key in self.whitelist_lookup:
                # Update existing entry
                _, row_idx = self.whitelist_lookup[key]
                self.whitelist.at[row_idx, 'Information'] = json.dumps(movie_data)
                self.whitelist_lookup[key] = (movie_data, row_idx)
            else:
                # Add new entry
                new_row = pd.DataFrame([{
                    'Title': film_title,
                    'Year': release_year,
                    'Information': json.dumps(movie_data)
                }])
                self.whitelist = pd.concat([self.whitelist, new_row], ignore_index=True)
                self.whitelist_lookup[key] = (movie_data, len(self.whitelist) - 1)
            
            # Save to Excel
            self.whitelist.to_excel(WHITELIST_PATH, index=False)
            self.load_whitelist()
            return True
            
        except Exception as e:
            print_to_csv(f"Error updating whitelist: {str(e)}")
            return False

    def get_whitelist_data(self, film_title: str, release_year: str = None, film_url: str = None) -> Optional[Tuple[Dict, int]]:
        """Get the whitelist data for a movie if it exists."""
        
        # Try title-only match first
        matches = []
        for key, (info, row_idx) in self.whitelist_lookup.items():
            title = key.split('_')[0]
            if title == film_title.lower():
                matches.append((info, row_idx))

        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            # If we have multiple matches and a film_url, scrape the release year
            if film_url:
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
                            return self.whitelist_lookup[key]
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

    def add_to_blacklist(self, film_title: str, release_year: str, reason: str) -> None:
        if not any((film_title.lower() == str(row['Title']).lower() and 
                   release_year == row['Year']) for _, row in self.blacklist.iterrows()):
            # Create a new row as a DataFrame
            new_row = pd.DataFrame([[film_title, release_year, reason]], 
                                 columns=['Title', 'Year', 'Reason'])
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

    def process_runtime_category(self, film_title: str, release_year: str, tmdb_id: str, runtime: int, driver=None):
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

        # Get the movie info from whitelist
        movie_info, _ = self.get_whitelist_data(film_title, release_year)
        if not movie_info:
            return

        # Update directors
        for director in movie_info.get('Directors', []):
            runtime_stats[category]['director_counts'][director] += 1

        # Update actors
        for actor in movie_info.get('Actors', []):
            runtime_stats[category]['actor_counts'][actor] += 1

        # Update decade
        decade = movie_info.get('Decade')
        if decade:
            runtime_stats[category]['decade_counts'][decade] += 1

        # Update genres
        for genre in movie_info.get('Genres', []):
            runtime_stats[category]['genre_counts'][genre] += 1

        # Update studios
        for studio in movie_info.get('Studios', []):
            runtime_stats[category]['studio_counts'][studio] += 1

        # Update languages
        for language in movie_info.get('Languages', []):
            runtime_stats[category]['language_counts'][language] += 1

        # Update countries
        for country in movie_info.get('Countries', []):
            runtime_stats[category]['country_counts'][country] += 1

    def update_statistics(self, mpaa_rating: str):
        """Update statistics for the given MPAA rating."""
        # Get the most recently added movie from the MPAA rating's film_data
        if not mpaa_stats[mpaa_rating]['film_data']:
            return
            
        latest_movie = mpaa_stats[mpaa_rating]['film_data'][-1]
        film_title = latest_movie['Title']
        release_year = latest_movie['Year']
        
        # Get the movie info from whitelist
        movie_info, _ = self.get_whitelist_data(film_title, release_year)
        if not movie_info:
            return

        # Update directors
        for director in movie_info.get('Directors', []):
            mpaa_stats[mpaa_rating]['director_counts'][director] += 1

        # Update actors
        for actor in movie_info.get('Actors', []):
            mpaa_stats[mpaa_rating]['actor_counts'][actor] += 1

        # Update decade
        decade = movie_info.get('Decade')
        if decade:
            mpaa_stats[mpaa_rating]['decade_counts'][decade] += 1

        # Update genres
        for genre in movie_info.get('Genres', []):
            mpaa_stats[mpaa_rating]['genre_counts'][genre] += 1

        # Update studios
        for studio in movie_info.get('Studios', []):
            mpaa_stats[mpaa_rating]['studio_counts'][studio] += 1

        # Update languages
        for language in movie_info.get('Languages', []):
            mpaa_stats[mpaa_rating]['language_counts'][language] += 1

        # Update countries
        for country in movie_info.get('Countries', []):
            mpaa_stats[mpaa_rating]['country_counts'][country] += 1

    def update_continent_statistics(self, continent: str):
        """Update statistics for the given continent."""
        # Get the most recently added movie from the continent's film_data
        if not continent_stats[continent]['film_data']:
            return
            
        latest_movie = continent_stats[continent]['film_data'][-1]
        film_title = latest_movie['Title']
        release_year = latest_movie['Year']
        
        # Get the movie info from whitelist
        movie_info, _ = self.get_whitelist_data(film_title, release_year)
        if not movie_info:
            return

        # Update directors
        for director in movie_info.get('Directors', []):
            continent_stats[continent]['director_counts'][director] += 1

        # Update actors
        for actor in movie_info.get('Actors', []):
            continent_stats[continent]['actor_counts'][actor] += 1

        # Update decade
        decade = movie_info.get('Decade')
        if decade:
            continent_stats[continent]['decade_counts'][decade] += 1

        # Update genres
        for genre in movie_info.get('Genres', []):
            continent_stats[continent]['genre_counts'][genre] += 1

        # Update studios
        for studio in movie_info.get('Studios', []):
            continent_stats[continent]['studio_counts'][studio] += 1

        # Update languages
        for language in movie_info.get('Languages', []):
            continent_stats[continent]['language_counts'][language] += 1

        # Update countries
        for country in movie_info.get('Countries', []):
            continent_stats[continent]['country_counts'][country] += 1

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

    def is_blacklisted(self, film_title: str, release_year: str = None) -> bool:
        """Check if a movie is in the blacklist using a lookup dictionary."""
        # Normalize the title for comparison
        normalized_title = normalize_text(film_title).lower()
        
        # If we have a year, try exact match first
        if release_year:
            normalized_year = str(release_year).strip()
            for _, row in self.blacklist.iterrows():
                if (normalize_text(row['Title']).lower() == normalized_title and 
                    str(row['Year']).strip() == normalized_year):
                    return True
        
        # If no year or no exact match, try title-only match
        for _, row in self.blacklist.iterrows():
            if normalize_text(row['Title']).lower() == normalized_title:
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

def add_to_continent_stats(continent: str, film_title: str, release_year: str, tmdb_id: str) -> bool:
    """
    Centralized function to add a movie to continent_stats if it's not already present.
    Returns True if the movie was added, False if it was already present or if we've reached the limit.
    """
    # Check if movie already exists
    if any(movie['Title'] == film_title and movie['Year'] == release_year 
           for movie in continent_stats[continent]['film_data']):
        return False
        
    # Determine the max limit based on the continent
    max_limit = (
        MAX_MOVIES_AFRICA if continent == 'Africa' else
        MAX_MOVIES_OCEANIA if continent == 'Oceania' else
        MAX_MOVIES_SOUTH_AMERICA if continent == 'South America' else
        MAX_MOVIES_CONTINENT
    )
    
    # Check if we've reached the limit
    if len(continent_stats[continent]['film_data']) >= max_limit:
        return False
        
    # Add the movie
    continent_stats[continent]['film_data'].append({
        'Title': film_title,
        'Year': release_year,
        'tmdbID': tmdb_id
    })
    return True

def add_to_runtime_stats(category: str, film_title: str, release_year: str, tmdb_id: str) -> bool:
    """
    Centralized function to add a movie to runtime_stats if it's not already present.
    Returns True if the movie was added, False if it was already present or if we've reached the limit.
    """
    # Check if movie already exists
    if any(movie['Title'] == film_title and movie['Year'] == release_year 
           for movie in runtime_stats[category]['film_data']):
        return False
        
    # Determine the max limit based on the category
    max_limit = (
        MAX_180 if category == '180_Minutes_or_Greater' else
        MAX_240 if category == '240_Minutes_or_Greater' else
        MAX_MOVIES_RUNTIME
    )
    
    # Check if we've reached the limit
    if len(runtime_stats[category]['film_data']) >= max_limit:
        return False
        
    # Add the movie
    runtime_stats[category]['film_data'].append({
        'Title': film_title,
        'Year': release_year,
        'tmdbID': tmdb_id
    })
    return True

def add_to_mpaa_stats(rating: str, film_title: str, release_year: str, tmdb_id: str) -> bool:
    """
    Centralized function to add a movie to mpaa_stats if it's not already present.
    Returns True if the movie was added, False if it was already present or if we've reached the limit.
    """
    # Check if movie already exists
    if any(movie['Title'] == film_title and movie['Year'] == release_year 
           for movie in mpaa_stats[rating]['film_data']):
        return False
        
    # Determine the max limit based on the rating
    max_limit = (
        MAX_MOVIES_G if rating == 'G' else
        MAX_MOVIES_NC17 if rating == 'NC-17' else
        MAX_MOVIES_MPAA
    )
    
    # Check if we've reached the limit
    if len(mpaa_stats[rating]['film_data']) >= max_limit:
        return False
        
    # Add the movie
    mpaa_stats[rating]['film_data'].append({
        'Title': film_title,
        'Year': release_year,
        'tmdbID': tmdb_id
    })
    return True

class LetterboxdScraper:
    def __init__(self):
        self.driver = setup_webdriver()
        self.processor = MovieProcessor()
        self.base_url = 'https://letterboxd.com/films/by/rating/'
        self.total_titles = 0
        self.processed_titles = 0
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()
        self.unknown_continent_films = []  # Initialize the list for unknown continent films
        self.top_movies_count = 0  # Track the number of movies added to the top 2500 list
        print_to_csv("Initialized Letterboxd Scraper.")

    def process_movie_data(self, info, film_title=None, film_url=None):
        """Process movie data from the whitelist."""
        try:            
            # If we have a complete info dict, process it directly without loading the page
            if isinstance(info, dict):
                film_title = info.get('Title')
                release_year = info.get('Year')
                
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
                        self.processor.update_whitelist(film_title, release_year, {})
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
                    self.processor.update_whitelist(film_title, release_year, {})
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
                        movie_data = self.update_statistics_for_movie(film_title, release_year, info.get('tmdbID'), self.driver)
                        if movie_data:
                            # Update whitelist with fresh data
                            if self.processor.update_whitelist(film_title, release_year, movie_data):
                                # Process through output channels
                                self.processor.process_whitelist_info(movie_data)
                                
                                # Process runtime category if we have runtime info
                                runtime = movie_data.get('Runtime')
                                if runtime:
                                    categories = []
                                    if runtime < 91:
                                        categories.append('90_Minutes_or_Less')
                                    if runtime < 121:
                                        categories.append('120_Minutes_or_Less')
                                    if runtime > 179:
                                        categories.append('180_Minutes_or_Greater')
                                    if runtime > 239:
                                        categories.append('240_Minutes_or_Greater')
                                    
                                    for category in categories:
                                        if add_to_runtime_stats(category, film_title, release_year, movie_data.get('tmdbID')):
                                            self.processor.update_runtime_statistics(film_title, release_year, movie_data.get('tmdbID'), self.driver, category)
                                
                                # Process MPAA rating if we have it
                                mpaa_rating = movie_data.get('MPAA')
                                if mpaa_rating and mpaa_rating in MPAA_RATINGS:
                                    if add_to_mpaa_stats(mpaa_rating, film_title, release_year, movie_data.get('tmdbID')):
                                        self.processor.update_statistics(mpaa_rating)
                                
                                # Process continent data if we have countries
                                countries = movie_data.get('Countries', [])
                                for country in countries:
                                    for continent, country_list in CONTINENTS_COUNTRIES.items():
                                        if country in country_list:
                                            if add_to_continent_stats(continent, film_title, release_year, movie_data.get('tmdbID')):
                                                self.processor.update_continent_statistics(continent)
                                            break
                                
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
                    self.driver.get(film_url)
                    # Wait for page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                    )
                    time.sleep(random.uniform(1.0, 1.5))
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
                    else:
                        print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                except Exception as e:
                    print_to_csv(f"Error extracting rating count: {str(e)}")

                # If movie is in whitelist but has insufficient ratings, reject it
                if whitelist_info and rating_count < MIN_RATING_COUNT:
                    print_to_csv(f"❌ {film_title} ({release_year}) was not added due to insufficient ratings: {rating_count} ratings.")
                    self.processor.rejected_data.append([film_title, release_year, None, 'Insufficient ratings (< 1000)'])
                    return

                # If movie is in whitelist and has sufficient ratings, process it
                if whitelist_info:
                    self.process_movie_data(whitelist_info)  # Process the whitelist data
                    return

                # For non-whitelisted movies, continue with normal checks
                if rating_count < MIN_RATING_COUNT:
                    print_to_csv(f"❌ {film_title} ({release_year}) was not added due to insufficient ratings: {rating_count} ratings.")
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
                self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver)
                
                # Extract runtime
                runtime = None
                try:
                    runtime_element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                    )
                    runtime_text = runtime_element.text
                    match = re.search(r'(\d+)\s*mins', runtime_text)
                    if match:
                        runtime = int(match.group(1))
                except Exception as e:
                    print_to_csv(f"Error extracting runtime: {str(e)}")
                    runtime = None

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
                except Exception:
                    pass

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
                
                # Process runtime category if we have runtime info
                runtime = info.get('Runtime')
                if runtime:
                    categories = []
                    if runtime < 91:
                        categories.append('90_Minutes_or_Less')
                    if runtime < 121:
                        categories.append('120_Minutes_or_Less')
                    if runtime > 179:
                        categories.append('180_Minutes_or_Greater')
                    if runtime > 239:
                        categories.append('240_Minutes_or_Greater')
                    
                    for category in categories:
                        if add_to_runtime_stats(category, film_title, release_year, tmdb_id):
                            self.processor.update_runtime_statistics(film_title, release_year, tmdb_id, self.driver, category)
                
                # Process MPAA rating if we have it
                mpaa_rating = info.get('MPAA')
                if mpaa_rating and mpaa_rating in MPAA_RATINGS:
                    if add_to_mpaa_stats(mpaa_rating, film_title, release_year, tmdb_id):
                        self.processor.update_statistics(mpaa_rating)
                
                # Process continent data if we have countries
                countries = info.get('Countries', [])
                for country in countries:
                    for continent, country_list in CONTINENTS_COUNTRIES.items():
                        if country in country_list:
                            if add_to_continent_stats(continent, film_title, release_year, tmdb_id):
                                self.processor.update_continent_statistics(continent)
                            break
                
                # Process MAX_MOVIES_2500 using centralized function
                if add_to_max_movies_2500(film_title, release_year, tmdb_id):
                    self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)
                
                # Add to unfiltered_approved if not already in whitelist
                if not self.processor.is_whitelisted(film_title, release_year):
                    if not any(film_title.lower() == movie[0].lower() and release_year == movie[1] for movie in self.processor.unfiltered_approved):
                        self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id])
                
                return True
            
        except Exception as e:
            print_to_csv(f"Error processing movie data: {str(e)}")
            print_to_csv(f"Error type: {type(e)}")
            print_to_csv(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            # Don't raise the exception, just continue

    def scrape_movies(self):
        seen_titles = set()  # <-- Add this at the start of the method

        try:
            while self.valid_movies_count < MAX_MOVIES:
                # Construct the URL for the current page
                url = f'{self.base_url}page/{self.page_number}/'
                print_to_csv(f"\nLoading page {self.page_number}: {url}")
                
                # Send a GET request to the URL with retry mechanism
                max_retries = 20
                for retry in range(max_retries):
                    try:
                        self.driver.get(url)
                        # Wait for the page to load
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.react-component.poster'))
                        )
                        break
                    except Exception as e:
                        if retry == max_retries - 1:
                            raise e
                        print_to_csv(f"Retry {retry + 1}/{max_retries} loading page {self.page_number}")
                        time.sleep(2)
                
                time.sleep(random.uniform(1.0, 1.5))
                        
                # Find all film containers with retry mechanism
                film_containers = []
                max_retries = 25  # Maximum number of retries
                for retry in range(max_retries):
                    try:
                        film_containers = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.react-component.poster'))
                        )
                        if len(film_containers) == 72:  # Check for exactly 72 containers
                            break
                        else:
                            print_to_csv(f"Found only {len(film_containers)} containers, retrying... (Attempt {retry + 1}/{max_retries})")
                            time.sleep(5)  # Wait longer between retries
                            self.driver.refresh()  # Refresh the page
                            time.sleep(2)  # Wait for refresh
                    except Exception as e:
                        if retry == max_retries - 1:
                            print_to_csv(f"Failed to find all 72 film containers after {max_retries} attempts. Ending program.")
                            self.driver.quit()
                            return
                        print_to_csv(f"Retry {retry + 1}/{max_retries} finding film containers")
                        time.sleep(5)
                        self.driver.refresh()
                        time.sleep(2)
                
                if len(film_containers) != 72:
                    print_to_csv(f"Failed to find all 72 film containers after {max_retries} attempts. Ending program.")
                    self.driver.quit()
                    return

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
                            
                            # Check blacklist using just the title and year from the title
                            is_blacklisted = self.processor.is_blacklisted(film_title, release_year)
                            
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
                    print_to_csv("No valid film data collected. Retrying...")
                    time.sleep(5)
                    continue

                # Now process each film one by one
                for film_data in film_data_list:
                    if self.valid_movies_count >= MAX_MOVIES:
                        print_to_csv(f"\nReached the target of {MAX_MOVIES} successful movies. Stopping scraping.")
                        return

                    film_title = film_data['title']
                    film_url = film_data['url']
                    release_year = film_data['release_year']

                    # --- BEGIN CHANGE ---
                    # If we've seen this title before, require title+year match
                    if film_title.lower() in seen_titles:
                        whitelist_info, _ = self.processor.get_whitelist_data(film_title, release_year, film_url)
                    else:
                        whitelist_info, _ = self.processor.get_whitelist_data(film_title, film_url=film_url)
                    # --- END CHANGE ---

                    # After processing, add the title to seen_titles
                    seen_titles.add(film_title.lower())

                    # Increment total_titles for each movie we process, including blacklisted ones
                    self.total_titles += 1
                    
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
                    self.driver.get(film_url)
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                    )
                    
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

                    # Check 1: Rating count minimum
                    if rating_count < MIN_RATING_COUNT:
                        print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                        self.processor.rejected_data.append([film_title, release_year, None, 'Insufficient ratings (< 1000)'])
                        continue
                    
                    # Check 2: Blacklist
                    if self.processor.is_blacklisted(film_title, release_year):
                        print_to_csv(f"❌ {film_title} was not added due to being blacklisted.")
                        self.processor.rejected_data.append([film_title, release_year, None, 'Blacklisted'])
                        continue
                    
                    # Check 3: Runtime
                    try:
                        self.driver.get(film_url)
                        # Wait for page to load
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                        )
                        time.sleep(random.uniform(1.0, 1.5))

                        runtime_element = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                        )
                        runtime_text = runtime_element.text
                        match = re.search(r'(\d+)\s*mins', runtime_text)
                        if match:
                            runtime = int(match.group(1))
                        else:
                            runtime = None
                    except Exception as e:
                        runtime = None
                        print_to_csv(f"Error extracting runtime for {film_title}: {str(e)}")

                    if runtime is None:
                        print_to_csv(f"⚠️ {film_title} skipped due to missing runtime")
                        continue
                        
                    if runtime < MIN_RUNTIME:
                        print_to_csv(f"❌ {film_title} was not added due to a short runtime of {runtime} minutes.")
                        self.processor.rejected_data.append([film_title, release_year, None, f'Short runtime of {runtime} minutes'])
                        self.processor.add_to_blacklist(film_title, release_year, f'Short runtime of {runtime} minutes')
                        continue
                    
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
                        self.processor.unfiltered_denied.append([film_title, release_year, None, 'Missing TMDB ID'])
                        continue
                    
                    # Check 5: Keywords and Genres
                    keywords, genres = self.processor.fetch_tmdb_details(tmdb_id)
                    
                    # Check keywords
                    matching_keywords = [k for k in FILTER_KEYWORDS if k in keywords]
                    if matching_keywords:
                        rejection_reason = f"due to being a {', '.join(matching_keywords)}."
                        print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                        self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                        continue
                    
                    # Check genres
                    matching_genres = [g for g in FILTER_GENRES if g in genres]
                    if matching_genres:
                        rejection_reason = f"due to being a {', '.join(matching_genres)}."
                        print_to_csv(f"❌ {film_title} was not added {rejection_reason}")
                        self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                        self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                        continue
                    
                    # Now do the full scrape and process the movie
                    self.driver.get(film_url)
                    # Wait for page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:title"]'))
                    )
                    time.sleep(random.uniform(1.0, 1.5))
                    # Extract all movie data using Selenium
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
                            self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id])
                            self.valid_movies_count += 1  # Increment the count since it's an approved movie
                            print_to_csv(f"✅ Successfully approved {film_title} ({self.valid_movies_count}/{MAX_MOVIES})")
                            
                            # Process runtime category
                            if runtime:
                                self.processor.process_runtime_category(film_title, release_year, tmdb_id, runtime, self.driver)
                            
                            # Process MPAA rating
                            mpaa_rating = extract_mpaa_rating(self.driver)
                            if mpaa_rating and mpaa_rating in MPAA_RATINGS:
                                if add_to_mpaa_stats(mpaa_rating, film_title, release_year, tmdb_id):
                                    self.processor.update_statistics(mpaa_rating)
                            
                            # Process continent data
                            for country in movie_data['Countries']:
                                for continent, country_list in CONTINENTS_COUNTRIES.items():
                                    if country in country_list:
                                        if add_to_continent_stats(continent, film_title, release_year, tmdb_id):
                                            self.processor.update_continent_statistics(continent)
                                        break
                            
                            # Process MAX_MOVIES_2500
                            if add_to_max_movies_2500(film_title, release_year, tmdb_id):
                                self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)

                    # Update statistics
                    self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver)

                self.page_number += 1
                time.sleep(random.uniform(1.0, 1.5))

        except Exception as e:
            print_to_csv(f"Error in scrape_movies: {str(e)}")
        finally:
            self.driver.quit()

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
            self.valid_movies_count += 1
            print_to_csv(f"✅ Successfully processed {film_title} ({self.valid_movies_count}/{MAX_MOVIES})")

        # Use centralized function for MAX_MOVIES_2500
        if add_to_max_movies_2500(film_title, release_year, tmdb_id):
            self.processor.update_max_movies_2500_statistics(film_title, release_year, tmdb_id)

        # Extract runtime using Selenium
        runtime = None
        try:
            runtime_text = self.driver.find_element(By.CSS_SELECTOR, 'p.text-link.text-footer').text
            match = re.search(r'(\d+)\s*mins', runtime_text)
            if match:
                runtime = int(match.group(1))
        except Exception:
            runtime = None

        self.processor.process_runtime_category(film_title, release_year, tmdb_id, runtime, self.driver)
        self.update_statistics_for_movie(film_title, release_year, tmdb_id, self.driver)

        # Only add to unfiltered_approved if the movie is not in the whitelist
        if not self.processor.is_whitelisted(film_title, release_year):
            if not any(film_title.lower() == movie[0].lower() and release_year == movie[1] for movie in self.processor.unfiltered_approved):
                self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id])

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
        
        # Track movies by title
        title_to_movies = defaultdict(list)
        
        # Add approved movies
        for movie in self.processor.film_data:
            # Only add if not already in the list with the same tmdbID
            if not any(m['tmdb_id'] == movie['tmdbID'] for m in title_to_movies[movie['Title'].lower()]):
                title_to_movies[movie['Title'].lower()].append({
                    'title': movie['Title'],
                    'year': movie['Year'],
                    'tmdb_id': movie['tmdbID'],
                    'status': 'Approved'
                })
        
        # Add rejected movies
        for movie in self.processor.rejected_data:
            if len(movie) >= 3:  # Ensure we have at least title, year, and reason
                # Only add if not already in the list with the same tmdbID
                if not any(m['tmdb_id'] == movie[2] for m in title_to_movies[movie[0].lower()]):
                    title_to_movies[movie[0].lower()].append({
                        'title': movie[0],
                        'year': movie[1],
                        'tmdb_id': movie[2] if len(movie) > 2 else 'N/A',
                        'status': f'Rejected: {movie[3]}' if len(movie) > 3 else 'Rejected'
                    })

        # Save unfiltered approved data (append mode)
        approved_path = os.path.join(BASE_DIR, 'unfiltered_approved.csv')
        with open(approved_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_approved:
                writer.writerow(movie + ["2500 Top"])

        # Save unfiltered denied data (append mode)
        denied_path = os.path.join(BASE_DIR, 'unfiltered_denied.csv')
        with open(denied_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_denied:
                writer.writerow(movie + ["2500 Top"])

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

    def update_statistics_for_movie(self, film_title: str, release_year: str, tmdb_id: str, driver):
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

            # Extract MPAA rating
            mpaa_rating = None
            try:
                mpaa_rating = extract_mpaa_rating(self.driver)
            except Exception as e:
                print_to_csv(f"Error extracting MPAA rating: {str(e)}")

            # Extract runtime
            runtime = None
            try:
                runtime_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'p.text-link.text-footer'))
                )
                runtime_text = runtime_element.text
                match = re.search(r'(\d+)\s*mins', runtime_text)
                if match:
                    runtime = int(match.group(1))
            except Exception as e:
                print_to_csv(f"Error extracting runtime: {str(e)}")
                runtime = None

            # Extract rating count
            rating_count = 0
            try:
                page_source = self.driver.page_source
                match = re.search(r'ratingCount":(\d+)', page_source)
                if match:
                    rating_count = int(match.group(1))
                else:
                    print_to_csv(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
            except Exception as e:
                print_to_csv(f"Error extracting rating count: {str(e)}")

            if rating_count < MIN_RATING_COUNT:
                print_to_csv(f"❌ {film_title} ({release_year}) was not added due to insufficient ratings: {rating_count} ratings.")
                return

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

            # Only update whitelist if the movie is already in it
            if self.processor.is_whitelisted(film_title, release_year):
                if self.processor.update_whitelist(film_title, release_year, movie_data):
                    print_to_csv(f"📝 Successfully updated whitelist data for {film_title}")
                    # Process through all output channels
                    self.processor.process_whitelist_info(movie_data)
                    
                    # Process runtime category if we have runtime info
                    if runtime:
                        categories = []
                        if runtime < 91:
                            categories.append('90_Minutes_or_Less')
                        if runtime < 121:
                            categories.append('120_Minutes_or_Less')
                        if runtime > 179:
                            categories.append('180_Minutes_or_Greater')
                        if runtime > 239:
                            categories.append('240_Minutes_or_Greater')
                        
                        for category in categories:
                            if add_to_runtime_stats(category, film_title, release_year, tmdb_id):
                                self.processor.update_runtime_statistics(film_title, release_year, tmdb_id, driver, category)
                    
                    # Process MPAA rating if we have it
                    if mpaa_rating and mpaa_rating in MPAA_RATINGS:
                        if add_to_mpaa_stats(mpaa_rating, film_title, release_year, tmdb_id):
                            self.processor.update_statistics(mpaa_rating)
                    
                    # Process continent data if we have countries
                    for country in movie_countries:
                        for continent, country_list in CONTINENTS_COUNTRIES.items():
                            if country in country_list:
                                if add_to_continent_stats(continent, film_title, release_year, tmdb_id):
                                    self.processor.update_continent_statistics(continent)
                                break
                    
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