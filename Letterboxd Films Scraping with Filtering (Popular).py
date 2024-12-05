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
from tqdm import tqdm

# Configure locale and constants
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
MAX_MOVIES = 2500
MIN_RATING_COUNT = 1000
MIN_RUNTIME = 40
MAX_RETRIES = 5
RETRY_DELAY = 15
CHUNK_SIZE = 1900

# File paths
BASE_DIR = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs'
BLACKLIST_PATH = os.path.join(BASE_DIR, 'blacklist.csv')
WHITELIST_PATH = os.path.join(BASE_DIR, 'whitelist.csv')

# TMDb API key
TMDB_API_KEY = 'Your API Key Here'

# Filtering criteria
FILTER_KEYWORDS = {
    'concert film', 'miniseries',
    'live performance', 'filmed theater', 'live theater', 
    'stand-up comedy', 'edited from tv series'
}

FILTER_GENRES = {'Documentary'}

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

class MovieProcessor:
    def __init__(self):
        self.session = RequestsSession()
        self.whitelist = pd.read_csv(WHITELIST_PATH, header=None, names=['Title', 'Year'], encoding='utf-8')
        self.blacklist = pd.read_csv(BLACKLIST_PATH, header=None, names=['Title', 'Year'], usecols=[0, 1], encoding='utf-8')
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

    def fetch_tmdb_details(self, tmdb_id: str) -> Tuple[List[str], List[str]]:
        movie_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=keywords"
        response = self.session.get(movie_url)

        if response.status_code == 200:
            movie_data = response.json()
            keywords = [keyword['name'] for keyword in movie_data['keywords']['keywords']]
            genres = [genre['name'] for genre in movie_data['genres']]
            return keywords, genres
        else:
            if response.status_code == 401:
                print("Check your API key.")
            return [], []

    def add_to_blacklist(self, film_title: str, release_year: str, reason: str) -> None:
        if not any((film_title.lower() == row['Title'].lower() and 
                   release_year == row['Year']) for _, row in self.blacklist.iterrows()):
            with open(BLACKLIST_PATH, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([film_title, release_year, reason])
            print(f"{film_title} ({release_year}) added to blacklist due to: {reason}")

    def is_whitelisted(self, film_title: str, release_year: str) -> bool:
        return any((film_title.lower() == row['Title'].lower() and 
                   release_year == row['Year']) for _, row in self.whitelist.iterrows())

    def is_blacklisted(self, film_title: str, release_year: str) -> bool:
        return any((film_title.lower() == row['Title'].lower() and 
                   release_year == row['Year']) for _, row in self.blacklist.iterrows())

    @staticmethod
    def extract_runtime(soup: BeautifulSoup) -> int:
        scripts = soup.find_all('script')
        for script in scripts:
            if 'var filmData' in script.text:
                match = re.search(r'runTime:\s*(\d+)', script.text)
                if match:
                    return int(match.group(1))
        return 0

def setup_webdriver() -> webdriver.Firefox:
    options = Options()
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

class LetterboxdScraper:
    def __init__(self):
        self.driver = setup_webdriver()
        self.processor = MovieProcessor()
        self.base_url = 'https://letterboxd.com/films/popular/'
        self.total_titles = 0
        self.processed_titles = 0
        self.valid_movies_count = 0
        self.page_number = 1
        self.start_time = time.time()

    def scrape_movies(self):
        try:
            with tqdm(total=MAX_MOVIES, desc="Processing movies", unit=" movies") as pbar:
                while self.valid_movies_count < MAX_MOVIES:
                    url = f'{self.base_url}page/{self.page_number}/'
                    retries = 0
                    success = False

                    # Page loading with retries
                    while retries < MAX_RETRIES and not success:
                        try:
                            print(f'Loading page {self.page_number}...')
                            print(f'Sending GET request to: {url}')
                            self.driver.get(url)
                            print(f'Received response. Parsing HTML content...')
                            time.sleep(2)
                            
                            film_containers = self.driver.find_elements(By.CSS_SELECTOR, 'div.react-component.poster')
                            if len(film_containers) == 72:
                                success = True
                            else:
                                print(f"Unexpected number of film containers ({len(film_containers)}). Retrying...")
                                retries += 1
                                time.sleep(RETRY_DELAY)
                        
                        except Exception as e:
                            print(f"Connection error: {e}. Retrying in {RETRY_DELAY} seconds...")
                            retries += 1
                            time.sleep(RETRY_DELAY)
                    
                    if not success:
                        print("Failed to retrieve the expected number of film containers after 5 attempts. Exiting.")
                        break

                    print(f"\n{f' Page {self.page_number} ':=^100}")
                    
                    # Process each film on the page
                    for container in film_containers:
                        if self.valid_movies_count >= MAX_MOVIES:
                            print(f"\nReached max movies limit ({MAX_MOVIES}). Stopping scraping.")
                            return

                        film_title = container.get_attribute('data-film-name')
                        film_url = container.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
                        
                        # Get initial movie data
                        response = self.processor.session.get(film_url)
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Increment total_titles only after we've successfully fetched the movie's data
                        self.total_titles += 1

                        # Extract release year
                        release_year_tag = soup.find('meta', property='og:title')
                        release_year = None
                        if release_year_tag:
                            release_year_content = release_year_tag['content']
                            release_year = release_year_content.split('(')[-1].strip(')')

                        # Extract TMDb ID
                        body_tag = soup.find('body')
                        tmdb_id = body_tag.get('data-tmdb-id') if body_tag else None

                        # Extract rating count
                        rating_count = 0
                        for script in soup.find_all('script'):
                            if 'aggregateRating' in script.text:
                                match = re.search(r'ratingCount":(\d+)', script.text)
                                if match:
                                    rating_count = int(match.group(1))
                                    break

                        # Check 1: Rating count minimum
                        if rating_count < MIN_RATING_COUNT:
                            print(f"❌ {film_title} was not added due to insufficient ratings: {rating_count} ratings.")
                            self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Insufficient ratings (< 1000)'])
                            continue

                        # Check 2: Whitelist
                        if self.processor.is_whitelisted(film_title, release_year):
                            movie_identifier = (film_title.lower(), release_year)
                            if movie_identifier not in self.processor.added_movies:
                                self.process_approved_movie(film_title, release_year, tmdb_id, soup, "whitelisted")
                                continue

                        # Check 3: Blacklist
                        if self.processor.is_blacklisted(film_title, release_year):
                            print(f"❌ {film_title} was not added due to being blacklisted.")
                            self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Blacklisted'])
                            continue

                        # Check 4: TMDB ID
                        if not tmdb_id:
                            print(f"❌ {film_title} was not added due to missing TMDB ID.")
                            self.processor.rejected_data.append([film_title, release_year, None, 'Missing TMDB ID'])
                            self.processor.unfiltered_denied.append([film_title, release_year, None])
                            continue

                        # Check 5: Runtime
                        runtime = self.processor.extract_runtime(soup)
                        if runtime < MIN_RUNTIME:
                            print(f"❌ {film_title} was not added due to a short runtime of {runtime} minutes.")
                            self.processor.rejected_data.append([film_title, release_year, tmdb_id, 'Short runtime'])
                            self.processor.add_to_blacklist(film_title, release_year, 'Short runtime')
                            continue

                        # Check 6: Keywords and Genres
                        if tmdb_id:
                            keywords, genres = self.processor.fetch_tmdb_details(tmdb_id)
                            
                            # Check keywords
                            matching_keywords = [k for k in FILTER_KEYWORDS if k in keywords]
                            if matching_keywords:
                                rejection_reason = f"Due to being a {', '.join(matching_keywords)}."
                                print(f"❌ {film_title} was not added {rejection_reason}")
                                self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                                self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                                continue

                            # Check genres
                            matching_genres = [g for g in FILTER_GENRES if g in genres]
                            if matching_genres:
                                rejection_reason = f"Due to being a {', '.join(matching_genres)}."
                                print(f"❌ {film_title} was not added {rejection_reason}")
                                self.processor.rejected_data.append([film_title, release_year, tmdb_id, rejection_reason])
                                self.processor.add_to_blacklist(film_title, release_year, rejection_reason)
                                continue

                        # If we reach here, the movie is approved
                        self.process_approved_movie(film_title, release_year, tmdb_id, soup, "approved")

                        # Update progress information
                        elapsed_time = time.time() - self.start_time
                        movies_per_second = self.valid_movies_count / elapsed_time if elapsed_time > 0 else 0
                        estimated_total_time = MAX_MOVIES / movies_per_second if movies_per_second > 0 else 0
                        time_remaining = estimated_total_time - elapsed_time if estimated_total_time > 0 else 0

                        print(f"\n{f'Overall Progress: {self.valid_movies_count}/{MAX_MOVIES} films':^100}")
                        print(f"{f'Elapsed Time: {format_time(elapsed_time)} | Estimated Time Remaining: {format_time(time_remaining)}':^100}")
                        print(f"{f'Processing Speed: {movies_per_second:.2f} movies/second':^100}")
                        
                        pbar.update(1)

                    print(f"\n{f'Completed Page {self.page_number}':=^100}")
                    self.page_number += 1
                    time.sleep(random.uniform(2, 4))

        finally:
            self.driver.quit()

    def process_approved_movie(self, film_title: str, release_year: str, tmdb_id: str, soup: BeautifulSoup, approval_type: str):
        """Process an approved movie and extract all its metadata"""
        # Check max movies before processing
        if self.valid_movies_count >= MAX_MOVIES:
            return

        movie_identifier = (film_title.lower(), release_year)
        if movie_identifier in self.processor.added_movies:
            return

        # Increment counter before processing to ensure we don't go over
        self.valid_movies_count += 1

        self.processor.film_data.append({
            'Title': film_title,
            'Year': release_year,
            'tmdbID': tmdb_id
        })
        self.processor.added_movies.add(movie_identifier)

        # Extract directors
        director_elements = soup.select('span.directorlist a.contributor')
        for director in director_elements:
            director_name = director.get_text(strip=True)
            if director_name:
                self.processor.director_counts[director_name] = self.processor.director_counts.get(director_name, 0) + 1

        # Extract actors without roles
        actor_elements = soup.select('#tab-cast .text-sluglist a.text-slug.tooltip')
        for actor in actor_elements:
            actor_name = actor.get_text(strip=True)
            if actor_name:
                self.processor.actor_counts[actor_name] = self.processor.actor_counts.get(actor_name, 0) + 1

        # Extract decades
        decade_elements = soup.select_one('meta[property="og:title"]')
        if decade_elements:
            content = decade_elements.get("content")
            if content:
                year = int(content.split('(')[-1].split(')')[0])
                decade = (year // 10) * 10
                self.processor.decade_counts[decade] = self.processor.decade_counts.get(decade, 0) + 1

        # Extract genres
        for heading in soup.select('#tab-genres h3'):
            if "Genre" in heading.get_text() or "Genres" in heading.get_text():
                sluglist = heading.find_next_sibling(class_='text-sluglist')
                if sluglist:
                    genre_elements = sluglist.select('a.text-slug')
                    for genre in genre_elements:
                        genre_name = genre.get_text(strip=True)
                        if genre_name:
                            self.processor.genre_counts[genre_name] = self.processor.genre_counts.get(genre_name, 0) + 1

        # Extract studios
        for heading in soup.select('#tab-details h3'):
            if "Studio" in heading.get_text() or "Studios" in heading.get_text():
                sluglist = heading.find_next_sibling(class_='text-sluglist')
                if sluglist:
                    studio_elements = sluglist.select('a.text-slug')
                    for studio in studio_elements:
                        studio_name = studio.get_text(strip=True)
                        if studio_name:
                            self.processor.studio_counts[studio_name] = self.processor.studio_counts.get(studio_name, 0) + 1

        # Extract languages (modified)
        movie_languages = set()  # Use a set to store unique languages for this movie
        for heading in soup.select('#tab-details h3'):
            if any(lang in heading.get_text() for lang in ["Language", "Primary Language", "Languages", "Primary Languages"]):
                sluglist = heading.find_next_sibling(class_='text-sluglist')
                if sluglist:
                    language_elements = sluglist.select('a.text-slug')
                    for language in language_elements:
                        language_name = language.get_text(strip=True)
                        if language_name:
                            movie_languages.add(language_name)  # Add to set of languages for this movie
        
        # Update the language counts only once per language per movie
        for language_name in movie_languages:
            self.processor.language_counts[language_name] = self.processor.language_counts.get(language_name, 0) + 1

        # Extract countries
        for heading in soup.select('#tab-details h3'):
            if "Country" in heading.get_text() or "Countries" in heading.get_text():
                sluglist = heading.find_next_sibling(class_='text-sluglist')
                if sluglist:
                    country_elements = sluglist.select('a.text-slug')
                    for country in country_elements:
                        country_name = country.get_text(strip=True)
                        if country_name:
                            self.processor.country_counts[country_name] = self.processor.country_counts.get(country_name, 0) + 1

        # Add to appropriate lists and print status
        if approval_type == "whitelisted":
            print(f"✅ {film_title} was added due to being whitelisted. ({self.valid_movies_count}/{MAX_MOVIES})")
        else:
            print(f"✅ {film_title} was approved. ({self.valid_movies_count}/{MAX_MOVIES})")
            self.processor.unfiltered_approved.append([film_title, release_year, tmdb_id])

    def save_results(self):
        """Save all results to files"""
        # Save movie data in chunks
        num_chunks = (len(self.processor.film_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(num_chunks):
            start_idx = i * CHUNK_SIZE
            end_idx = min((i + 1) * CHUNK_SIZE, len(self.processor.film_data))
            chunk_df = pd.DataFrame(self.processor.film_data[start_idx:end_idx])
            chunk_df = chunk_df[['Title', 'Year', 'tmdbID']]
            output_path = os.path.join(BASE_DIR, f'popular_filtered_movie_titles{i + 1}.csv')
            chunk_df.to_csv(output_path, index=False, encoding='utf-8')

        # Save rejected data
        rejected_path = os.path.join(BASE_DIR, 'rejected_movies.csv')
        with open(rejected_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Year', 'tmdbID', 'Reason'])
            writer.writerows(self.processor.rejected_data)

        # Save unfiltered approved data (append mode)
        approved_path = os.path.join(BASE_DIR, 'unfiltered_approved.csv')
        with open(approved_path, mode='a', newline='', encoding='utf-8') as file:  # Change to 'a' for append
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_approved:
                writer.writerow(movie + ["2500 Popular"])  # Append "2500 Popular" as the fourth column

        # Save unfiltered denied data (append mode)
        denied_path = os.path.join(BASE_DIR, 'unfiltered_denied.csv')
        with open(denied_path, mode='a', newline='', encoding='utf-8') as file:  # Change to 'a' for append
            writer = csv.writer(file)
            for movie in self.processor.unfiltered_denied:
                writer.writerow(movie + ["2500 Popular"])  # Append "2500 Popular" as the fourth column

        # Save statistics
        self.save_statistics()

    def save_statistics(self):
        """Save statistics to text file"""
        def get_top_10(counts_dict):
            return sorted(counts_dict.items(), key=lambda item: item[1], reverse=True)[:10]

        def get_ordinal(n):
            if 10 <= n % 100 <= 20:
                suffix = 'th'
            else:
                suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
            return str(n) + suffix

        current_date = datetime.now()
        formatted_date = current_date.strftime('%B ') + get_ordinal(current_date.day) + f", {current_date.year}"

        stats_path = os.path.join(BASE_DIR, 'popular_filtered_titles_stats.txt')
        with open(stats_path, mode='w', encoding='utf-8') as file:
            file.write("<strong>The Top 2,500 Most Popular Narrative Feature Films on Letterboxd.</strong>\n\n")
            file.write(f"<strong>Last updated: {formatted_date}</strong>\n\n")
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

            # Write all top 10 statistics
            categories = [
                ("directors", self.processor.director_counts),
                ("actors", self.processor.actor_counts),
                ("decades", self.processor.decade_counts),
                ("genres", self.processor.genre_counts),
                ("studios", self.processor.studio_counts),
                ("languages", self.processor.language_counts),
                ("countries", self.processor.country_counts)
            ]

            for category_name, counts in categories:
                file.write(f"<strong>The ten most appearing {category_name}:</strong>\n")
                for item, count in get_top_10(counts):
                    file.write(f"{item}: {count}\n")
                file.write("\n")

            file.write("If you notice any movies you believe should/should not be included just let me know!")

        print("Top 10 statistics saved to popular_filtered_titles_stats.txt")

def main():
    start_time = time.time()
    try:
        scraper = LetterboxdScraper()
        scraper.scrape_movies()
        scraper.save_results()

        # Format final statistics
        print(f"\n{'Final Statistics':=^100}")
        print(f"{'Total movies scraped:':<30} {scraper.total_titles:>10}")
        print(f"{'Total accepted:':<30} {scraper.valid_movies_count:>10}")
        print(f"{'Total rejected:':<30} {len(scraper.processor.rejected_data):>10}")
        print(f"{'Total unfiltered approved:':<30} {len(scraper.processor.unfiltered_approved):>10}")
        print(f"{'Total unfiltered denied:':<30} {len(scraper.processor.unfiltered_denied):>10}")

        # Format execution time
        execution_time = time.time() - start_time
        print(f"\n{'Execution Summary':=^100}")
        print(f"Total execution time: {format_time(execution_time)}")
        print(f"Average processing speed: {scraper.valid_movies_count / execution_time:.2f} movies/second")

    except Exception as e:
        print(f"\n{'Error':=^100}")
        print(f"❌ An error occurred during execution: {e}")
    finally:
        if 'scraper' in locals():
            try:
                scraper.driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()