import time
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import os
from tqdm import tqdm
import csv

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open('Outputs/All_Outputs.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

# Set up Firefox options and service
options = Options()
options.headless = False  # Set to True if you don't want the browser to open

# Initialize the Firefox driver with GeckoDriver in PATH
service = Service()
driver = webdriver.Firefox(service=service, options=options)

# Base URL of the Letterboxd films page
base_url = 'https://letterboxd.com/films/by/rating/'
film_titles = []
total_titles = 0  # Counter for total titles scraped
page_number = 1  # Start at page 1

max_movies = 250

# Add this constant at the top with other variables
MIN_RATING_COUNT = 1000

class ProgressTracker:
    def __init__(self, total_films):
        self.total_films = total_films
        self.current_count = 0
        self.start_time = time.time()
    
    def increment(self):
        self.current_count += 1
        return self.current_count
    
    def get_elapsed_time(self):
        return time.time() - self.start_time
    
    def get_progress_stats(self):
        elapsed_time = self.get_elapsed_time()
        movies_per_second = self.current_count / elapsed_time if elapsed_time > 0 else 0
        estimated_total_time = self.total_films / movies_per_second if movies_per_second > 0 else 0
        time_remaining = estimated_total_time - elapsed_time if estimated_total_time > 0 else 0
        
        return {
            'elapsed_time': elapsed_time,
            'movies_per_second': movies_per_second,
            'time_remaining': time_remaining
        }

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

# Initialize progress tracker
progress_tracker = ProgressTracker(max_movies)
print_to_csv(f"\n{' Starting Film Scraping ':=^100}")

# Create overall progress bar before the while loop
with tqdm(total=max_movies, desc="Total Progress", unit=" films") as overall_pbar:
    while total_titles < max_movies:
        # Construct the URL for the current page
        url = f'{base_url}page/{page_number}/'
        
        # Send a GET request to the URL
        print_to_csv(f'Sending GET request to: {url}')
        driver.get(url)
        print_to_csv(f'Received response. Parsing HTML content...')

        # Give the page some time to load
        time.sleep(2)

        # Find all film containers
        film_containers = driver.find_elements(By.CSS_SELECTOR, 'div.react-component.poster')

        print_to_csv(f'Found {len(film_containers)} film containers.')

        print_to_csv(f"\n{f' Page {page_number} ':=^100}")
        
        # Remove the nested progress bar and just loop through containers
        for container in film_containers:
            # Break out if we've reached max_movies
            if total_titles >= max_movies:
                break
                
            film_title = container.get_attribute('data-film-name')
            
            if film_title:
                # Get the film's URL and fetch additional details
                film_url = container.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
                response = requests.get(film_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
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
                
                # Only add movies with sufficient ratings
                if rating_count >= MIN_RATING_COUNT:
                    film_titles.append({
                        'Title': film_title,
                        'Year': release_year,
                        'tmdbID': tmdb_id
                    })
                    total_titles += 1
                    progress_tracker.increment()
                    
                    # Update the overall progress bar
                    overall_pbar.update(1)
                    
                    # Break out if we've reached max_movies after adding this film
                    if total_titles >= max_movies:
                        break
                    
                    # Print progress every movie
                    if (len(film_titles) % 1) == 0:
                        stats = progress_tracker.get_progress_stats()
                        print_to_csv(f"\n{f'Overall Progress: {total_titles}/{max_movies} films':^100}")
                        print_to_csv(f"{'Elapsed Time: ' + format_time(stats['elapsed_time']) + ' | Estimated Time Remaining: ' + format_time(stats['time_remaining']):^100}")
                        print_to_csv(f"{'Processing Speed: {:.2f} movies/second'.format(stats['movies_per_second']):^100}")
                        print_to_csv(f"Last Scraped: {film_title} ({release_year})")

        # Move the max_movies check outside the for loop
        if total_titles >= max_movies:
            break

        # Increment the page number for the next iteration
        page_number += 1

# Close the browser
driver.quit()

# Check if any titles were scraped
if film_titles:
    print_to_csv(f'{len(film_titles)} Film titles were scraped successfully:')
else:
    print_to_csv("No film titles were scraped.")

# Create a DataFrame and save to CSV if desired
df = pd.DataFrame(film_titles)
output_csv = os.path.join(r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping\Outputs', 'film_titles.csv')
df.to_csv(output_csv, index=False, encoding='utf-8')
print_to_csv("Film titles have been successfully saved to film_titles.csv.")