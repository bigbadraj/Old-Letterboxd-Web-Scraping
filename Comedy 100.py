import requests
from bs4 import BeautifulSoup
import json
import time
import csv
import random
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
import os
import platform
from tqdm import tqdm

# Detect operating system and set appropriate paths
def get_os_specific_paths():
    """Return OS-specific file paths."""
    system = platform.system()
    
    if system == "Windows":
        # Windows paths
        base_dir = r'C:\Users\bigba\aa Personal Projects\Letterboxd List Scraping'
        output_dir = os.path.join(base_dir, 'Outputs')
    elif system == "Darwin":  # macOS
        # macOS paths
        base_dir = '/Users/calebcollins/Documents/Letterboxd List Scraping'
        output_dir = os.path.join(base_dir, 'Outputs')
    
    return {
        'base_dir': base_dir,
        'output_dir': output_dir
    }

# Get OS-specific paths
paths = get_os_specific_paths()
output_dir = paths['output_dir']

# Define a custom print function
def print_to_csv(message: str):
    """Prints a message to the terminal and appends it to All_Outputs.csv."""
    print(message)  # Print to terminal
    with open('Outputs/All_Outputs.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([message])  # Write the message as a new row

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session

def process_film(session, film_url, list_number, min_watches, approved_films):
    try:
        if not film_url.startswith('https://'):
            film_url = film_url.strip('/')
            film_url = f"https://letterboxd.com/film/{film_url}/"
            
        response = session.get(film_url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get film details early to check for duplicates
        title_tag = soup.find('meta', property='og:title')
        title_text = title_tag['content'] if title_tag else "Unknown Title"
        
        # Extract year and title
        year = ''
        if '(' in title_text and ')' in title_text:
            year = title_text[title_text.rindex('(')+1:title_text.rindex(')')]
            title = title_text[:title_text.rindex('(')].strip()
        else:
            title = title_text
            
        # Check for duplicate using title+year combination
        film_key = f"{title}_{year}"
        if film_key in approved_films:
            print_to_csv(f"❌ {title_text} - Not added (Duplicate film)")
            return None
            
        # Get film ID after duplicate check
        film_poster_div = soup.find('div', class_='film-poster')
        film_id = film_poster_div.get('data-film-id') if film_poster_div else "Unknown"
        
        # Extract watch count and title first for logging
        title_tag = soup.find('meta', property='og:title')
        title_text = title_tag['content'] if title_tag else "Unknown Title"
        
        json_ld = soup.find('script', type='application/ld+json')
        if json_ld:
            try:
                json_text = json_ld.string.strip()
                if '/* <![CDATA[ */' in json_text:
                    json_text = json_text.replace('/* <![CDATA[ */', '').replace('/* ]]> */', '')
                film_data = json.loads(json_text)
                
                watch_count = film_data.get('aggregateRating', {}).get('ratingCount', 0)
                if watch_count < min_watches:
                    print_to_csv(f"❌ {title_text} - Not added (Watch count: {watch_count} < {min_watches})")
                    return None
            except json.JSONDecodeError:
                print_to_csv(f"❌ {title_text} - Not added (Error parsing watch count)")
                return None
        else:
            print_to_csv(f"❌ {title_text} - Not added (No watch count data)")
            return None

        print_to_csv(f"✅ {title_text} - Added")
        approved_films.add(film_key)  # Add the title+year combination to approved set
        return {
            'title': title,
            'year': year,
            'id': film_id,
            'original_order': list_number  # Rename to make purpose clearer
        }
        
    except Exception as e:
        print_to_csv(f"❌ {film_url} - Not added (Error: {str(e)})")
        return None

def process_page(session, url, max_films, min_watches, approved_films):
    try:
        # Check if we've already hit the max_films limit before processing the page
        if len(approved_films) >= max_films:
            return False, []
            
        response = session.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        film_grid = soup.find('ul', class_='poster-list')
        if not film_grid:
            return False, []
            
        film_elements = film_grid.find_all('div', class_='film-poster')
        film_data_list = []
        
        for i, film in enumerate(film_elements, 1):
            # Check if we've hit the max_films limit before processing each film
            if len(approved_films) >= max_films:
                print_to_csv(f"\nReached maximum number of films ({max_films}). Stopping...")
                return False, film_data_list
                
            film_url = film.get('data-film-slug')
            if film_url:
                film_data = process_film(session, film_url, len(approved_films) + 1, min_watches, approved_films)
                if film_data:
                    film_data_list.append(film_data)
                    
        has_next = bool(soup.find('a', class_='next'))
        return has_next, film_data_list
        
    except Exception as e:
        print_to_csv(f"Error processing page: {str(e)}")
        return False, []

def main():
    base_url = 'https://letterboxd.com/asset/list/stand-up-comedy-a-comprehensive-list/by/rating/'
    min_watches = 1000
    max_films = 100
    
    session = create_session()
    all_movies = []
    approved_films = set()  # Changed from approved_ids to approved_films
    page = 1
    
    while True:  # Changed from while len(all_movies) < max_films
        url = f'{base_url}page/{page}/'
        print_to_csv(f"\n=== Page {page} ===")
        print_to_csv(f"Progress: {len(all_movies)}/{max_films} movies collected")
        
        has_next, page_data = process_page(session, url, max_films, min_watches, approved_films)
        all_movies.extend(page_data)
        
        # Check if we've hit the max_films limit or reached the end of pages
        if len(approved_films) >= max_films or not has_next:
            break
            
        page += 1
        time.sleep(1)
    
    # Save to CSV maintaining original order
    list_name = "stand_up_comedy"  # You can modify this based on your list
    filepath = os.path.join(output_dir, f"{list_name}.csv")
    
    # Write to CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Year', 'LetterboxdID'])  # Header row
        for movie in all_movies:  # Will naturally maintain the order from processing
            writer.writerow([movie['title'], movie['year'], movie['id']])
    
    print_to_csv(f"Scraped {len(all_movies)} movies")

if __name__ == "__main__":
    main()