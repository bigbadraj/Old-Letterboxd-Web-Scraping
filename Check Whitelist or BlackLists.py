import pandas as pd
import requests
from bs4 import BeautifulSoup
import difflib
import unicodedata

WHITELIST_PATH = r'C:\\Users\\bigba\\aa Personal Projects\\Letterboxd List Scraping\\Whitelist.xlsx'

def get_movie_info(letterboxd_url):
    response = requests.get(letterboxd_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    og_title = soup.find('meta', property='og:title')
    if not og_title:
        raise Exception("Could not find movie title/year on the page.")
    content = og_title['content']
    if '(' in content and ')' in content:
        title = content.split('(')[0].strip()
        year = content.split('(')[-1].split(')')[0].strip()
        return title, year
    else:
        raise Exception("Could not parse title/year from og:title meta tag.")

def normalize_text(text):
    return unicodedata.normalize('NFKC', str(text)).strip()

def load_normalized_whitelist():
    df = pd.read_excel(WHITELIST_PATH, header=0, usecols=[0,1], names=['Title', 'Year'])
    df['Title'] = df['Title'].apply(normalize_text)
    df['Year'] = df['Year'].astype(str).str.strip()
    return df

def is_whitelisted(title, year, df, url=None):
    title = normalize_text(title)
    year = str(year).strip()
    
    # URL is king - if we have a URL, check for URL match first
    if url:
        match = df[df['Link'] == url]
        if not match.empty:
            return True
    
    # Only proceed with title/year matching if no URL match was found or no URL was provided
    match = df[
        (df['Title'].str.lower() == title.lower()) &
        (df['Year'] == year)
    ]
    return not match.empty

def find_close_matches(title, whitelist_titles, n=3, cutoff=0.6):
    return difflib.get_close_matches(title, whitelist_titles, n=n, cutoff=cutoff)

if __name__ == "__main__":
    print("Enter Letterboxd movie URLs one at a time. Type 'quit' to exit.")
    df = load_normalized_whitelist()
    while True:
        letterboxd_url = input("\nEnter Letterboxd movie URL (or 'quit' to exit): ").strip()
        if letterboxd_url.lower() == 'quit':
            print("Goodbye!")
            break

        try:
            title, year = get_movie_info(letterboxd_url)
            print(f"Movie found: {normalize_text(title)} ({year})")
            print(f"Comparing: '{normalize_text(title).lower()}' to whitelist entries")
            if is_whitelisted(title, year, df, letterboxd_url):
                print("✅ This movie IS in the whitelist.")
            else:
                print("❌ This movie is NOT in the whitelist.")
                whitelist_titles = df['Title'].str.lower().tolist()
                scraped_title = normalize_text(title).lower()
                close_matches = find_close_matches(scraped_title, whitelist_titles)
                if close_matches:
                    print("Did you mean one of these?")
                    for match in close_matches:
                        match_rows = df[df['Title'].str.lower() == match]
                        years_for_match = match_rows['Year'].unique()
                        years_str = ', '.join(years_for_match)
                        year_match = str(year).strip() in years_for_match
                        print(f"  - {match} (whitelist year(s): {years_str}) {'<-- year matches!' if year_match else ''}")
                    print(f"Scraped year: '{year}'")
        except Exception as e:
            print(f"Error: {e}")