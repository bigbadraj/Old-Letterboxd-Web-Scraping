import subprocess
import time
from datetime import datetime
import sys
import io
import os

# Set console output encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def format_time(seconds):
    """Format time in a human-readable format"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def run_script(script_name, description):
    """Run a Python script and track its execution"""
    print(f"\n{f' Running {description} ':=^100}")
    start_time = time.time()
    
    try:
        # Set environment variable for UTF-8 encoding
        env = dict(os.environ, PYTHONIOENCODING='utf-8')
        
        # Add encoding parameters to handle special characters
        process = subprocess.Popen(
            ['python', '-u', script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            encoding='utf-8',
            errors='replace',
            env=env  # Add environment variables
        )

        # Print output in real-time with proper encoding
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                # Don't add extra newlines for progress bars
                if '\r' in output:
                    print(output.strip(), end='\r', flush=True)
                else:
                    print(output.strip(), flush=True)

        # Wait for the process to complete
        process.poll()

        # Calculate execution time
        execution_time = time.time() - start_time

        if process.returncode == 0:
            print(f"\n[+] {description} completed successfully")
        else:
            print(f"\n[-] {description} failed with return code {process.returncode}")

        print(f"⏱️ Execution time: {format_time(execution_time)}")
        return True

    except Exception as e:
        print(f"\n[-] Error running {description}: {str(e)}")
        return False

def main():
    start_time = time.time()
    current_date = datetime.now().strftime("%B %d, %Y")
    
    print(f"\n{'='*100}")
    print(f"Starting all scrapers - {current_date}".center(100))
    print(f"{'='*100}\n")

    scripts = [
        ("BoxOfficeMojo Page Scraping CSV.py", "Box Office Mojo Scraper"),
        ("Letterboxd Films Scraping Min Filtering.py", "Letterboxd Min Filtering Scraper"),
        ("Letterboxd List Scraping with Min Filtering (Comedy).py", "Letterboxd Comedy List Scraper"),
        ("Letterboxd Films Scraping with Filtering (Popular).py", "Letterboxd Popular Films Scraper"),
        ("Letterboxd Films Scraping with Filtering (Rating).py", "Letterboxd Rating Films Scraper"),
        ("top_250_genres.py", "Top 250 Genres Scraper")
    ]

    total_scripts = len(scripts)
    completed_scripts = 0

    for script_file, description in scripts:
        completed_scripts += 1
        print(f"\nProgress: {completed_scripts}/{total_scripts} scripts")
        
        if not run_script(script_file, description):
            print(f"\n⚠️ Stopping execution due to error in {description}")
            break

    # Calculate and display total execution time
    total_time = time.time() - start_time
    print(f"\n{'='*100}")
    print(f"All scrapers completed - Total execution time: {format_time(total_time)}".center(100))
    print(f"{'='*100}")

if __name__ == "__main__":
    main()