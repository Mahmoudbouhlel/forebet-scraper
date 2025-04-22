from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import cloudscraper
import time
import traceback
import pymysql
import pandas as pd
import datetime
import argparse
import logging
import sys
from typing import List, Dict, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from urllib.parse import urljoin
import re
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('forebet_scraper.log')
    ]
)
logger = logging.getLogger('forebet_scraper')

# Base configuration
BASE_URL = "https://www.forebet.com"
MAX_RETRIES = 3
REQUEST_DELAY = 5  # seconds between requests
MIN_DELAY = 3  # minimum delay
MAX_DELAY = 7  # maximum delay

# MySQL configuration
MYSQL_CONFIG = {
    "host": "db-9e954167-46f4-4bab-90a3-b65ba795ae3e.us-east-2.public.db.laravel.cloud",
    "port": 3306,
    "user": "w2svkrr7cmxtqzaz",
    "password": "YJpUV7Ei13F6CniBk0Em",
    "database": "main",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_url_with_date(date: str) -> str:
    """Generate the URL with the specified date.
    
    Args:
        date: Date in YYYY-MM-DD format
        
    Returns:
        URL string with the specified date
    """
    # Validate date format
    try:
        datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {date}. Date must be in YYYY-MM-DD format.")
        raise ValueError(f"Invalid date format: {date}. Date must be in YYYY-MM-DD format.")
    
    # Construct URL with date
    return f"{BASE_URL}/en/football-predictions/predictions-1x2/{date}"

def test_mysql_connection() -> bool:
    """Test the MySQL connection."""
    logger.info("Testing database connection...")
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        conn.ping(reconnect=True)
        conn.close()
        logger.info("Database connection successful!")
        return True
    except pymysql.MySQLError as err:
        logger.error(f"MySQL Error: {err}")
        traceback.print_exc()
        return False
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        traceback.print_exc()
        return False

def setup_driver() -> webdriver.Chrome:
    """Configure and start the Chrome WebDriver."""
    logger.info("Setting up Chrome WebDriver...")
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36")
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # Performance optimizations
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheet": 2,
        "profile.default_content_setting_values.notifications": 2
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        # Execute CDP commands to avoid detection
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })
        logger.info("WebDriver setup complete")
        return driver
    except Exception as e:
        logger.error(f"Failed to set up WebDriver: {e}")
        raise

def random_delay():
    """Add a random delay between requests to avoid detection."""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    return delay

def load_full_page(driver: webdriver.Chrome, url: str) -> str:
    """Load the full page content by clicking 'More' buttons."""
    logger.info(f"Loading page: {url}")
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".rcnt"))
        )
        
        # Scroll slowly to simulate human behavior
        total_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")
        current_position = 0
        
        while current_position < total_height:
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            current_position += int(viewport_height * 0.7)  # Scroll 70% of viewport
            time.sleep(0.5)
            
            # Update total height as it might change dynamically
            total_height = driver.execute_script("return document.body.scrollHeight")
        
        # Click "More" buttons to load all matches
        more_buttons_clicked = 0
        max_attempts = 20  # Safety limit
        attempt = 0
        
        while attempt < max_attempts:
            try:
                more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//div[@id="mrows"]/span[text()="More"]'))
                )
                # Scroll to button
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", more_button)
                time.sleep(1)
                more_button.click()
                more_buttons_clicked += 1
                logger.info(f"Clicked 'More' button ({more_buttons_clicked})")
                # Random delay between clicks
                time.sleep(random.uniform(1.5, 3))
                attempt += 1
            except Exception:
                logger.info(f"No more 'More' buttons found after {more_buttons_clicked} clicks")
                break
        
        logger.info("Finished loading full page content")
        return driver.page_source
    except Exception as e:
        logger.error(f"Error loading page: {e}")
        # Take screenshot for debugging
        try:
            driver.save_screenshot("error_screenshot.png")
            logger.info("Error screenshot saved as error_screenshot.png")
        except:
            pass
        raise

def fix_forebet_url(url: str) -> str:
    """Fix duplicate base URL in Forebet URLs."""
    if not url:
        return ""
    
    # Remove any duplicate base URLs
    if url.count(BASE_URL) > 1:
        return BASE_URL + url.split(BASE_URL, 1)[1]
    
    # Ensure URL has proper base
    if not url.startswith(BASE_URL):
        if url.startswith('/'):
            return BASE_URL + url
        else:
            return f"{BASE_URL}/{url}"
    
    return url

def extract_standing(soup: BeautifulSoup, team_name: str) -> str:
    """Extract team standing position from the page."""
    # Check in standings tables
    for table_selector in ["#stand_hidden table.standings", "#short_standings table.standings"]:
        table = soup.select_one(table_selector)
        if table:
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 2 and team_name.lower() in cols[1].text.strip().lower():
                    return cols[0].text.strip()

    # Check in team container
    teams_container = soup.find("div", class_="teamtablesp_container")
    if teams_container:
        left = teams_container.find("span", class_="teamtableleft")
        right = teams_container.find("span", class_="teamtableright")
        
        if left and team_name.lower() in left.text.lower():
            return left.text.strip().split()[0] if left.text.strip().split() else ""
        if right and team_name.lower() in right.text.lower():
            return right.text.strip().split()[0] if right.text.strip().split() else ""
            
    return ""

def extract_standing_details(soup: BeautifulSoup, team_name: str) -> Dict[str, str]:
    """Extract detailed team statistics from standings table."""
    stats_fields = {
        "PTS": "", "GP": "", "W": "", "D": "", "L": "",
        "GF": "", "GA": "", "GD": ""
    }
    
    # Try both possible tables
    tables = [
        soup.select_one("#stand_hidden table.standings"),
        soup.select_one("#short_standings table.standings")
    ]

    for table in tables:
        if not table:
            continue

        rows = table.find_all("tr", class_=["color0", "color1"])
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 10:
                continue

            try:
                team_cell = cols[1].get_text(strip=True)
                if team_name.lower() in team_cell.lower():
                    stats = {}
                    # Map columns to stats with safer indexing
                    col_map = {2: "PTS", 3: "GP", 4: "W", 5: "D", 6: "L", 7: "GF", 8: "GA", 9: "GD"}
                    
                    for idx, stat_key in col_map.items():
                        if len(cols) > idx:
                            stats[stat_key] = cols[idx].get_text(strip=True)
                        else:
                            stats[stat_key] = ""
                    
                    return stats
            except Exception as e:
                logger.warning(f"Error parsing standings for {team_name}: {e}")
                continue

    logger.warning(f"No standings found for {team_name}")
    return stats_fields

def fetch_match_details(game_url: str, home_team: str, away_team: str, scraper: cloudscraper.CloudScraper) -> Dict[str, str]:
    """Fetch detailed match information from the match page."""
    logger.info(f"Fetching details for {home_team} vs {away_team}".encode('utf-8', errors='ignore').decode('utf-8'))
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Random delay between requests
            delay = random_delay()
            
            # Add unique query param to avoid cache
            cache_buster = f"?_cb={int(time.time())}"
            full_url = f"{game_url}{cache_buster}"
            
            response = scraper.get(full_url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")

                # Extract rankings
                home_rank = extract_standing(soup, home_team)
                away_rank = extract_standing(soup, away_team)

                # Extract detailed stats
                home_stats = extract_standing_details(soup, home_team)
                away_stats = extract_standing_details(soup, away_team)
                
                # Extract league name
                league_name = ""
                league_container = soup.find("div", class_="teamtablesp_container")
                if league_container:
                    league_center = league_container.find("center", class_="leagpredlnk")
                    if league_center:
                        league_link = league_center.find("a", class_="leagpred_btn")
                        if league_link:
                            league_name = league_link.get_text(strip=True)
                
                # Combine results
                result = {
                    "Home Rank": home_rank,
                    "Away Rank": away_rank,
                    "League": league_name
                }

                # Add home team stats with prefix
                result.update({f"Home {k}": v for k, v in home_stats.items()})
                # Add away team stats with prefix
                result.update({f"Away {k}": v for k, v in away_stats.items()})

                logger.info(f"Successfully fetched details for {home_team} vs {away_team}")
                return result
            else:
                logger.warning(f"HTTP {response.status_code} for {game_url}, attempt {attempt}/{MAX_RETRIES}")
                
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {home_team} vs {away_team}: {e}")
            
        # Increase delay on failures
        time.sleep(attempt * 2)

    logger.error(f"Failed to fetch details for {home_team} vs {away_team} after {MAX_RETRIES} attempts")
    return {
        "Home Rank": "", "Away Rank": "", "League": "",
        "Home PTS": "", "Home GP": "", "Home W": "", "Home D": "", "Home L": "",
        "Home GF": "", "Home GA": "", "Home GD": "",
        "Away PTS": "", "Away GP": "", "Away W": "", "Away D": "", "Away L": "",
        "Away GF": "", "Away GA": "", "Away GD": ""
    }

def parse_page(html: str, scraper: cloudscraper.CloudScraper) -> List[Dict[str, str]]:
    """Parse the page HTML to extract match information."""
    soup = BeautifulSoup(html, "html.parser")
    matches = soup.find_all("div", class_="rcnt")
    total = len(matches)
    logger.info(f"Found {total} matches to parse")

    if total == 0:
        logger.warning("No matches found on the page. Check if the page structure has changed.")
        return []

    predictions = []
    batch_size = 10  # Process matches in smaller batches
    
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        logger.info(f"Processing batch {batch_start+1}-{batch_end} of {total} matches")
        
        temp_matches = []
        
        # Extract basic info for this batch
        for i in range(batch_start, batch_end):
            match = matches[i]
            try:
                # Extract all required elements
                meta = match.find("meta", {"itemprop": "name"})
                prediction_span = match.find("span", class_="forepr")
                probs = match.find("div", class_="fprc")
                link_tag = match.find("a", class_="tnmscn")
                time_tag = match.find("span", class_="date_bah")
                time_element = match.find("time", {"itemprop": "startDate"})
                score_full = match.find("b", class_="l_scr")
                score_half = match.find("span", class_="ht_scr")
                et_min = match.find("div", class_="ladtm")
                et_minute = match.find("span", class_="l_min")
                live_odds_tag = match.find("span", class_="lscrsp")
                
                # Skip if essential elements are missing
                if not all([meta, prediction_span, probs, link_tag]):
                    logger.warning(f"Match #{i+1} skipped: Missing essential elements")
                    continue

                # Extract all data
                game_name = meta.get("content", "").strip()
                prediction = prediction_span.get_text(strip=True)
                match_time = time_tag.text.strip() if time_tag else ""
                match_datetime = time_element.get("datetime", "") if time_element else ""
                match_score = score_full.text.strip() if score_full else ""
                half_time_score = score_half.text.strip() if score_half else ""
                extra_time = et_min.text.strip() if et_min else ""
                extra_minute = et_minute.text.strip() if et_minute else ""
                live_odds = live_odds_tag.text.strip() if live_odds_tag else ""

                # Extract probabilities
                prob_spans = probs.find_all("span")
                if len(prob_spans) != 3:
                    logger.warning(f"Match #{i+1} skipped: Incorrect probability format")
                    continue
                    
                prob_1, prob_x, prob_2 = [p.text.strip() for p in prob_spans]

                # Extract teams and URL
                home_team = link_tag.find("span", class_="homeTeam").text.strip()
                away_team = link_tag.find("span", class_="awayTeam").text.strip()
                raw_href = link_tag.get('href', '')
                game_url = fix_forebet_url(raw_href)
                
                # Try to extract league name
                league_tag = match.find_previous("center", class_="leagpredlnk")
                league_name = ""
                if league_tag:
                    league_link = league_tag.find("a", class_="leagpred_btn")
                    if league_link:
                        league_name = league_link.get_text(strip=True)

                # Store all basic data
                match_data = {
                    "base": {
                        "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Game": game_name,
                        "Time": match_time,
                        "ISO Time": match_datetime,
                        "Score": match_score,
                        "Half-Time Score": half_time_score,
                        "ET": extra_time,
                        "ET Minute": extra_minute,
                        "Prediction": prediction,
                        "1%": prob_1,
                        "X%": prob_x,
                        "2%": prob_2,
                        "Home Team": home_team,
                        "Away Team": away_team,
                        "Match URL": game_url,
                        "League": league_name,
                        "Live Odds": live_odds
                    },
                    "url": game_url,
                    "home": home_team,
                    "away": away_team
                }
                
                temp_matches.append(match_data)
                
            except Exception as e:
                logger.error(f"Error processing match #{i+1}: {str(e)}")
                traceback.print_exc()
                continue

        # Fetch detailed info for this batch using threads
        if temp_matches:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {}
                for m in temp_matches:
                    # Only fetch details if we have a valid URL
                    if m["url"] and m["url"].startswith("http"):
                        future = executor.submit(
                            fetch_match_details, m["url"], m["home"], m["away"], scraper
                        )
                        futures[future] = m
                
                # Process completed futures
                for future in as_completed(futures):
                    match = futures[future]
                    try:
                        result = future.result()
                        match["base"].update(result)
                        predictions.append(match["base"])
                        logger.info(f"Processed: {match['base']['Game']}")
                    except Exception as e:
                        logger.error(f"Error in match detail processing: {e}")
                        # Add match with basic info only if details failed
                        predictions.append(match["base"])

            # Save batch to database
            save_to_mysql(predictions[-len(temp_matches):])
            logger.info(f"Processed {len(temp_matches)} matches in this batch")
            
    return predictions

def save_to_excel(data: List[Dict[str, str]], filename: str = None):
    """Save extracted data to Excel file."""
    if not filename:
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = f"forebet_matches_{date_str}.xlsx"
    
    try:
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Excel file saved as: {filename}")
        return filename
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        return None

def save_to_mysql(data: List[Dict[str, str]]):
    """Save or update match data in MySQL database."""
    if not data:
        logger.warning("No data to save to MySQL")
        return
        
    logger.info(f"Saving {len(data)} records to MySQL...")
    conn = None
    cursor = None
    
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS forebet_matches (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    game VARCHAR(255),
                    time_str VARCHAR(50),
                    iso_time VARCHAR(50),
                    score VARCHAR(50),
                    half_time_score VARCHAR(50),
                    et VARCHAR(50),
                    et_minute VARCHAR(50),
                    prediction VARCHAR(50),
                    prob_1 VARCHAR(20),
                    prob_x VARCHAR(20),
                    prob_2 VARCHAR(20),
                    home_team VARCHAR(255),
                    away_team VARCHAR(255),
                    home_rank VARCHAR(20),
                    away_rank VARCHAR(20),
                    match_url VARCHAR(255) UNIQUE,
                    live_odds VARCHAR(100),
                    home_pts VARCHAR(20),
                    home_gp VARCHAR(20),
                    home_w VARCHAR(20),
                    home_d VARCHAR(20),
                    home_l VARCHAR(20),
                    home_gf VARCHAR(20),
                    home_ga VARCHAR(20),
                    home_gd VARCHAR(20),
                    away_pts VARCHAR(20),
                    away_gp VARCHAR(20),
                    away_w VARCHAR(20),
                    away_d VARCHAR(20),
                    away_l VARCHAR(20),
                    away_gf VARCHAR(20),
                    away_ga VARCHAR(20),
                    away_gd VARCHAR(20),
                    league VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            conn.commit()
        except pymysql.MySQLError as err:
            logger.warning(f"Table check/creation warning: {err}")

        inserted = 0
        updated = 0
        errors = 0

        for row in data:
            try:
                # Check if record exists
                cursor.execute("SELECT id FROM forebet_matches WHERE match_url = %s", (row["Match URL"],))
                exists = cursor.fetchone()

                if exists:
                    # Update existing record
                    cursor.execute("""
                        UPDATE forebet_matches SET
                            score=%s, half_time_score=%s, et=%s, et_minute=%s,
                            prediction=%s, prob_1=%s, prob_x=%s, prob_2=%s,
                            home_team=%s, away_team=%s, home_rank=%s, away_rank=%s,
                            live_odds=%s,
                            home_pts=%s, home_gp=%s, home_w=%s, home_d=%s, home_l=%s,
                            home_gf=%s, home_ga=%s, home_gd=%s,
                            away_pts=%s, away_gp=%s, away_w=%s, away_d=%s, away_l=%s,
                            away_gf=%s, away_ga=%s, away_gd=%s, league=%s
                        WHERE match_url=%s
                    """, (
                        row["Score"], row["Half-Time Score"], row["ET"], row["ET Minute"],
                        row["Prediction"], row["1%"], row["X%"], row["2%"],
                        row["Home Team"], row["Away Team"], row["Home Rank"], row["Away Rank"],
                        row["Live Odds"],
                        row.get("Home PTS", ""), row.get("Home GP", ""), row.get("Home W", ""),
                        row.get("Home D", ""), row.get("Home L", ""), row.get("Home GF", ""),
                        row.get("Home GA", ""), row.get("Home GD", ""),
                        row.get("Away PTS", ""), row.get("Away GP", ""), row.get("Away W", ""),
                        row.get("Away D", ""), row.get("Away L", ""), row.get("Away GF", ""),
                        row.get("Away GA", ""), row.get("Away GD", ""), row.get("League", ""),
                        row["Match URL"]
                    ))
                    updated += 1
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO forebet_matches (
                            timestamp, game, time_str, iso_time, score, half_time_score,
                            et, et_minute, prediction, prob_1, prob_x, prob_2,
                            home_team, away_team, home_rank, away_rank, match_url, live_odds,
                            home_pts, home_gp, home_w, home_d, home_l,
                            home_gf, home_ga, home_gd,
                            away_pts, away_gp, away_w, away_d, away_l,
                            away_gf, away_ga, away_gd, league
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row["Timestamp"], row["Game"], row["Time"], row["ISO Time"], row["Score"],
                        row["Half-Time Score"], row["ET"], row["ET Minute"], row["Prediction"],
                        row["1%"], row["X%"], row["2%"], row["Home Team"], row["Away Team"],
                        row["Home Rank"], row["Away Rank"], row["Match URL"], row["Live Odds"],
                        row.get("Home PTS", ""), row.get("Home GP", ""), row.get("Home W", ""),
                        row.get("Home D", ""), row.get("Home L", ""), row.get("Home GF", ""),
                        row.get("Home GA", ""), row.get("Home GD", ""),
                        row.get("Away PTS", ""), row.get("Away GP", ""), row.get("Away W", ""),
                        row.get("Away D", ""), row.get("Away L", ""), row.get("Away GF", ""),
                        row.get("Away GA", ""), row.get("Away GD", ""), row.get("League", "")
                    ))
                    inserted += 1
                
                # Commit each record individually to avoid losing all on error
                conn.commit()
                
            except pymysql.MySQLError as err:
                errors += 1
                logger.error(f"MySQL Error with {row['Game']}: {err}")
                conn.rollback()  # Rollback the failed transaction
                
        logger.info(f"Database sync complete: {inserted} inserted, {updated} updated, {errors} errors")
        
    except pymysql.MySQLError as err:
        logger.error(f"MySQL Error: {err}")
        traceback.print_exc()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        traceback.print_exc()
    finally:
        # Close connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def run_scraper(date):
    """Main function to run the scraper with a specified date.
    
    Args:
        date: Date in YYYY-MM-DD format
    """
    if not date:
        logger.error("Date parameter is required. Format: YYYY-MM-DD")
        return False
        
    start_time = time.time()
    logger.info(f"Starting scrape at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target date: {date}")
    
    # Generate the URL with the target date
    try:
        target_url = get_url_with_date(date)
        logger.info(f"Target URL: {target_url}")
    except ValueError as e:
        logger.error(str(e))
        return False
    
    # Test database connection before proceeding
    if not test_mysql_connection():
        logger.error("Aborting: Cannot connect to the database.")
        return False
    
    driver = None
    try:
        # Setup and use WebDriver
        driver = setup_driver()
        html = load_full_page(driver, target_url)
        
        # Create a cloudscraper session for detail pages
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Parse the page and get the data
        data = parse_page(html, scraper)
        
        # Export data to Excel if needed
        if data:
            excel_file = save_to_excel(data, f"forebet_matches_{date}.xlsx")
            logger.info(f"Data exported to {excel_file}")
            return True
        else:
            logger.warning("No data was found or extracted.")
            return False
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        # Clean up resources
        if driver:
            driver.quit()
        logger.info(f"Scraping completed in {time.time() - start_time:.2f} seconds")



if __name__ == "__main__":
    # Ask user input
    start_date_str = input("Enter the start date (YYYY-MM-DD): ").strip()
    days_to_scrape_str = input("Enter the number of days to scrape: ").strip()

    try:
        # Parse start date
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        # Parse number of days
        N = int(days_to_scrape_str)
    except ValueError:
        logger.error("Invalid input. Please check your date format (YYYY-MM-DD) and number of days (integer).")
        sys.exit(1)

    logger.info(f"Starting scrape from {start_date} for {N} day(s)")

    for i in range(N):
        current_date = start_date + datetime.timedelta(days=i)
        logger.info(f"Scraping date: {current_date}")
        success = run_scraper(current_date.strftime("%Y-%m-%d"))

        if not success:
            logger.warning(f"Scraping failed for {current_date}")
        else:
            logger.info(f"Scraping finished for {current_date}")

    logger.info("All scraping finished 🎉")
