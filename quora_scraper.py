import time
import json
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import signal
import sys

# Configuration
OUTPUT_FILE = "quora_discussions.json"
SEARCH_KEYWORDS = [
    # App-specific keywords
    "Rapido app review",
    "Swiggy delivery experience",
    "Zomato food delivery",
    "Ekart delivery service",
    "Shiprocket logistics",
    "Delhivery tracking",
    "Amazon shopping app",
    "Flipkart online shopping",
    "Tata Neu app",
    "BookMyShow tickets",
    "RedBus booking",
    "IRCTC train booking",
    "ConfirmTkt app",
    "JioMart grocery",
    "Zepto quick delivery",
    "Blinkit grocery delivery",
    "MakeMyTrip booking",
    
    # General keywords
    "food delivery apps India",
    "best grocery delivery app",
    "online shopping apps",
    "train booking apps",
    "movie ticket booking apps",
    "logistics tracking apps",
    "quick delivery services",
    "e-commerce delivery",
    "ride sharing apps",
    "bike taxi services",
    "auto booking apps",
    "cab booking apps",
    "last mile delivery",
    "same day delivery",
    "instant delivery services"
]

MAX_QUESTIONS_PER_KEYWORD = 50
MAX_ANSWERS_PER_QUESTION = 20
SCROLL_PAUSE_TIME = 2
MAX_RETRIES = 3
RETRY_DELAY = 5

# Global flag for graceful shutdown
should_continue = True

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global should_continue
    print("\nReceived interrupt signal. Saving current progress and exiting gracefully...")
    should_continue = False

def save_discussions_to_json(all_discussions):
    """Save all scraped discussions to a single JSON file."""
    data = {
        "total_keywords": len(all_discussions),
        "total_questions": sum(len(discussions) for discussions in all_discussions.values()),
        "scraped_date": datetime.now().isoformat(),
        "discussions": all_discussions
    }
    
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"\nSaved all discussions to {OUTPUT_FILE}")
        print(f"Total keywords: {len(all_discussions)}")
        print(f"Total questions: {data['total_questions']}")
    except Exception as e:
        print(f"Error saving discussions to file: {str(e)}")

def setup_driver():
    """Set up and return a configured Chrome WebDriver."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_to_bottom(driver):
    """Scroll to the bottom of the page to load more content."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def extract_question_data(question_element):
    """Extract data from a question element."""
    try:
        question_text = question_element.find_element(By.CSS_SELECTOR, "div.q-box.qu-display--block").text
        question_url = question_element.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
        
        # Get answer count if available
        try:
            answer_count = question_element.find_element(By.CSS_SELECTOR, "div.q-box.qu-color--gray").text
        except NoSuchElementException:
            answer_count = "0 answers"
        
        return {
            "question": question_text,
            "url": question_url,
            "answer_count": answer_count,
            "scraped_date": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error extracting question data: {str(e)}")
        return None

def scrape_quora_discussions():
    """Main function to scrape Quora discussions."""
    driver = setup_driver()
    wait = WebDriverWait(driver, 10)
    all_discussions = {}
    
    try:
        for keyword in SEARCH_KEYWORDS:
            if not should_continue:
                break
                
            print(f"\nSearching for discussions about: {keyword}")
            discussions = []
            
            # Search for the keyword
            driver.get("https://www.quora.com/search?q=" + keyword.replace(" ", "+"))
            time.sleep(SCROLL_PAUSE_TIME)
            
            # Scroll to load more questions
            for _ in range(3):  # Scroll 3 times to load more content
                scroll_to_bottom(driver)
            
            # Extract questions
            question_elements = driver.find_elements(By.CSS_SELECTOR, "div.q-box.qu-display--block")
            
            for element in question_elements[:MAX_QUESTIONS_PER_KEYWORD]:
                if not should_continue:
                    break
                    
                question_data = extract_question_data(element)
                if question_data:
                    discussions.append(question_data)
                    print(f"Found question: {question_data['question'][:100]}...")
            
            if discussions:
                all_discussions[keyword] = discussions
                print(f"Found {len(discussions)} discussions for '{keyword}'")
            
            time.sleep(RETRY_DELAY)  # Delay between keywords
            
        # Save all discussions to a single file
        if all_discussions:
            save_discussions_to_json(all_discussions)
            
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        scrape_quora_discussions()
        print("\nScraping complete!")
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting gracefully...")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
    finally:
        print("\nScript execution finished.") 