import json
import time
import os
import signal
from google_play_scraper import Sort, reviews, app
import random
from datetime import datetime, date
import sys

# Configuration
APP_IDS = {
    "Amazon Shopping": "com.amazon.mShop.android.shopping",
    "Flipkart Online Shopping": "com.flipkart.android",
    "Tata Neu": "com.tatadigital.tcp",
    "BookMyShow": "com.bt.bms",
    "RedBus": "in.redbus.android",
    "IRCTC": "cris.org.in.prs.ima",
    "ConfirmTkt": "com.confirmtkt.lite",
    "JioMart": "com.jiomart",
    "Zepto": "com.zepto.app",
    "Blinkit": "com.grofers.customerapp",
    "MakeMyTrip": "com.makemytrip",
    "Rapido": "com.rapido.passenger",
    "Swiggy": "in.swiggy.android",
    "Zomato": "com.application.zomato",
    "Ekart": "com.ekart.firstmile",
    "Shiprocket": "com.shiprocket.shiprocket",
    "Delhivery": "com.delhiveryConsigneeApp"
}

OUTPUT_DIR = "playstore_reviews"
TARGET_REVIEW_COUNT = 1000  # Target 1000 reviews per app
MAX_RETRIES = 3
RETRY_DELAY = 10  # Reduced delay between retries
REQUEST_DELAY = 1  # Delay between requests
START_DATE = date(2025, 1, 1)  # January 1st, 2025

# Global flag for graceful shutdown
should_continue = True

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global should_continue
    print("\nReceived interrupt signal. Saving current progress and exiting gracefully...")
    should_continue = False

def create_output_directory():
    """Create the output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_output_file(app_name_key):
    """Generates the output filename for a given app."""
    return os.path.join(OUTPUT_DIR, f"reviews_{app_name_key.replace(' ', '_').lower()}.json")

def save_reviews_to_json(app_name_key, all_reviews):
    """Save all scraped reviews for an app to its final JSON file."""
    output_file = get_output_file(app_name_key)
    data = {
        "app_name": app_name_key,
        "app_id": APP_IDS[app_name_key],
        "play_store_url": f"https://play.google.com/store/apps/details?id={APP_IDS[app_name_key]}&hl=en_IN",
        "reviews": all_reviews,
        "total_reviews": len(all_reviews),
        "last_updated": datetime.now().isoformat(),
        "date_range": {
            "start": START_DATE.isoformat(),
            "end": datetime.now().date().isoformat()
        }
    }
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Saved {len(all_reviews)} reviews to {output_file}")
    except Exception as e:
        print(f"Error saving reviews to file: {str(e)}")

def verify_app_exists(app_id):
    """Verify if the app exists in Google Play Store."""
    try:
        app_info = app(app_id)
        return True, app_info.get('title', 'Unknown App')
    except Exception as e:
        print(f"Error verifying app {app_id}: {str(e)}")
        return False, None

def format_review(review):
    """Format a review with proper date handling and content structure."""
    try:
        review_date = review.get('at')
        if not isinstance(review_date, datetime):
            return None

        # Skip reviews before START_DATE
        if review_date.date() < START_DATE:
            return None

        return {
            'review_id': review.get('reviewId'),
            'score': review.get('score'),
            'author_name': review.get('userName'),
            'date': review_date.strftime('%Y-%m-%d %H:%M:%S'),
            'rating': review.get('score'),
            'content': review.get('content', ''),
            'reply_content': review.get('replyContent', ''),
            'thumbs_up_count': review.get('thumbsUpCount', 0),
            'review_created_version': review.get('reviewCreatedVersion', ''),
            'at': review_date.isoformat()
        }
    except Exception as e:
        print(f"Error formatting review: {str(e)}")
        return None

def scrape_app_reviews(app_name, app_id):
    """Scrapes user reviews for a given app from Google Play Store."""
    global should_continue
    
    print(f"\n--- Starting scraping for {app_name} (ID: {app_id}) ---")
    print(f"Play Store URL: https://play.google.com/store/apps/details?id={app_id}&hl=en_IN")
    print(f"Scraping reviews from {START_DATE} to present (Target: {TARGET_REVIEW_COUNT} reviews)")
    
    # Verify app exists first
    exists, actual_app_name = verify_app_exists(app_id)
    if not exists:
        print(f"App {app_name} (ID: {app_id}) not found in Google Play Store. Skipping...")
        return
    
    if actual_app_name != app_name:
        print(f"Note: App name in Play Store is '{actual_app_name}', different from provided name '{app_name}'")
    
    all_reviews = []
    current_reviews_count = 0
    retry_count = 0
    continuation_token = None
    no_new_reviews_count = 0

    while should_continue and retry_count < MAX_RETRIES and current_reviews_count < TARGET_REVIEW_COUNT:
        try:
            # Fetch reviews in batches with proper error handling
            result, new_continuation_token = reviews(
                app_id,
                lang='en',
                country='in',
                sort=Sort.MOST_RELEVANT,
                count=100,
                continuation_token=continuation_token
            )
            
            if not result:
                no_new_reviews_count += 1
                if no_new_reviews_count >= 3:
                    print(f"No more new reviews found for {app_name}. Ending scraping for this app.")
                    break
                time.sleep(REQUEST_DELAY)
                continue

            no_new_reviews_count = 0
            new_reviews_added = False

            for review in result:
                if not should_continue or current_reviews_count >= TARGET_REVIEW_COUNT:
                    break
                    
                formatted_review = format_review(review)
                if formatted_review and not any(r['review_id'] == formatted_review['review_id'] for r in all_reviews):
                    all_reviews.append(formatted_review)
                    current_reviews_count += 1
                    new_reviews_added = True
                    print(f"Scraped review {current_reviews_count}/{TARGET_REVIEW_COUNT} (Date: {formatted_review['date']})")

            if not new_reviews_added:
                no_new_reviews_count += 1
                if no_new_reviews_count >= 3:
                    print(f"No more new reviews found for {app_name}. Ending scraping for this app.")
                    break

            continuation_token = new_continuation_token
            time.sleep(REQUEST_DELAY)

        except KeyboardInterrupt:
            print("\nReceived keyboard interrupt. Saving progress...")
            should_continue = False
            break
        except Exception as e:
            print(f"Error scraping reviews for {app_name}: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds... (Attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Max retries reached for {app_name}. Saving current progress.")
                break
    
    if all_reviews:
        save_reviews_to_json(app_name, all_reviews)
        print(f"--- Finished scraping for {app_name}. Total reviews: {len(all_reviews)} ---")
    else:
        print(f"--- No reviews collected for {app_name} ---")

def is_app_already_scraped(app_name_key):
    """Check if reviews for an app have already been scraped."""
    output_file = get_output_file(app_name_key)
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if len(data.get('reviews', [])) >= TARGET_REVIEW_COUNT:
                    print(f"\nSkipping {app_name_key} - already scraped {len(data['reviews'])} reviews")
                    return True
        except Exception as e:
            print(f"Error checking existing reviews for {app_name_key}: {str(e)}")
    return False

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        create_output_directory()
        for app_name, app_id in APP_IDS.items():
            if not should_continue:
                break
            if not is_app_already_scraped(app_name):
                scrape_app_reviews(app_name, app_id)
            else:
                print(f"Skipping {app_name} as it has already been scraped")
        print("\nAll scraping complete!")
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting gracefully...")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
    finally:
        print("\nScript execution finished.")