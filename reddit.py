import requests
import json
import time
import os
import random
from datetime import datetime

# Configuration
APPS = [
    "Amazon Shopping",
    "Flipkart Online Shopping",
    "BookMyShow"
]

# Keywords for specific categories
KEYWORDS = {
    "GroupBooking": "group ticket booking BookMyShow",
    "RedBusGroup": "group booking experience RedBus",
    "AITravel": "generative AI for travel booking",
    "ChatbotTickets": "chatbot ticket booking app",
    "AIAutoBooking": "auto book tickets using AI"
}

OUTPUT_DIR = "reddit_conversations"

# Reddit API configuration
REDDIT_API_URL = "https://www.reddit.com/r/{subreddit}/search.json"
SUBREDDITS = [
    "india",
    "indiaspeaks",
    "IndianGaming",
    "developersIndia",
    "indiaTech",
    "StartUpIndia",
    "IndianStreetBets",
    "IndianGamers",
    "IndianGamingDeals",
    "IndiaSpeaks",
    "IndianTeenagers",
    "IndianFood",
    "IndianFashionAddicts",
    "IndianCinema",
    "IndianGaming",
    "IndianGamingDeals",
    "IndianGamingMarketplace",
    "IndianGamingDeals",
    "IndianGamingMarketplace",
    "IndianGamingDeals"
]

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
]

def get_random_headers():
    """Generate headers with a random user agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

def create_output_directory():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def search_reddit_posts(subreddit, query, limit=100):
    """Search for posts in a subreddit using Reddit's API."""
    url = REDDIT_API_URL.format(subreddit=subreddit)
    params = {
        "q": query,
        "limit": limit,
        "sort": "relevance",
        "t": "all",
        "restrict_sr": "on"
    }
    
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_random_headers(), params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limited. Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print(f"Error {response.status_code} for subreddit {subreddit}")
                return None
        except Exception as e:
            print(f"Error searching {subreddit}: {str(e)}")
            time.sleep(retry_delay)
            retry_delay *= 2
    
    return None

def fetch_reddit_thread_json(thread_id):
    """Fetch a Reddit thread's JSON data."""
    url = f"https://www.reddit.com/comments/{thread_id}.json"
    
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_random_headers())
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limited. Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print(f"Error {response.status_code} for thread {thread_id}")
                return None
        except Exception as e:
            print(f"Error fetching thread {thread_id}: {str(e)}")
            time.sleep(retry_delay)
            retry_delay *= 2
    
    return None

def extract_reddit_conversation(thread_data, max_comments=200):
    """Extract conversation data from a Reddit thread."""
    if not thread_data or len(thread_data) < 2:
        return None
    
    post_data = thread_data[0]['data']['children'][0]['data']
    comments_data = thread_data[1]['data']['children']
    
    # Extract post information
    post = {
        'title': post_data.get('title', ''),
        'text': post_data.get('selftext', ''),
        'author': post_data.get('author', '[deleted]'),
        'score': post_data.get('score', 0),
        'created_utc': post_data.get('created_utc', 0),
        'permalink': f"https://www.reddit.com{post_data.get('permalink', '')}",
        'subreddit': post_data.get('subreddit', ''),
        'num_comments': post_data.get('num_comments', 0)
    }
    
    # Extract comments
    comments = []
    for comment in comments_data:
        if comment['kind'] == 't1':  # Regular comment
            comment_data = comment['data']
            comments.append({
                'text': comment_data.get('body', ''),
                'author': comment_data.get('author', '[deleted]'),
                'score': comment_data.get('score', 0),
                'created_utc': comment_data.get('created_utc', 0),
                'permalink': f"https://www.reddit.com{comment_data.get('permalink', '')}"
            })
    
    return {
        'post': post,
        'comments': comments[:max_comments],
        'total_comments': len(comments),
        'subreddit': post['subreddit'],
        'thread_id': post_data.get('id', ''),
        'url': post['permalink']
    }

def scrape_reddit_conversations_for_app(app_name):
    """Scrape Reddit conversations for a specific app."""
    print(f"\n--- Scraping Reddit conversations for {app_name} ---")
    
    conversations = []
    seen_threads = set()
    
    for subreddit in SUBREDDITS:
        print(f"Searching in r/{subreddit}...")
        
        # Search for posts
        posts_data = search_reddit_posts(subreddit, app_name)
        if not posts_data:
            continue
        
        # Process each post
        for post in posts_data['data']['children']:
            thread_id = post['data']['id']
            
            if thread_id in seen_threads:
                continue
            
            seen_threads.add(thread_id)
            
            # Fetch thread data
            thread_data = fetch_reddit_thread_json(thread_id)
            if not thread_data:
                continue
            
            # Extract conversation
            conversation = extract_reddit_conversation(thread_data)
            if conversation:
                conversations.append(conversation)
                print(f"Found conversation with {len(conversation['comments'])} comments")
            
            # Random delay between thread fetches
            time.sleep(random.uniform(2, 4))
        
        # Random delay between subreddits
        time.sleep(random.uniform(5, 8))
    
    # Save conversations to file
    output_file = f"reddit_conversations/reddit_{app_name.lower().replace(' ', '_')}.json"
    os.makedirs("reddit_conversations", exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(conversations, f, indent=2, ensure_ascii=False)
    
    print(f"Saved {len(conversations)} Reddit conversations to {output_file}")
    return len(conversations)

def scrape_reddit_conversations_for_gen_ai_booking():
    print("\n--- Scraping Reddit conversations for Gen AI Booking Tickets ---")
    all_conversations = []
    total_posts = 0
    
    for subreddit in SUBREDDITS:
        print(f"\nSearching in r/{subreddit}...")
        posts = search_reddit_posts(subreddit, "gen ai booking tickets OR AI travel booking OR automated travel booking")
        total_posts += len(posts['data']['children'])
        
        for i, post in enumerate(posts['data']['children'], 1):
            post_data = post['data']
            print(f"Processing post {i}/{len(posts['data']['children'])}: {post_data.get('title', '')[:50]}...")
            
            thread_data = fetch_reddit_thread_json(post_data['id'])
            if thread_data:
                conversation = extract_reddit_conversation(thread_data)
                if conversation:
                    all_conversations.append(conversation)
                    print(f"Successfully extracted conversation with {len(conversation['comments'])} comments")
            
            # Add random delay between requests
            delay = random.uniform(2, 4)
            print(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)
        
        # Add delay between subreddits
        delay = random.uniform(5, 8)
        print(f"Waiting {delay:.1f} seconds before next subreddit...")
        time.sleep(delay)
    
    # Save to file
    output_file = os.path.join(OUTPUT_DIR, "reddit_gen_ai_booking_tickets.json")
    data = {
        "topic": "Gen AI Booking Tickets",
        "total_posts_found": total_posts,
        "total_conversations": len(all_conversations),
        "total_comments": sum(len(conv['comments']) for conv in all_conversations),
        "scraped_at": datetime.now().isoformat(),
        "conversations": all_conversations
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Saved {len(all_conversations)} Reddit conversations to {output_file}")

def scrape_reddit_by_keywords():
    """Scrape Reddit conversations based on predefined keywords."""
    print("\n--- Scraping Reddit conversations for specific keywords ---")
    all_conversations = {}
    
    for category, keyword in KEYWORDS.items():
        print(f"\nSearching for category: {category} with keyword: {keyword}")
        category_conversations = []
        seen_threads = set()
        
        for subreddit in SUBREDDITS:
            print(f"Searching in r/{subreddit}...")
            
            # Search for posts
            posts_data = search_reddit_posts(subreddit, keyword)
            if not posts_data:
                continue
            
            # Process each post
            for post in posts_data['data']['children']:
                thread_id = post['data']['id']
                
                if thread_id in seen_threads:
                    continue
                
                seen_threads.add(thread_id)
                
                # Fetch thread data
                thread_data = fetch_reddit_thread_json(thread_id)
                if not thread_data:
                    continue
                
                # Extract conversation
                conversation = extract_reddit_conversation(thread_data)
                if conversation:
                    conversation['category'] = category
                    conversation['keyword'] = keyword
                    category_conversations.append(conversation)
                    print(f"Found conversation with {len(conversation['comments'])} comments")
                
                # Random delay between thread fetches
                time.sleep(random.uniform(2, 4))
            
            # Random delay between subreddits
            time.sleep(random.uniform(5, 8))
        
        # Save category conversations
        if category_conversations:
            output_file = os.path.join(OUTPUT_DIR, f"reddit_{category.lower()}.json")
            data = {
                "category": category,
                "keyword": keyword,
                "total_conversations": len(category_conversations),
                "total_comments": sum(len(conv['comments']) for conv in category_conversations),
                "scraped_at": datetime.now().isoformat(),
                "conversations": category_conversations
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            print(f"Saved {len(category_conversations)} conversations for {category} to {output_file}")
            all_conversations[category] = category_conversations
        
        # Random delay between categories
        if category != list(KEYWORDS.keys())[-1]:
            delay = random.uniform(10, 15)
            print(f"\nWaiting {delay:.1f} seconds before next category...")
            time.sleep(delay)
    
    return all_conversations

def main():
    create_output_directory()
    total_conversations = 0
    
    # Scrape app-specific conversations
    for app in APPS:
        conversations_count = scrape_reddit_conversations_for_app(app)
        total_conversations += conversations_count
        
        # Random delay between apps
        if app != APPS[-1]:
            delay = random.uniform(10, 15)
            print(f"\nWaiting {delay:.1f} seconds before next app...")
            time.sleep(delay)
    
    # Scrape keyword-based conversations
    keyword_conversations = scrape_reddit_by_keywords()
    keyword_total = sum(len(convs) for convs in keyword_conversations.values())
    total_conversations += keyword_total
    
    print(f"\nAll Reddit scraping complete!")
    print(f"Total app-specific conversations: {total_conversations - keyword_total}")
    print(f"Total keyword-based conversations: {keyword_total}")
    print(f"Grand total conversations: {total_conversations}")

if __name__ == "__main__":
    main() 