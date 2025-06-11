import json
import re
import os
from pathlib import Path
import emoji
import unicodedata

def clean_text(text):
    """
    Clean text by:
    1. Removing emojis
    2. Removing extra spaces
    3. Removing special characters
    4. Normalizing unicode characters
    5. Converting to lowercase
    """
    if not isinstance(text, str):
        return ""
    
    # Convert to string if not already
    text = str(text)
    
    # Remove emojis
    text = emoji.replace_emoji(text, replace='')
    
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    
    # Remove special characters and extra spaces
    text = re.sub(r'[^\w\s]', ' ', text)  # Replace special chars with space
    text = re.sub(r'\s+', ' ', text)      # Replace multiple spaces with single space
    text = text.strip()                    # Remove leading/trailing spaces
    
    # Convert to lowercase
    text = text.lower()
    
    return text

def process_reviews_file(file_path):
    """Process a single reviews file and clean the reviews."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Clean each review
        for review in data.get('reviews', []):
            if 'content' in review:
                review['content'] = clean_text(review['content'])
            if 'title' in review:
                review['title'] = clean_text(review['title'])
        
        # Create cleaned directory if it doesn't exist
        cleaned_dir = Path('cleaned_reviews')
        cleaned_dir.mkdir(exist_ok=True)
        
        # Save cleaned reviews
        output_file = cleaned_dir / f"cleaned_{file_path.name}"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print(f"Processed {file_path.name}")
        return True
    except Exception as e:
        print(f"Error processing {file_path.name}: {str(e)}")
        return False

def main():
    """Main function to process all review files."""
    # Get all review files
    review_files = Path('playstore_reviews').glob('reviews_*.json')
    
    # Process each file
    total_files = 0
    successful_files = 0
    
    for file_path in review_files:
        total_files += 1
        if process_reviews_file(file_path):
            successful_files += 1
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed {successful_files} out of {total_files} files")
    print(f"Cleaned reviews are saved in the 'cleaned_reviews' directory")

if __name__ == "__main__":
    main() 