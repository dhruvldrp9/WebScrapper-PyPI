import logging
import os
import sys

# Add the parent directory to sys.path to import the local package
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from webscraper import WebScraper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Create WebScraper with all extraction options enabled
scraper = WebScraper(
    url="https://cloud.google.com/learn/what-is-artificial-intelligence?hl=en",
    extract_text=True,
    extract_links=True,
    extract_documents=True,
    extract_images=True
)

# Run the scraper
results = scraper.scrape()

# Print results
print("\nScraping Results (WebScraper-Plus):")
for result in results:
    print(f"- {result}")

# Print extracted text
print("\nExtracted Text Preview (first 500 chars):")
text = scraper.get_text()
print(text[:500] if text else "No text extracted")

# Print output location
print(f"\nOutput Directory: {scraper.output_dir}")
