#!/usr/bin/env python
"""
Selective scraping example for WebScraper-Plus package.

This example demonstrates how to use the WebScraper-Plus with specific extraction options.
"""

import os
import sys
import logging

# Add the parent directory to sys.path to import the local package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from webscraper import WebScraper

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    
    # Define URL to scrape
    url = "https://www.python.org"
    
    # Define output directory
    base_output_dir = os.path.join(os.getcwd(), "python_org_output")
    
    print(f"Scraping {url} with different extraction options...")
    
    # Example 1: Extract only text and links with flat directory structure
    print("\n--- Example 1: Text and Links Only (Flat Structure) ---")
    text_links_scraper = WebScraper(
        url=url,
        base_output_dir=base_output_dir,
        extract_text=True,
        extract_links=True,
        extract_documents=False,
        extract_images=False,
        flat_structure=True  # Use flat directory structure
    )
    
    # Execute scraping
    results1 = text_links_scraper.scrape()
    
    # Print results
    print("\nResults:")
    for result in results1:
        print(f"- {result}")
    
    print(f"\nExtracted {len(text_links_scraper.get_links())} links")
    print(f"Output Directory: {text_links_scraper.output_dir}")
    
    # Example 2: Extract only images with nested directory structure
    print("\n--- Example 2: Images Only (Nested Structure) ---")
    images_scraper = WebScraper(
        url=url,
        base_output_dir=base_output_dir,
        extract_text=False,
        extract_links=False,
        extract_documents=False,
        extract_images=True,
        flat_structure=False  # Use nested directory structure
    )
    
    # Execute scraping
    results2 = images_scraper.scrape()
    
    # Print results
    print("\nResults:")
    for result in results2:
        print(f"- {result}")
    
    print(f"\nDownloaded {len(images_scraper.get_images())} images")
    print(f"Output Directory: {images_scraper.output_dir}")

if __name__ == "__main__":
    main() 