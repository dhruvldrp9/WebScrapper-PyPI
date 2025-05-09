#!/usr/bin/env python
"""
Basic usage example for WebScraper-Plus package.

This example demonstrates how to use the WebScraper-Plus to extract content from a webpage.
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
    url = "https://en.wikipedia.org/wiki/Web_scraping"
    
    # Define output directory
    output_dir = os.path.join(os.getcwd(), "scraped_output")
    
    # Create WebScraper instance with all extraction options enabled
    scraper = WebScraper(
        url=url,
        base_output_dir=output_dir,
        extract_text=True,
        extract_links=True,
        extract_documents=True,
        extract_images=True,
        flat_structure=False  # Use nested directory structure
    )
    
    # Execute scraping
    print(f"Scraping {url}...")
    results = scraper.scrape()
    
    # Print results
    print("\nScraping Results:")
    for result in results:
        print(f"- {result}")
    
    # Print extracted text preview
    print("\nExtracted Text Preview (first 500 chars):")
    text = scraper.get_text()
    print(text[:500] if text else "No text extracted")
    
    # Print link count
    print(f"\nExtracted {len(scraper.get_links())} links")
    
    # Print output location
    print(f"\nOutput Directory: {scraper.output_dir}")

if __name__ == "__main__":
    main() 