"""
Command-line interface for WebScraper-Plus
"""

import sys
import argparse
import tempfile
import logging
from webscraper import WebScraper, __version__

def main():
    """
    Main entry point for the webscraper-plus command-line interface
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='WebScraper-Plus - Extract content from web pages',
        epilog='Example: webscraper-plus --url https://example.com --output ./scraped --text --links --docs --images'
    )
    
    parser.add_argument('--version', action='version', version=f'WebScraper-Plus {__version__}')
    parser.add_argument('--url', type=str, help='URL to scrape')
    parser.add_argument('--output', type=str, default=tempfile.gettempdir(), 
                        help='Output directory for scraped content')
    parser.add_argument('--text', action='store_true', help='Extract webpage text')
    parser.add_argument('--links', action='store_true', help='Extract hyperlinks')
    parser.add_argument('--docs', action='store_true', help='Download documents')
    parser.add_argument('--images', action='store_true', help='Download images')
    parser.add_argument('--flat', action='store_true', help='Use flat directory structure')
    parser.add_argument('--all', action='store_true', help='Enable all extraction options')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    
    # Check for URL
    if not args.url:
        parser.print_help()
        print("\nError: URL is required")
        sys.exit(1)
    
    # Set extraction flags
    extract_all = args.all
    extract_text = args.text or extract_all
    extract_links = args.links or extract_all
    extract_docs = args.docs or extract_all
    extract_images = args.images or extract_all
    
    # If no extraction flags specified, enable all
    if not any([extract_text, extract_links, extract_docs, extract_images]):
        extract_text = extract_links = extract_docs = extract_images = True
    
    try:
        # Print configuration
        print(f"\nWebScraper-Plus {__version__}")
        print(f"Scraping URL: {args.url}")
        print(f"Output directory: {args.output}")
        print(f"Extraction options - Text: {extract_text}, Links: {extract_links}, "
              f"Docs: {extract_docs}, Images: {extract_images}, "
              f"Structure: {'Flat' if args.flat else 'Nested'}")
        
        # Create scraper instance
        scraper = WebScraper(
            args.url,
            base_output_dir=args.output,
            extract_text=extract_text,
            extract_links=extract_links,
            extract_documents=extract_docs,
            extract_images=extract_images,
            flat_structure=args.flat
        )
        
        # Execute scraping
        results = scraper.scrape()
        
        # Print results
        print("\nScraping Results:")
        for item in results:
            print(f"- {item}")
        
        # Print output location
        print(f"\nScraped content saved to: {scraper.output_dir}")
        
    except ValueError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)
    
if __name__ == "__main__":
    main() 