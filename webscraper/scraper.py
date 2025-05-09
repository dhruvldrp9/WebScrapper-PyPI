"""
Core WebScraper implementation

This module contains the main WebScraper class that handles extraction of content
from web pages, with support for text, links, documents, and images.
"""

import os
import re
import requests
from bs4 import BeautifulSoup
import urllib.parse
import logging
import tempfile
from typing import List, Dict, Any, Optional, Union

from webscraper.utils import (
    create_safe_folder_name,
    create_safe_filename,
    clean_text,
    validate_url
)

from webscraper.extraction.document import (
    extract_pdf_text,
    extract_docx_text,
    extract_csv_text,
    extract_excel_text
)

from webscraper.extraction.image import (
    extract_image_text,
    check_tesseract_available
)

class WebScraper:
    def __init__(self, 
                url: str, 
                base_output_dir: str = tempfile.gettempdir(),
                extract_text: bool = False,
                extract_links: bool = False,
                extract_documents: bool = False,
                extract_images: bool = False,
                flat_structure: bool = False):
        """
        Initialize the web scraper with precise extraction control
        
        Args:
            url: Target webpage URL
            base_output_dir: Base directory for saving scraped content
            extract_text: Flag to extract webpage text
            extract_links: Flag to extract hyperlinks
            extract_documents: Flag to download documents
            extract_images: Flag to download images
            flat_structure: Flag to use flat directory structure (no subdirectories)
        """
        # Configure logging
        self.logger = logging.getLogger(__name__)
        
        # Validate URL
        if not url or not isinstance(url, str):
            raise ValueError("URL must be a valid string")
        
        if not url.startswith(('http://', 'https://')):
            raise ValueError("URL must start with http:// or https://")

        # Core configuration
        self.url = url
        
        # Strict extraction flags
        self.extract_text = extract_text
        self.extract_links = extract_links
        self.extract_documents = extract_documents
        self.extract_images = extract_images
        self.flat_structure = flat_structure
        
        # Initialize result containers
        self.text_content = ""
        self.links = []
        self.downloaded_documents = []
        self.downloaded_images = []
        self.soup = None
        
        # Validate base_output_dir
        if not os.path.exists(base_output_dir):
            try:
                os.makedirs(base_output_dir, exist_ok=True)
                self.logger.info(f"Created base output directory: {base_output_dir}")
            except Exception as e:
                raise IOError(f"Failed to create base output directory: {str(e)}")
        
        # Create safe, unique folder name
        self.safe_folder_name = create_safe_folder_name(url)
        
        # Create output paths
        self.output_dir = os.path.join(base_output_dir, self.safe_folder_name)
        
        # Set up directory structure based on flat_structure setting
        if self.flat_structure:
            self.links_dir = self.output_dir
            self.docs_dir = self.output_dir
            self.images_dir = self.output_dir
        else:
            self.links_dir = os.path.join(self.output_dir, 'links')
            self.docs_dir = os.path.join(self.output_dir, 'documents')
            self.images_dir = os.path.join(self.output_dir, 'images')
        
        # Conditionally create directories only if extraction is enabled
        try:
            if self.extract_text or self.extract_links or self.extract_documents or self.extract_images:
                os.makedirs(self.output_dir, exist_ok=True)
                
            if not self.flat_structure:
                if self.extract_links:
                    os.makedirs(self.links_dir, exist_ok=True)
                if self.extract_documents:
                    os.makedirs(self.docs_dir, exist_ok=True)
                if self.extract_images:
                    os.makedirs(self.images_dir, exist_ok=True)
        except Exception as e:
            raise IOError(f"Failed to create output directories: {str(e)}")
            
        # Check dependencies if extraction is enabled
        if extract_images:
            self._check_ocr_dependencies()
            
        # Initialize spaCy NLP for text processing if needed
        self.nlp = None
        if extract_text or extract_documents or extract_images:
            self._load_nlp_model()

    def _check_ocr_dependencies(self):
        """Check if OCR dependencies are available"""
        if not check_tesseract_available():
            self.logger.warning("Tesseract OCR not available. OCR functionality will be disabled.")
            self.logger.warning("Install Tesseract OCR for full image text extraction capabilities.")

    def _load_nlp_model(self):
        """Load NLP model for text processing"""
        try:
            import spacy
            try:
                self.nlp = spacy.load('en_core_web_sm')
                self.logger.info("SpaCy model loaded successfully")
            except OSError:
                # Model not found, try to download
                try:
                    self.logger.info("Downloading spaCy English model...")
                    from spacy.cli import download
                    download('en_core_web_sm')
                    self.nlp = spacy.load('en_core_web_sm')
                    self.logger.info("SpaCy model downloaded and loaded successfully")
                except Exception as e:
                    self.logger.warning(f"Failed to download spaCy model: {str(e)}")
                    self.logger.warning("Using basic text cleaning without spaCy")
                    self.nlp = None
        except ImportError:
            self.logger.warning("spaCy not installed. Using basic text cleaning.")
            self.nlp = None

    def fetch_page_content(self):
        """
        Fetch webpage content with robust error handling
        
        Returns:
            Response object or None if fetch fails
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(self.url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Webpage fetch failed: {e}")
            return None

    def extract_links_from_soup(self, soup):
        """
        Extract links from BeautifulSoup object
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of extracted links
        """
        if not self.extract_links:
            return []

        links = set()
        base_url = urllib.parse.urlparse(self.url).scheme + "://" + urllib.parse.urlparse(self.url).netloc
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urllib.parse.urljoin(base_url, href)
            links.add(full_url)
        
        return list(links)

    def save_links(self, links):
        """
        Save extracted links to file
        
        Args:
            links: List of links to save
        """
        if not links:
            return

        try:
            links_filepath = os.path.join(self.links_dir, 'extracted_links.txt')
            with open(links_filepath, 'w', encoding='utf-8') as f:
                for link in links:
                    f.write(f"{link}\n")
            
            self.logger.info(f"Saved {len(links)} links to {links_filepath}")
            
        except Exception as e:
            self.logger.error(f"Failed to save links: {str(e)}")

    def download_documents(self, soup):
        """
        Download documents linked from the webpage
        
        Args:
            soup: BeautifulSoup object
        """
        if not self.extract_documents:
            return
        
        document_extensions = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
            '.csv', '.txt', '.rtf', '.odt', '.ods', '.odp'
        ]
        
        base_url = urllib.parse.urlparse(self.url).scheme + "://" + urllib.parse.urlparse(self.url).netloc
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urllib.parse.urljoin(base_url, href)
            
            # Check if URL points to a document
            if any(full_url.lower().endswith(ext) for ext in document_extensions):
                # Validate URL before downloading
                if validate_url(full_url, extract_documents=True, logger=self.logger):
                    self._download_document(full_url)

    def _download_document(self, url):
        """
        Download a document from URL
        
        Args:
            url: Document URL
        """
        try:
            # Extract filename from URL
            filename = os.path.basename(urllib.parse.urlparse(url).path)
            if not filename:
                filename = "document_" + create_safe_folder_name(url)
            
            # Ensure the filename has extension
            if not any(filename.lower().endswith(ext) for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.csv']):
                filename += '.pdf'  # Default extension
            
            # Create save path
            filepath = os.path.join(self.docs_dir, filename)
            filepath = create_safe_filename(filepath)
            
            # Download file
            self.logger.info(f"Downloading document: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30, stream=True)
            response.raise_for_status()
            
            # Check content type for safety
            content_type = response.headers.get('Content-Type', '').lower()
            safe_types = [
                'application/pdf', 'application/msword', 
                'application/vnd.openxmlformats-officedocument',
                'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
                'application/csv', 'text/csv', 'text/plain'
            ]
            
            if not any(s_type in content_type for s_type in safe_types):
                self.logger.warning(f"Skipping download of {url} due to unsafe content type: {content_type}")
                return
            
            # Save file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"Downloaded document to {filepath}")
            self.downloaded_documents.append(filepath)
            
            # Extract text from document if possible
            document_text = self.extract_text_from_document(filepath)
            
            # Create a text version of the document
            if document_text:
                text_filepath = os.path.splitext(filepath)[0] + ".txt"
                with open(text_filepath, 'w', encoding='utf-8') as f:
                    f.write(document_text)
                self.logger.info(f"Extracted text from document to {text_filepath}")
                
        except Exception as e:
            self.logger.error(f"Failed to download document {url}: {str(e)}")

    def extract_text_from_document(self, filepath):
        """
        Extract text from downloaded document
        
        Args:
            filepath: Path to document file
            
        Returns:
            Extracted text
        """
        try:
            file_lower = filepath.lower()
            
            if file_lower.endswith('.pdf'):
                text = extract_pdf_text(filepath, self.logger)
                
            elif file_lower.endswith('.docx'):
                text = extract_docx_text(filepath, self.logger)
                
            elif file_lower.endswith('.csv'):
                text = extract_csv_text(filepath, self.logger)
                
            elif file_lower.endswith(('.xls', '.xlsx')):
                text = extract_excel_text(filepath, self.logger)
                
            else:
                # For unsupported file types, just note that we can't extract
                self.logger.warning(f"Unsupported document type for text extraction: {filepath}")
                return ""
                
            # Clean extracted text
            return clean_text(text, self.nlp)
            
        except Exception as e:
            self.logger.error(f"Error extracting text from document {filepath}: {str(e)}")
            return ""

    def download_images(self, soup):
        """
        Download images from the webpage
        
        Args:
            soup: BeautifulSoup object
        """
        if not self.extract_images:
            return
            
        base_url = urllib.parse.urlparse(self.url).scheme + "://" + urllib.parse.urlparse(self.url).netloc
        
        try:
            # Process <img> tags
            for img_tag in soup.find_all('img', src=True):
                img_url = img_tag['src']
                
                # Handle relative URLs
                img_url = urllib.parse.urljoin(base_url, img_url)
                
                # Skip data URLs
                if img_url.startswith('data:'):
                    continue
                    
                # Validate before downloading
                if validate_url(img_url, extract_images=True, logger=self.logger):
                    self._download_image(img_url)
                    
            # Also look for images in CSS background
            for tag in soup.find_all(style=True):
                style = tag['style']
                # Basic CSS background image extraction
                if 'background-image' in style and 'url(' in style:
                    match = re.search(r'url\([\'"]?([^\'"]+)[\'"]?\)', style)
                    if match:
                        img_url = match.group(1)
                        img_url = urllib.parse.urljoin(base_url, img_url)
                        if validate_url(img_url, extract_images=True, logger=self.logger):
                            self._download_image(img_url)
                    
        except Exception as e:
            self.logger.error(f"Error downloading images: {str(e)}")

    def _download_image(self, url):
        """
        Download image from URL
        
        Args:
            url: Image URL
        """
        try:
            # Extract filename from URL
            filename = os.path.basename(urllib.parse.urlparse(url).path)
            
            # Create default name if empty
            if not filename or '.' not in filename:
                filename = "image_" + create_safe_folder_name(url) + ".jpg"
            
            # Create save path
            filepath = os.path.join(self.images_dir, filename)
            filepath = create_safe_filename(filepath)
            
            # Skip if already downloaded (checking by file path)
            if filepath in self.downloaded_images:
                return
                
            # Download image
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10, stream=True)
            response.raise_for_status()
            
            # Check content type for safety
            content_type = response.headers.get('Content-Type', '').lower()
            if not content_type.startswith('image/'):
                self.logger.warning(f"Skipping download of {url} - not an image: {content_type}")
                return
                
            # Save image
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"Downloaded image to {filepath}")
            self.downloaded_images.append(filepath)
            
            # Extract text from image if OCR is available
            if check_tesseract_available():
                image_text = extract_image_text(filepath, self.logger)
                
                # Save extracted text if not empty
                if image_text:
                    text_filepath = os.path.splitext(filepath)[0] + "_ocr.txt"
                    with open(text_filepath, 'w', encoding='utf-8') as f:
                        f.write(image_text)
                    self.logger.info(f"Extracted OCR text from image to {text_filepath}")
                    
        except Exception as e:
            self.logger.error(f"Failed to download image {url}: {str(e)}")

    def save_webpage_text(self, soup):
        """
        Extract and save text content from webpage
        
        Args:
            soup: BeautifulSoup object
        """
        if not self.extract_text:
            return
            
        try:
            # Get all text from the page
            text = soup.get_text(separator='\n')
            
            # Clean text
            clean_page_text = clean_text(text, self.nlp)
            
            # Save to file
            text_filepath = os.path.join(self.output_dir, 'webpage_text.txt')
            with open(text_filepath, 'w', encoding='utf-8') as f:
                f.write(clean_page_text)
                
            self.logger.info(f"Saved webpage text to {text_filepath}")
            self.text_content = clean_page_text
            
        except Exception as e:
            self.logger.error(f"Failed to save webpage text: {str(e)}")

    def scrape(self):
        """
        Main method to scrape the webpage
        
        Returns:
            List of results
        """
        results = []
        
        try:
            # Fetch webpage
            self.logger.info(f"Fetching {self.url}...")
            response = self.fetch_page_content()
            
            if not response:
                self.logger.error("Failed to fetch webpage")
                return ["Failed to fetch webpage"]
                
            # Parse HTML
            self.logger.info("Parsing webpage content...")
            self.soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            self.logger.info(f"Created output directory: {self.output_dir}")
            
            if not self.flat_structure:
                if self.extract_links:
                    os.makedirs(self.links_dir, exist_ok=True)
                if self.extract_documents:
                    os.makedirs(self.docs_dir, exist_ok=True)
                if self.extract_images:
                    os.makedirs(self.images_dir, exist_ok=True)
            
            # Process based on enabled features
            self.logger.info(f"Starting extraction with settings - Text: {self.extract_text}, Links: {self.extract_links}, Documents: {self.extract_documents}, Images: {self.extract_images}")
            
            # Extract and save text
            if self.extract_text:
                self.logger.info("Extracting webpage text...")
                self.save_webpage_text(self.soup)
                results.append(f"Extracted webpage text to {self.output_dir}/webpage_text.txt")
                
            # Extract and save links
            if self.extract_links:
                self.logger.info("Extracting links...")
                self.links = self.extract_links_from_soup(self.soup)
                self.save_links(self.links)
                results.append(f"Extracted {len(self.links)} links to {self.links_dir}/extracted_links.txt")
                
            # Download documents
            if self.extract_documents:
                self.logger.info("Downloading documents...")
                self.download_documents(self.soup)
                results.append(f"Downloaded {len(self.downloaded_documents)} documents to {self.docs_dir}")
                
            # Download images
            if self.extract_images:
                self.logger.info("Downloading images...")
                self.download_images(self.soup)
                results.append(f"Downloaded {len(self.downloaded_images)} images to {self.images_dir}")
                
            # Save HTML source
            source_path = os.path.join(self.output_dir, 'source.html')
            with open(source_path, 'wb') as f:
                f.write(response.content)
            results.append(f"Saved source HTML to {source_path}")
            
            self.logger.info("Scraping completed successfully")
            
        except Exception as e:
            error_message = f"Error during scraping: {str(e)}"
            self.logger.error(error_message)
            results.append(error_message)
            
        return results

    def get_text(self):
        """
        Get extracted text content
        
        Returns:
            Extracted text content
        """
        return self.text_content

    def get_links(self):
        """
        Get extracted links
        
        Returns:
            List of extracted links
        """
        return self.links

    def get_documents(self):
        """
        Get list of downloaded documents
        
        Returns:
            List of document file paths
        """
        return self.downloaded_documents

    def get_images(self):
        """
        Get list of downloaded images
        
        Returns:
            List of image file paths
        """
        return self.downloaded_images 