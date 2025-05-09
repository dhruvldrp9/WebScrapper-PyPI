"""
Utility functions for WebScraper
"""

import os
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def create_safe_folder_name(url):
    """
    Create a safe, unique folder name from URL
    
    Args:
        url: Target URL
        
    Returns:
        Safe folder name
    """
    safe_name = url.replace('https://', '').replace('http://', '')
    safe_name = "".join(c for c in safe_name if c.isalnum() or c in ['-', '_'])
    
    # Add timestamp (using ISO format for readability and filesystem compatibility)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Combine safe name and timestamp
    full_safe_name = f"{safe_name[:20]}_{timestamp}"
    return full_safe_name

def create_safe_filename(original_filename):
    """
    Create a safe filename by removing invalid characters and ensuring uniqueness
    
    Args:
        original_filename: Original filename path
        
    Returns:
        Safe filename path
    """
    directory = os.path.dirname(original_filename)
    filename = os.path.basename(original_filename)
    
    # Remove invalid characters
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', filename)
    
    # Ensure the name isn't too long
    if len(safe_name) > 100:
        name, ext = os.path.splitext(safe_name)
        safe_name = name[:95] + ext
        
    # Make sure we have a unique filename
    counter = 1
    final_path = os.path.join(directory, safe_name)
    
    while os.path.exists(final_path):
        name, ext = os.path.splitext(safe_name)
        safe_name = f"{name}_{counter}{ext}"
        final_path = os.path.join(directory, safe_name)
        counter += 1
        
    return final_path

def clean_text(text, nlp=None):
    """
    Clean extracted text using spaCy if available, otherwise use basic regex
    
    Args:
        text: Input text to clean
        nlp: Optional spaCy NLP model
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
        
    # Remove HTML tags
    text = re.sub(r'<.*?>', '', text)
    
    # Process with spaCy if available
    if nlp is not None:
        try:
            doc = nlp(text)
            cleaned_text = ' '.join([token.text for token in doc if not token.is_space])
            return cleaned_text
        except Exception as e:
            logger.warning(f"spaCy processing failed: {str(e)}. Using basic cleaning.")
            # Fall back to basic cleaning
    
    # Basic cleaning if spaCy is not available or failed
    # Remove extra whitespace
    cleaned_text = re.sub(r'\s+', ' ', text)
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

def validate_url(url, extract_documents=False, extract_images=False, logger=None):
    """
    Validate if a URL is safe to download from
    
    Args:
        url: URL to validate
        extract_documents: Whether documents will be extracted
        extract_images: Whether images will be extracted
        logger: Logger instance
        
    Returns:
        bool: True if URL is valid and safe, False otherwise
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        
    try:
        import urllib.parse
        import requests
        
        # Basic validation
        parsed_url = urllib.parse.urlparse(url)
        
        # Check if scheme is http or https
        if parsed_url.scheme not in ('http', 'https'):
            logger.warning(f"Invalid URL scheme: {url}")
            return False
            
        # Check for IP address to avoid local network access
        import re
        hostname = parsed_url.netloc.split(':')[0]
        is_ip = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", hostname)
        
        if is_ip:
            try:
                ip_parts = [int(part) for part in hostname.split('.')]
                # Check for private IP ranges
                if (ip_parts[0] == 10 or  # 10.0.0.0/8
                    (ip_parts[0] == 172 and 16 <= ip_parts[1] <= 31) or  # 172.16.0.0/12
                    (ip_parts[0] == 192 and ip_parts[1] == 168) or  # 192.168.0.0/16
                    ip_parts[0] == 127):  # 127.0.0.0/8 (localhost)
                    logger.warning(f"Private IP address detected, skipping: {url}")
                    return False
            except:
                pass
                
        # Check content type for document and image downloads
        if extract_documents or extract_images:
            try:
                # Send HEAD request to check content type
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)
                
                # Get content type
                content_type = response.headers.get('Content-Type', '').lower()
                
                # Validate content type for safety
                safe_types = [
                    'text/', 'image/', 'application/pdf', 'application/msword',
                    'application/vnd.openxmlformats-officedocument',
                    'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
                    'application/csv', 'text/csv'
                ]
                
                if not any(safe_type in content_type for safe_type in safe_types):
                    logger.warning(f"Unsafe content type: {content_type} for URL: {url}")
                    return False
                    
                # Check file size
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB
                    logger.warning(f"File too large ({int(content_length) / (1024*1024):.2f} MB): {url}")
                    return False
                    
            except requests.RequestException:
                # If HEAD request fails, we'll try GET when actually downloading
                pass
                
        return True
        
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {str(e)}")
        return False 