"""
Image processing utilities for WebScraper

This module provides functions for extracting text from images using OCR.
"""

import os
import logging

logger = logging.getLogger(__name__)

def extract_image_text(filepath, logger=None):
    """
    Extract text from an image using OCR with robust error handling
    
    Args:
        filepath: Path to the image file
        logger: Logger instance
        
    Returns:
        Extracted text or empty string if extraction fails
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        
    try:
        # Check if file exists and is accessible
        if not os.path.exists(filepath):
            logger.error(f"Image file not found: {filepath}")
            return ""
            
        # Check file size to avoid processing very large images
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if file_size_mb > 10:  # 10MB limit
            logger.warning(f"Image file too large ({file_size_mb:.2f} MB), skipping OCR: {filepath}")
            return ""
        
        # Import pytesseract with error handling
        try:
            import pytesseract
            from PIL import Image, UnidentifiedImageError
        except ImportError as e:
            logger.error(f"Required library not found: {str(e)}. OCR processing skipped.")
            return ""
            
        try:
            # Open image with PIL
            image = Image.open(filepath)
            
            # Perform OCR
            text = pytesseract.image_to_string(image)
            
            # Return extracted text
            return text.strip()
                
        except UnidentifiedImageError:
            logger.warning(f"Could not identify image format: {filepath}")
            return ""
        except pytesseract.TesseractNotFoundError:
            logger.error("Tesseract OCR not installed or not in PATH")
            logger.error("Please install Tesseract OCR: https://github.com/tesseract-ocr/tesseract")
            return ""
        except Exception as e:
            logger.error(f"Error in OCR processing for {filepath}: {str(e)}")
            return ""
            
    except Exception as e:
        logger.error(f"Error extracting text from image {filepath}: {str(e)}")
        return ""

def check_tesseract_available():
    """
    Check if Tesseract OCR is available
    
    Returns:
        bool: True if Tesseract is available, False otherwise
    """
    try:
        import pytesseract
        version = pytesseract.get_tesseract_version()
        return True
    except (ImportError, Exception):
        return False 