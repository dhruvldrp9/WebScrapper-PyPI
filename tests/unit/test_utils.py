"""
Unit tests for WebScraper utility functions
"""

import os
import unittest
import tempfile
from webscraper.utils import create_safe_folder_name, create_safe_filename, clean_text

class TestUtils(unittest.TestCase):
    """Test cases for WebScraper utility functions"""

    def test_create_safe_folder_name(self):
        """Test creating safe folder names from URLs"""
        # Test basic URL
        url = "https://example.com"
        folder_name = create_safe_folder_name(url)
        self.assertTrue(folder_name.startswith("examplecom_"))
        self.assertFalse('/' in folder_name)
        self.assertFalse(':' in folder_name)
        
        # Test URL with path
        url = "https://example.com/path/to/page"
        folder_name = create_safe_folder_name(url)
        self.assertTrue(folder_name.startswith("examplecompathtopage"))
        
        # Test URL with special characters
        url = "https://example.com/?query=test&param=value"
        folder_name = create_safe_folder_name(url)
        self.assertTrue(folder_name.startswith("examplecomquerytest"))

    def test_create_safe_filename(self):
        """Test creating safe filenames"""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tempdir:
            # Test basic filename
            original_path = os.path.join(tempdir, "test:file.txt")
            safe_path = create_safe_filename(original_path)
            self.assertEqual(os.path.dirname(safe_path), tempdir)
            self.assertEqual(os.path.basename(safe_path), "test_file.txt")
            
            # Test filename with invalid characters
            original_path = os.path.join(tempdir, "file*with?invalid<chars>.txt")
            safe_path = create_safe_filename(original_path)
            self.assertFalse('*' in safe_path)
            self.assertFalse('?' in safe_path)
            self.assertFalse('<' in safe_path)
            self.assertFalse('>' in safe_path)
            
            # Test filename uniqueness
            test_file = os.path.join(tempdir, "existing.txt")
            with open(test_file, 'w') as f:
                f.write("existing file")
                
            safe_path = create_safe_filename(test_file)
            self.assertNotEqual(safe_path, test_file)
            self.assertTrue("existing_" in safe_path)

    def test_clean_text(self):
        """Test text cleaning functionality"""
        # Test basic cleaning without spaCy
        text = "  This   is a  test  with   extra   spaces.  "
        cleaned = clean_text(text)
        self.assertEqual(cleaned, "This is a test with extra spaces.")
        
        # Test HTML cleanup
        text = "<p>This is <b>HTML</b> text with <script>some script</script> tags.</p>"
        cleaned = clean_text(text)
        self.assertNotIn("<p>", cleaned)
        self.assertNotIn("<b>", cleaned)
        self.assertNotIn("script", cleaned)
        self.assertIn("This is HTML text with tags", cleaned)
        
        # Test empty text
        self.assertEqual(clean_text(""), "")
        self.assertEqual(clean_text(None), "")

if __name__ == "__main__":
    unittest.main() 