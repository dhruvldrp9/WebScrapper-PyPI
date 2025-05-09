# WebScraper-Plus

A versatile web scraping library for extracting content from websites with robust error handling and customizable output options.

## Features

- **Text Extraction**: Extract and clean text content from web pages
- **Link Extraction**: Find and save all hyperlinks from a page
- **Document Download**: Download documents like PDF, DOCX, CSV, Excel files
- **Document Text Extraction**: Extract text from downloaded documents
- **Image Download**: Download images from web pages
- **OCR Capability**: Extract text from images using Tesseract OCR
- **Configurable Output**: Choose between flat or nested directory structure
- **Robust Error Handling**: Gracefully handle network issues, missing dependencies, and more
- **Command-line Interface**: Easy to use CLI for quick scraping tasks
- **Python API**: Clean API for integration into your Python projects

## Installation

### Basic Installation

```bash
pip install webscraper-plus
```

### With Document Support

```bash
pip install webscraper-plus[pdf,docx,excel]
```

### With Image OCR Support

```bash
pip install webscraper-plus[ocr]
```

### Full Installation (All Features)

```bash
pip install webscraper-plus[all]
```

### Development Installation

```bash
pip install webscraper-plus[dev]
```

## Dependencies

- **Core**: requests, BeautifulSoup4, spacy, chardet
- **PDF**: PyPDF2
- **DOCX**: python-docx
- **Excel**: openpyxl
- **OCR**: pytesseract, Pillow (and Tesseract OCR system package)

### Tesseract OCR

For OCR functionality, you need to install Tesseract OCR on your system:

- **Windows**: Download installer from [GitHub](https://github.com/UB-Mannheim/tesseract/wiki)
- **macOS**: `brew install tesseract`
- **Ubuntu/Debian**: `sudo apt-get install tesseract-ocr`
- **Fedora/RHEL**: `sudo dnf install tesseract`

## Command-line Usage

```bash
# Basic usage (extracts all content types)
webscraper-plus --url https://example.com --output ./scraped_data

# Extract only text and links
webscraper-plus --url https://example.com --text --links

# Extract documents and images with flat structure
webscraper-plus --url https://example.com --docs --images --flat

# Extract all content types with verbose logging
webscraper-plus --url https://example.com --all --verbose
```

### Command-line Options

```
  -h, --help       show this help message and exit
  --version        show program's version number and exit
  --url URL        URL to scrape
  --output OUTPUT  Output directory for scraped content
  --text           Extract webpage text
  --links          Extract hyperlinks
  --docs           Download documents
  --images         Download images
  --flat           Use flat directory structure
  --all            Enable all extraction options
  --verbose        Enable verbose logging
```

## Python Usage

### Basic Usage

```python
from webscraper import WebScraper

# Create scraper (extracts everything)
scraper = WebScraper(
    url="https://example.com",
    base_output_dir="./scraped_data",
    extract_text=True,
    extract_links=True,
    extract_documents=True,
    extract_images=True
)

# Run scraper and get results
results = scraper.scrape()
print(results)
```

### Selective Extraction

```python
# Create scraper with selective extraction
scraper = WebScraper(
    url="https://example.com",
    extract_text=True,     # Only extract text
    extract_links=False,
    extract_documents=False,
    extract_images=False,
    flat_structure=True    # Use flat directory structure
)

# Run scraper
results = scraper.scrape()
```

## Output Structure

By default, WebScraper-Plus uses a nested directory structure:

```
output_directory/
├── domain_timestamp/
│   ├── webpage_text.txt
│   ├── links/
│   │   └── links.txt
│   ├── documents/
│   │   ├── document1.pdf
│   │   ├── document1.txt (extracted text)
│   │   └── ...
│   └── images/
│       ├── image1.jpg
│       ├── image1.txt (OCR text)
│       └── ...
```

With `flat_structure=True`, all files are saved in a single directory with prefixes:

```
output_directory/
├── domain_timestamp/
│   ├── webpage_text_main.txt
│   ├── links_extracted.txt
│   ├── doc_document1.pdf
│   ├── doc_document1.txt
│   ├── img_image1.jpg
│   ├── img_ocr_image1.txt
│   └── ...
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 

## Examples

The package includes several examples to help you get started:

### Basic Usage Example

The `examples/basic_usage.py` script demonstrates how to use WebScraper with all extraction options enabled:

```python
from webscraper import WebScraper

# Create WebScraper instance with all extraction options enabled
scraper = WebScraper(
    url="https://en.wikipedia.org/wiki/Web_scraping",
    base_output_dir="./scraped_output",
    extract_text=True,
    extract_links=True,
    extract_documents=True,
    extract_images=True
)

# Execute scraping
results = scraper.scrape()
```

### Selective Scraping Example

The `examples/selective_scraping.py` script shows how to use WebScraper with specific extraction options:

```python
# Example 1: Extract only text and links with flat directory structure
text_links_scraper = WebScraper(
    url="https://www.python.org",
    base_output_dir="./python_org_output",
    extract_text=True,
    extract_links=True,
    extract_documents=False,
    extract_images=False,
    flat_structure=True
)

# Example 2: Extract only images with nested directory structure
images_scraper = WebScraper(
    url="https://www.python.org",
    base_output_dir="./python_org_output",
    extract_text=False,
    extract_links=False,
    extract_documents=False,
    extract_images=True,
    flat_structure=False
)
```

### Running Examples

To run the examples, clone the repository and run:

```bash
# Basic usage example
python examples/basic_usage.py

# Selective scraping example
python examples/selective_scraping.py
``` 