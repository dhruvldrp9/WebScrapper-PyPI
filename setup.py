from setuptools import setup, find_packages
import re

with open("webscraper/__init__.py", "r") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="webscraper-plus",
    version=version,
    author="Dhruvkumar Patel",
    author_email="dhruv.ldrp9@gmail.com",
    description="A versatile web scraping library for extracting content from websites",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dhruvldrp9/WebScrapper-PyPI",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing :: Markup :: HTML",
    ],
    python_requires=">=3.7",
    install_requires=[
        "requests>=2.25.0",
        "beautifulsoup4>=4.9.0",
        "spacy>=3.0.0",
        "chardet>=4.0.0",
    ],
    extras_require={
        "pdf": ["PyPDF2>=2.0.0"],
        "docx": ["python-docx>=0.8.11"],
        "excel": ["openpyxl>=3.0.7"],
        "ocr": ["pytesseract>=0.3.8", "Pillow>=8.0.0"],
        "all": [
            "PyPDF2>=2.0.0",
            "python-docx>=0.8.11",
            "openpyxl>=3.0.7",
            "pytesseract>=0.3.8",
            "Pillow>=8.0.0",
        ],
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.12.0",
            "black>=21.5b2",
            "flake8>=3.9.0",
            "isort>=5.9.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "webscraper-plus=webscraper.cli:main",
        ],
    },
) 