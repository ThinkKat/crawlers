import pytest
from unittest.mock import patch
from src.parser import Parser

@pytest.fixture
def parser():
    """A fixture to create a Parser instance with mocked dependencies."""
    # Mock dependencies that have side effects (env vars, db connection)
    with patch('src.parser.load_dotenv'):
        with patch('src.parser.sqlite3.connect'):
            p = Parser()
            yield p

def test_extract_links_simple(parser):
    """
    Tests basic link extraction from a simple HTML string.
    It should find both absolute and relative links and resolve them.
    """
    html = """
    <html>
        <body>
            <a href="https://example.com/page1">Page 1</a>
            <a href="/page2">Page 2</a>
            <a href="page3">Page 3</a>
        </body>
    </html>
    """
    base_url = "https://another.com/path/"
    urls = parser.extract_links(html, base_url)
    # Using a set for comparison to ignore order
    expected_urls = {
        "https://example.com/page1",
        "https://another.com/page2",
        "https://another.com/path/page3"
    }
    print(set(urls))
    print(expected_urls)
    assert set(urls) == expected_urls

def test_extract_links_no_links(parser):
    """
    Tests HTML with no anchor tags.
    """
    html = "<html><body><p>No links here.</p></body></html>"
    base_url = "https://example.com"
    urls = parser.extract_links(html, base_url)
    assert len(urls) == 0

def test_extract_links_skip(parser):
    """
    Tests the 'skip' functionality in parse_meta, which should prevent any link extraction.
    """
    html = '<a href="https://example.com/page1">Page 1</a>'
    base_url = "https://example.com"
    parse_meta = {"skip": True}
    urls = parser.extract_links(html, base_url, parse_meta)
    
    assert len(urls) == 0

def test_extract_links_with_selector(parser):
    """
    Tests link extraction using a specific CSS class selector.
    Only links within the element with the specified class should be extracted.
    """
    html = """
    <div class="header">
        <a href="/ignored_link">Ignored</a>
    </div>
    <div class="content">
        <a href="/link1">Link 1</a>
        <a href="https://example.com/link2">Link 2</a>
    </div>
    <div class="footer">
        <a href="/another_ignored_link">Ignored Again</a>
    </div>
    """
    base_url = "https://test.com"
    parse_meta = {
        "selector": {
            "class": "content"
        }
    }
    urls = parser.extract_links(html, base_url, parse_meta)
    
    expected_urls = {
        "https://test.com/link1",
        "https://example.com/link2"
    }
    assert set(urls) == expected_urls

def test_extract_links_duplicates(parser):
    """
    Tests that duplicate links are handled correctly (returns only unique links).
    """
    html = """
    <a href="/page1">Page 1</a>
    <a href="/page1">Page 1 again</a>
    <a href="https://example.com/page1">Absolute Page 1</a>
    """
    base_url = "https://example.com"
    urls = parser.extract_links(html, base_url)
    
    expected_urls = {
        "https://example.com/page1"
    }
    assert set(urls) == expected_urls
