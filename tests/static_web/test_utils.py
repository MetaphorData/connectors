from metaphor.static_web.utils import text_from_HTML, title_from_HTML
from tests.test_utils import load_text


# Test for extracting visible text from HTML, with filtering
def test_text_from_HTML(test_root_dir: str):
    html_content = load_text(f"{test_root_dir}/common/samples/titles_text.html")
    text = text_from_HTML(html_content)
    assert "Visible paragraph 1." in text
    assert "Visible paragraph 2." in text
    assert "Test Title" not in text
    assert "Some style" not in text
    assert "Some script" not in text
    assert "Some meta" not in text
    assert "Commented text" not in text
    assert "Script text" not in text
    assert "Style text" not in text


# Test for extracting visible text from HTML, with filtering
def test_get_text_from_HTML_with_filtering(test_root_dir: str):
    html_content = load_text(
        f"{test_root_dir}/static_web/sample_pages/titles_text.html"
    )
    text = text_from_HTML(html_content)
    assert "Visible paragraph 1." in text
    assert "Visible paragraph 2." in text
    assert "Test Title" not in text
    assert "Some style" not in text
    assert "Some script" not in text
    assert "Some meta" not in text
    assert "Commented text" not in text
    assert "Script text" not in text
    assert "Style text" not in text


# Test for extracting title from HTML
def test_get_title_from_HTML_success(test_root_dir: str):
    html_content = "<html><head><title>Test Title</title></head></html>"
    title = title_from_HTML(html_content)
    assert title == "Test Title"


# Test for extracting empty title
def test_get_title_from_HTML_failure(test_root_dir: str):
    html_content = "<html><head></head><body><h1>Hello World!</h1></body></html>"
    title = title_from_HTML(html_content)
    assert title == ""
