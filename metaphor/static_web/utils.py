from bs4 import BeautifulSoup
from bs4.element import Comment


def text_from_HTML(html_content: str) -> str:
    """
    Extracts and returns visible text from given HTML content as a single string.
    """

    def filter_visible(el):
        if el.parent.name in [
            "style",
            "script",
            "head",
            "title",
            "meta",
            "[document]",
        ]:
            return False
        elif isinstance(el, Comment):
            return False
        else:
            return True

    # Use bs4 to find visible text elements
    soup = BeautifulSoup(html_content, "lxml")
    visible_text = filter(filter_visible, soup.findAll(string=True))
    return "\n".join(t.strip() for t in visible_text)


def title_from_HTML(html_content: str) -> str:
    """
    Extracts the title of a webpage given HTML content as a single string.
    Designed to handle output from get_page_HTML.
    """

    soup = BeautifulSoup(html_content, "lxml")
    title_tag = soup.find("title")

    if title_tag:
        return title_tag.text

    else:
        return ""
