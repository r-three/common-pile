import re
import requests
from bs4 import BeautifulSoup, NavigableString
from usp.tree import sitemap_tree_for_homepage

def build_url_index(base_url, keyword=None):

    if keyword is None:
        keyword = [""]
    elif isinstance(keyword, str):
        keyword = [keyword]

    tree = sitemap_tree_for_homepage(base_url)
    page_index = [page.url for key in keyword for idx, page in enumerate(tree.all_pages()) if key in page.url]
    return page_index


def get_text_from_page(url=None, html_path=None, tag="article", attrs=None):

    assert bool(url is not None) != bool(html_path is not None)

    if url is not None:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        page = requests.get(url, headers=headers)
        soup = BeautifulSoup(page.content, "html.parser")
    elif html_path is not None:
        with open(html_path, "rb") as fp:
            soup = BeautifulSoup(fp, "html.parser")

    text = [soup.title.getText() if soup.title else ""]

    # Search for dateline
    if dateline := soup.find("time", class_=re.compile("title")):
        text.append(dateline.getText().strip())
    elif dateline := soup.find("span", class_=re.compile("date")):
        text.append(dateline.getText().strip())
    elif dateline := soup.find("span", class_=re.compile("on")):
        text.append(dateline.getText().strip())
    elif dateline := soup.find("li", class_=re.compile("time")):
        text.append(dateline.getText().strip())

    if byline := soup.find(class_=re.compile("authors")):
        text.append(byline.getText().strip())
    elif byline := soup.find(class_=re.compile("byline")):
        text.append(byline.getText().strip())
    elif dateline := soup.find(class_=re.compile("by")):
        text.append(dateline.getText().strip())

    # article = soup.find_all(tag, attrs=attrs)
    article = soup.find_all(tag, attrs={k:re.compile(v) for k,v in attrs.items()})
    for a in article:

        if dateline is None:
            if dateline := a.find("time", class_="timestamp"):
                text.append(dateline.getText(strip=True))
            elif dateline := a.find("div", class_="timestamps"):
                text.append(dateline.getText(strip=True))    
            elif dateline := a.find("time"):
                text.append(dateline.getText(strip=True))

        # Adapted from 
        # https://github.com/bltlab/mot/blob/63ef942f2a4cc7fff5823b4cdefbccc5c7464b5f/extraction/extracttext.py#L540-L558
        p_tag = a.find_all("p")
        for p in p_tag:
            # split_p = p.getText().split("\n")
            split_p = []
            text_pieces = []
            for child in p.children:
                if type(child) is NavigableString:
                    text_pieces.extend(child.split("\n"))
                elif child.name == "br":
                    split_p.append("".join(text_pieces))
                    text_pieces = []
                elif child.name == "em":
                    text_pieces.extend(child.getText())
                elif child.name == "a":
                    text_pieces.extend(child.getText())
                elif child.name == "i":
                    text_pieces.extend(child.getText())
                elif child.name == "strong":
                    text_pieces.extend(child.getText())
                elif child.name == "span":
                    text_pieces.extend(child.getText())

            # Remaining pieces
            if text_pieces:
                split_p.append("".join(text_pieces))
            text_article = [
                article_paragraph
                for s in split_p
                if is_valid(article_paragraph := s.strip()) and s.strip()
            ]
            # text_article = [text_string for text_string in text_article if text_string not in text]
            text.extend(text_article)

    return "\n".join(text)

def is_valid(text: str) -> bool:
    """
    Simple check to eliminate and filter obviously bad text in paragraph tags.
    """
    text = text.strip()
    text = " ".join(text.split())
    if not text:
        return False
    elif text.startswith("Attention Required! | Cloudflare"):
        return False
    elif text.startswith("403 Forbidden"):
        return False
    else:
        return True

def sanitize_url(url: str) -> str:
    """Remove parts of url string we don't want or can't use as a filename"""
    base = (
        url.replace("?", "_")
        .replace(",", "_")
        .replace("=", "_")
        .replace("https://www.", "")
        .replace("http://www.", "")
        .replace("https://", "")
        .replace("/", "_")
    )
    return re.sub(r"\s+", "_", base)