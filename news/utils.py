import requests
from bs4 import BeautifulSoup
from usp.tree import sitemap_tree_for_homepage

def build_url_index(base_url, keyword=None):

    if keyword is None:
        keyword = [""]
    elif isinstance(keyword, str):
        keyword = [keyword]

    tree = sitemap_tree_for_homepage(base_url)
    page_index = [page.url for key in keyword for idx, page in enumerate(tree.all_pages()) if key in page.url]
    return page_index


ignore = "ProPublica is a nonprofit newsroom that investigates abuses of power. Sign up to receive"

# Democracy Now
# article = soup.find_all("div", attrs={"class": "text"})

# Mongabay
# article = soup.find_all("div", attrs={"id":"main"})

# Tasnim Agency
# article = soup.find_all("div", attrs={"class":"row"})

# The Conversation
# article = soup.find_all("article", attrs={"id":"article"})


def get_text_from_page(url=None, html_path=None, tag="article", attrs={"class": "article center"}):

    assert bool(url is not None) != bool(html_path is not None)

    if url is not None:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
    elif html_path is not None:
        with open(url) as fp:
            soup = BeautifulSoup(fp, "html.parser")

    text = [soup.title.getText() if soup.title else ""]

    # Search for dateline
    if dateline := soup.find("span", class_="date"):
        text.append(dateline.getText(strip=True))
    elif dateline := soup.find("li", class_="time"):
        text.append(dateline.getText(strip=True))

    # article = soup.find_all("div", attrs={"class": "article-body"})
    # article = soup.find_all("div", attrs={"class": "text"})
    article = soup.find_all(tag, attrs=attrs)
    for a in article:

        if dateline is None:
            if dateline := a.find("time", class_="timestamp"):
                text.append(dateline.getText(strip=True))
            elif dateline := a.find("div", class_="timestamps"):
                text.append(dateline.getText(strip=True))    

        if byline := a.find("span", class_="article-meta-1__byline"):
            text.append(byline.getText().strip())
        # elif byline := a.find("a", attrs{"itemprop": "author"}):
        #     text.append(byline.getText().strip())

        if lead_text := a.find("h3", class_="lead"):
            text.append(lead_text.getText().strip())

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
                elif child.name == "strong":
                    text_pieces.extend(child.getText())

            # Remaining pieces
            if text_pieces:
                split_p.append("".join(text_pieces))
            text_article = [
                article_paragraph
                for s in split_p
                if is_valid(article_paragraph := s.strip()) and s.strip()
            ]
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