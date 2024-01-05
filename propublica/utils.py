import requests
from bs4 import BeautifulSoup
from usp.tree import sitemap_tree_for_homepage

def build_url_index(base_url, keyword=None):

    if keyword is None:
        keyword = [""]
    elif isinstance(keyword, str):
        keyword = [keyword]

    tree = sitemap_tree_for_homepage(base_url)
    page_index = [(idx, page) for key in keyword for page in enumerate(tree.all_pages()) if key in page.url]
    return page_index


def get_text_from_page(url, tag="p"):
    page = requests.get(url)
    soup = BeautifulSoup(page.content)

    return "\n".join([element.text for soup.find_all(tag)])