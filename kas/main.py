from collections import defaultdict
from io import BytesIO
import re
from typing import Generator
import requests
from kas.url import MAZ_URL, KAS_URL
from tenacity import retry, wait_exponential
from lxml import etree, html
from urllib.parse import urljoin
import pymupdf

@retry(wait=wait_exponential(multiplier=2, min=1, max=10))
def get_page(url: str) -> str:
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    return response.text

@retry(wait=wait_exponential(multiplier=2, min=1, max=10))
def get_file(url: str) -> BytesIO:
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    byte_stream = BytesIO()
    byte_stream.write(response.content)
    byte_stream.seek(0)
    return byte_stream

def _get_pages_num(tree: html.Element) -> int:
    pages_num = 1
    for elem in tree.cssselect("li.page-links-option"):
        for sub_elem in elem.cssselect("a"):
            match_ = re.search(r"\d+", sub_elem.attrib["title"])
            if match_ and (new_max := int(match_.group())) > pages_num:
                pages_num = new_max
    return pages_num


def _get_page_tree(page_url) -> html.Element:
    page_content = get_page(page_url)
    parser = etree.HTMLParser()
    tree = etree.fromstring(page_content, parser)
    return tree

def _get_auction_pdf_path(url: str) -> str:
    tree = _get_page_tree(url)
    link_elem = tree.xpath('.//a[contains(text(),"amoch")]')[0]
    return link_elem.attrib["href"]

def _get_page_auctions_url(tree: html.Element) -> Generator[str, None, None]:
    for elem in tree.cssselect("div.article-summary"):
        for link_elem in elem.xpath('.//a[contains(text(),"amoch")]'):
            yield link_elem.attrib["href"]
            

def _get_auctions_url(url: KAS_URL) -> Generator[str, None, None]:
    page_url = url.get_listing_url(page=1)
    tree = _get_page_tree(page_url)
    pages_num = _get_pages_num(tree)
    for auction_url in _get_page_auctions_url(tree):
        yield auction_url
    for page_num in range(2, pages_num):
        page_url = url.get_listing_url(page=page_num)
        tree = _get_page_tree(page_url)
        for auction_url in _get_page_auctions_url(tree):
            yield auction_url

def _get_pdf_url(kas_url: KAS_URL, auction_url: str) -> str:
    pdf_path = _get_auction_pdf_path(auction_url)
    return urljoin("https://" + kas_url.netloc, pdf_path)

def collect_data(kas_url: KAS_URL, auction_url: str):
    pdf_url = _get_pdf_url(kas_url, auction_url)
    bytes_pdf = get_file(pdf_url)
    doc = pymupdf.open(stream=bytes_pdf)
    page = doc[0]

def _parse_tables(page: pymupdf.Page) -> dict[str, str]:
    result = defaultdict(list)
    for tab in page.find_tables():
        for table in tab.tables:
            for key, val in zip(*table.extract()):
                key = key.replace("\n", " ").strip()
                if key in ["Określenie ruchomości", "Wartość szacunkowa", "Cena wywołania", "Uwagi"]:
                    result[key].append(val.replace("\n", " ").strip())
    return {
        key: " ".join(val)
        for key, val in result
    }

def main() -> None:
    ...


if __name__ == "__main__":
    main()
