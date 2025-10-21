import logging
from kas.requests import get_file, get_page
from kas.url import KAS_URL


import pymupdf
from lxml import etree, html


import re
from collections import defaultdict
from typing import ClassVar, Generator
from urllib.parse import urljoin

logger = logging.getLogger()

AuctionData = dict[str, str]


class AuctionCrawler:
    url: KAS_URL
    _FILE_FIELDS: ClassVar[list[str]] = (
        "Określenie ruchomości",
        "Wartość szacunkowa",
        "Cena wywołania",
        "Uwagi",
    )
    FIELDS: ClassVar[tuple[str]] = (
        "url",
        *_FILE_FIELDS,
    )

    def __init__(self, url: KAS_URL) -> None:
        self.url = url

    def collect_data(self) -> Generator[AuctionData, None, None]:
        for auction_url in self._get_auction_urls():
            try:
                yield self.collect_single_auction(auction_url)
            except ValueError:
                
                yield 

    def _get_auction_urls(self) -> Generator[str, None, None]:
        counter = 0
        page_url = self.url.get_listing_url(page=1)
        tree = self._get_page_tree(page_url)
        pages_num = self._get_pages_num(tree)
        for auction_url in self._get_page_auctions_url(tree):
            counter += 1
            logger.info("%d URL: %s", counter, auction_url)
            yield auction_url
        for page_num in range(2, pages_num):
            page_url = self.url.get_listing_url(page=page_num)
            tree = self._get_page_tree(page_url)
            for auction_url in self._get_page_auctions_url(tree):
                counter += 1
                logger.info("%d URL: %s", counter, auction_url)
                yield auction_url

    @staticmethod
    def _get_page_tree(page_url: str) -> html.Element:
        page_content = get_page(page_url)
        parser = etree.HTMLParser()
        tree = etree.fromstring(page_content, parser)
        return tree

    @staticmethod
    def _get_pages_num(tree: html.Element) -> int:
        pages_num = 1
        for elem in tree.cssselect("li.page-links-option"):
            for sub_elem in elem.cssselect("a"):
                match_ = re.search(r"\d+", sub_elem.attrib["title"])
                if match_ and (new_max := int(match_.group())) > pages_num:
                    pages_num = new_max
        return pages_num

    @staticmethod
    def _get_page_auctions_url(tree: html.Element) -> Generator[str, None, None]:
        for elem in tree.cssselect("div.article-summary"):
            for link_elem in elem.xpath('.//a[contains(text(),"amoch")]'):
                yield link_elem.attrib["href"]

    def collect_single_auction(self, auction_url: str) -> AuctionData:
        pdf_url = self._get_pdf_url(auction_url)
        bytes_pdf = get_file(pdf_url)
        doc = pymupdf.open(stream=bytes_pdf)
        result = self._parse_tables(doc[0])
        result["url"] = auction_url
        return result

    def _get_pdf_url(self, auction_url: str) -> str:
        tree = self._get_page_tree(auction_url)
        link_elem = tree.xpath('.//a[contains(text(),"amoch")]')[0]
        pdf_path = link_elem.attrib["href"]
        return urljoin("https://" + self.url.netloc, pdf_path)

    @classmethod
    def _parse_tables(cls, page: pymupdf.Page) -> AuctionData:
        result = defaultdict(list)

            for table in page.find_tables():
                for key, val in zip(*table.extract()):
                    key = key.replace("\n", " ").strip()
                    if key in cls._FILE_FIELDS:
                        result[key].append(val.replace("\n", " ").strip())
        return {key: " ".join(val) for key, val in result.items()}
