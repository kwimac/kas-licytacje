from io import BytesIO
import logging
from kas.url import KAS_URL

import httpx
import httpx_retries
import pymupdf
from lxml import etree, html
from tqdm.asyncio import tqdm_asyncio
import asyncio

import re
from collections import defaultdict
from typing import AsyncGenerator, ClassVar, Generator
from urllib.parse import urljoin

logger = logging.getLogger()

AuctionData = dict[str, str]


class AuctionCrawler:
    url: KAS_URL
    _client: httpx.AsyncClient
    _FILE_FIELDS: ClassVar[list[str]] = (
        "Określenie ruchomości",
        "Wartość szacunkowa",
        "Cena wywołania",
        "Uwagi",
    )
    FIELDS: ClassVar[tuple[str]] = (
        "auction_url",
        "pdf_url",
        *_FILE_FIELDS,
    )

    def __init__(self, url: KAS_URL) -> None:
        self.url = url
        self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Client not initialized")
        return self._client

    @staticmethod
    def build_client() -> httpx.AsyncClient:
        transport = httpx_retries.RetryTransport(
            httpx_retries.Retry(
                total=3,
                backoff_factor=0.5,
            ),
        )
        return httpx.AsyncClient(
            timeout=3 * 60,
            # transport=transport,
        )

    async def collect_data(self) -> list[AuctionData]:
        async with self.build_client() as client:
            self._client = client
            sem = asyncio.Semaphore(3)
            tasks = [
                self.collect_single_auction(auction_url, sem)
                async for auction_url in self._get_auction_urls()
            ]
            return await tqdm_asyncio.gather(*tasks, total=len(tasks))

    async def _get_auction_urls(self) -> AsyncGenerator[str, None]:
        page_url = self.url.get_listing_url(page=1)
        tree = await self._get_page_tree(page_url)
        pages_num = self._get_pages_num(tree)
        for auction_url in self._get_page_auctions_url(tree):
            yield auction_url
        for page_num in range(2, pages_num):
            page_url = self.url.get_listing_url(page=page_num)
            tree = await self._get_page_tree(page_url)
            for auction_url in self._get_page_auctions_url(tree):
                yield auction_url

    async def _get_page_tree(self, page_url: str) -> html.Element:
        response = await self.client.get(page_url)
        response.raise_for_status()
        page_content = response.text
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

    async def collect_single_auction(
        self,
        auction_url: str,
        semaphore: asyncio.Semaphore,
    ) -> AuctionData:
        async with semaphore:
            result = {
                "auction_url": auction_url,
            }
            try:
                pdf_url = await self._get_pdf_url(auction_url)
                result["pdf_url"] = pdf_url
                bytes_pdf = await self._get_file(pdf_url)
                if bytes_pdf is not None:
                    doc = pymupdf.open(stream=bytes_pdf)
                    result.update(**self._parse_tables(doc[0]))
            except IndexError as exc:
                # logger.error(exc)
                ...
            return result

    async def _get_pdf_url(self, auction_url: str) -> str:
        tree = await self._get_page_tree(auction_url)
        link_elem = tree.xpath('.//a[contains(text(),"amoch")]')[0]
        pdf_path = link_elem.attrib["href"]
        return urljoin("https://" + self.url.netloc, pdf_path)

    async def _get_file(self, url: str) -> BytesIO | None:
        response = await self.client.get(url)
        if not response.is_success:
            return None
        byte_stream = BytesIO()
        byte_stream.write(response.content)
        byte_stream.seek(0)
        return byte_stream

    @classmethod
    def _parse_tables(cls, page: pymupdf.Page) -> AuctionData:
        try:
            result = defaultdict(list)
            for table in page.find_tables():
                for key, val in zip(*table.extract()):
                    key = key.replace("\n", " ").strip()
                    if key in cls._FILE_FIELDS:
                        result[key].append(val.replace("\n", " ").strip())
            return {key: " ".join(val) for key, val in result.items()}
        except ValueError:
            return {}
