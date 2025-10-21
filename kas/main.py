import csv
import logging
from pathlib import Path
import sys
from kas.crawler import AuctionCrawler
from kas.url import KAS_URL, MAZ_URL, KUJ_POM_URL

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))

def collect_voivodeship_auctions(url: KAS_URL, dir_path: Path = Path(".")) -> None:
    path = dir_path / f"{url.voivodship}.csv"
    logger.info("Collecting data for: %s", url.voivodship)
    with path.open("w", encoding="utf-8") as _file:
        writer = csv.DictWriter(_file, fieldnames=AuctionCrawler.FIELDS)
        writer.writeheader()
        writer.writerows(AuctionCrawler(url).collect_data())


def main() -> None:
    collect_voivodeship_auctions(MAZ_URL)
    collect_voivodeship_auctions(KUJ_POM_URL)


if __name__ == "__main__":
    main()
