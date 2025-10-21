import requests
from tenacity import retry, wait_exponential
from io import BytesIO


@retry(wait=wait_exponential(multiplier=2, min=1, max=10))
def get_file(url: str) -> BytesIO:
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    byte_stream = BytesIO()
    byte_stream.write(response.content)
    byte_stream.seek(0)
    return byte_stream


@retry(wait=wait_exponential(multiplier=2, min=1, max=10))
def get_page(url: str) -> str:
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    return response.text
