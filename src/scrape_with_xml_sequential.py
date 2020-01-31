from lxml import etree
from bs4 import BeautifulSoup
import aiohttp
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


async def iterread(url: str,
                   session: aiohttp.ClientSession,
                   chunksize: int = 1024) -> bytes:
    """
    Downloads `url` in chunks (streamed).
    Yields chunks with size as specified by `chunksize` (default 1024 Bytes)
    """
    async with session.get(url) as resp:
        try:
            size = int(resp.headers['Content-Length']) / 1024**2
            logging.info(f'Downloading {url} {size:.2f} MB')
        except KeyError:
            logging.info(f'Downloading {url}')

        async for chunk in resp.content.iter_chunked(chunksize):
            yield chunk


async def parsexml(session: aiohttp.ClientSession) -> str:
    """
    Parses elementtree from streamed input (by chunks) in-memory
    """
    url = "http://www.fit-pro.cz/export/fitpro-cf6ad8215df1f1cf993029a1684d5251.xml"
    ns = {"zbozi": "http://www.zbozi.cz/ns/offer/1.0"}
    parser = etree.XMLPullParser()
    async for chunk in iterread(url, session, 1024):
        parser.feed(chunk)
        for _, elem in parser.read_events():
            if elem.tag == r"{http://www.zbozi.cz/ns/offer/1.0}SHOPITEM":
                url = elem.xpath('//zbozi:URL', namespaces=ns)[0]
                elem.clear()
                yield url.text


async def main():
    async with aiohttp.ClientSession() as session:
        async for url in parsexml(session):
            async with session.get(url) as resp:
                data = await resp.read()
                size = len(data) / 1024
                soup = BeautifulSoup(data, "lxml")
                title = soup.select_one('h1').text
                logging.info(f"{title} -> {resp.url} ({size:.2f} KB)")


if __name__ == "__main__":
    asyncio.run(main())
