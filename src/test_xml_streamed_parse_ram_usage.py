import asyncio
import logging

import aiohttp
from guppy import hpy
from lxml import etree, objectify

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


async def iterread(url: str,
                   session: aiohttp.ClientSession,
                   chunksize: int = 1024):
    """
    Downloads `url` in chunks (streamed).
    Yields chunks with size as specified by `chunksize` (default 1024 Bytes)
    """
    async with session.get(url) as resp:
        try:
            logging.info('Downloading %.2f MB' %
                         (int(resp.headers['Content-Length']) / 1024**2))
        except KeyError:
            logging.info('Downloading')

        async for chunk in resp.content.iter_chunked(chunksize):
            yield chunk


async def parsexml(session: aiohttp.ClientSession) -> str:
    """ Parses ~100MB gzipped XML in Chunks (for memory profiling purposes) """
    import zlib
    url = "http://aiweb.cs.washington.edu/research/projects/xmltk/xmldata/data/pir/psd7003.xml.gz"
    d = zlib.decompressobj(zlib.MAX_WBITS | 16)
    parser = etree.XMLPullParser()
    async for chunk in iterread(url, session, 1024):
        chunk = d.decompress(chunk)
        parser.feed(chunk)
        for _, elem in parser.read_events():
            if elem.tag == r"ProteinEntry":
                yield elem
                elem.clear()


async def main():
    h = hpy()
    async with aiohttp.ClientSession() as session:
        async for elem in parsexml(session):
            memory_usage = h.heap().size / 1024**2
            # dumps = etree.tostring(elem, pretty_print=True, encoding='unicode')
            sequence = elem.xpath('//sequence')[0].text
            msg = f"MEM: {memory_usage:.2f} MB | Sequence data: {sequence}"
            logging.debug(msg)


if __name__ == "__main__":
    asyncio.run(main())
