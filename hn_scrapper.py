import aiohttp
import asyncio
import aioredis
import async_timeout
import hashlib
import re
from bs4 import BeautifulSoup
import random

URL = 'https://news.ycombinator.com/'

async def fetch(session, url, sleep=False):
    '''
    resp = await session.request(method="GET", url=url, **kwargs)
    '''

    async with session.get(url, ssl=False) as response:
        return await response.text()

async def parse_page(page):
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find("table", {"class":"itemlist"})
    comment_links = table.find_all('a', {'href': re.compile(r"^item")})
    story_links = table.find_all('a', {"class":"storylink"})
    ranks = table.find_all('span', {"class":"rank"})
    posts = zip(comment_links, story_links)
    # check if story link already in cache and change it rank
    hashed_links = ["link_id_" + hashlib.md5(links['href'].encode('utf-8')).hexdigest() for links in story_links]
    return [links['href'] for links in comment_links]

async def parse_comment_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find("table", {"class":"comment-tree"})
    return len(table.find_all('tr'))

async def write_links_to_store(links_hash, rank):
    pairs = zip(links_hash, rank)
    # await redis.msetnx(pairs)
    # redis.close()
    # await redis.wait_closed()
   
async def main():
    tasks = []
    async with aiohttp.ClientSession() as session:
        loop = asyncio.get_event_loop()
        home_page_html = await fetch(session, URL)
        comment_links = await parse_page(home_page_html)
        print(comment_links)
        for comment_url in comment_links:
            task = loop.create_task(fetch(session, URL+comment_url, True))
            tasks.append(task)
            await asyncio.sleep(1)
        repsonses = await asyncio.gather(*tasks)
        print(repsonses)
        # commetns = await asyncio.gather(*repsonses)
        # print(commetns)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())