import aioredis
import asyncio
import async_timeout
import argparse
import hashlib
import re
import sys
from bs4 import BeautifulSoup
import pathlib
import random
import logging
import os

from store import RedisAsyncStore

from aiohttp import ClientSession, ClientTimeout, TCPConnector

URL = 'https://news.ycombinator.com/'
REQUEST_TIMEOUT = 60
MAX_HOST_CONNECTIONS = 10
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15',
}
DOCUMENT_ROOT = 'links'
TIMEOUT_INTERVAL = 60

async def fetch(url):
    '''
    Fetch URL
    Args:
        session (str): Page URL

    Returns:
        HTML text
    '''
    logging.debug('Downloading url: {}'.format(url))
    timeout = ClientTimeout(total=REQUEST_TIMEOUT)
    connector = TCPConnector(limit_per_host=MAX_HOST_CONNECTIONS, ssl=False)
    try:
        async with ClientSession(timeout=timeout, headers=HEADERS, connector=connector) as session:
            async with session.get(url, ssl=False) as response:
                context = await response.text()
                return context
    except Exception as e:
        logging.error('Downloading error: {} [{}]'.format(url, e))
        raise

async def parse_main_page(page):
    '''
    Parse the hackernews main page
    
    discission_links - link to the pages within hn website, 
    like https://news.ycombinator.com/item?id=26628233

    stoty_links - links to the exteranl resources
    https://hpyproject.org/blog/posts/2021/03/hello-hpy/

    ranks - page rank on the main page at the time of the parsing

    On the crawling stage, save the discussion_link id and the last_rank to the cache storage. 
    Optioonally store the list of seen ranks. 
    '''
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find("table", {"class":"itemlist"})
    discussion_links = table.find_all('a', {'href': re.compile(r"^item")})
    story_links = table.find_all('a', {"class":"storylink"})
    ranks = table.find_all('span', {"class":"rank"})

    posts = zip(discussion_links, story_links)
    # check if story link already in cache and change it rank
    hashed_links = ["link_id_" + hashlib.md5(links['href'].encode('utf-8')).hexdigest() for links in story_links]
    return [links['href'] for links in discussion_links]


async def parse_article_page(article_dir, article_url):
    '''Parse the article's page

        Args:
            artcile_dir (str): Name of the article's folder
            article_url (str): URL for the article to parse
    '''
    logging.info('Parsing the article page: {} searching for the links in the comments'.format(article_url))
    article_page = await fetch(article_url)
    soup = BeautifulSoup(article_page, 'html.parser')
    links = soup.find_all(rel="nofollow")
    valid_links = [link['href'] for link in links]
    for link in valid_links:
        write_articles_to_disk(article_dir, link)
    return valid_links


def write_articles_to_disk(article_dir, link_url):
    '''Write the comment's link's URL to the artcile's .txtx file 
        in a the dedicated folder
        
        Args:
            article_dir (str): Name of the article's folder
            link_url (str): URL from the comment's section
    '''
    filename = 'links_in_comments.txt'
    path = os.path.join(article_dir, filename)
    with open(path, 'w') as fp:
        fp.write(link_url+'\n')
        logging.info('Write {} to the file the {}'.format(link_url, path))


async def write_links_to_store(links_hash, rank):
    '''Save to visited artciels to cache [WIP]
    '''
    pass 
   

async def main(output_dir):
    ''' Run homepage parser and the parser for the article's pages 
    '''
    tasks = []

    home_page_html = await fetch(URL)
    articles_url = await parse_main_page(home_page_html)

    # TO-DO check if link has been downloaded and processed
    logging.info('Parsed the main page: {} new articles'.format(len(articles_url)))

    for article_url in articles_url:
        article_id = re.search("^item\?id=(.*)", article_url).groups()[0]
        article_dir = output_dir.joinpath(article_id)
        if os.path.isdir(article_dir):
            continue
        article_dir.mkdir(parents=True, exist_ok=True)
        
        task = asyncio.create_task(parse_article_page(article_dir, URL+article_url))
        tasks.append(task)
        await asyncio.sleep(1)
    await asyncio.gather(*tasks)


async def monitor_main(output_dir, interval):
    '''Monitor the main on the specified intervals
        Args:
            output_dir (str): Name of the output's folder
            interval (int): Time interval for the periodic tasks
    '''
    while True:
        try:
            await asyncio.wait_for(main(output_dir), timeout=interval)
        except Exception as e:
            logging.error('Crawler failed: {}'.format(e))
        await asyncio.sleep(interval)


def setup_logger(is_debug):
    ''' Setup logger conf
    Args:
        is_debug(bool): Debug flag
    '''
    logging.basicConfig(level=logging.DEBUG if is_debug else logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')


def parse_arguments():
    """
    Get program arguments
    Returns:
        argparse.Namespace: Program arguments
    """
    parser = argparse.ArgumentParser(
        description='Async crawler for news.ycombinator.com'
    )
    parser.add_argument('-o', '--output', type=str, default=DOCUMENT_ROOT,
                        help='Output files directory')
    parser.add_argument('-i', '--interval', type=str, default=TIMEOUT_INTERVAL,
                        help='Output files directory')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Show debug messages')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    setup_logger(args.debug)

    output_dir = pathlib.Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    interval = args.interval
    
    try:
        asyncio.run(monitor_main(output_dir, interval))
    except asyncio.CancelledError:
        logging.info('Crawler canceled')
    except KeyboardInterrupt:
        logging.info('Crawler stopped')
    