# YCrawler
Async crawler for news.ycombinator.com.

The follwoing script:

- Crawl top 50 news from root page
- Download and save links in comments to news
- Download pages non-recursively
- Download pages without requisites (css/img/js/etc)
- Use standard library and aiohttp

## Requirements
```
Python 3.x
Install dependencies
pip3 install -r requirements.txt
```

## How to run
````
$ python3 ycrawler.py -h

usage: hn_scrapper.py [-h] [-o OUTPUT] [-i INTERVAL] [-d]

Async crawler for news.ycombinator.com

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output files directory
  -i INTERVAL, --interval INTERVAL
                        Interval for parsing                  
  -d, --debug           Show debug messages
```

## TO-DO
- Save visited links to Redis cache