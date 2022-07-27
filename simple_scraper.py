import re
import requests
from urllib.parse import urlsplit
# a container that is in the form of a list used for appending and popping on either end.
from collections import deque
from bs4 import BeautifulSoup
import pandas as pd


def parse_url_for_email(url):
    '''
    Crawls urls saving any emails we find
    '''

    # to save urls to be scraped
    unscraped_urls = deque([url])

    # to save scraped urls
    scraped_urls = set()

    # to save fetched emails
    emails = set()

    # While there are still urls in the deque
    while len(unscraped_urls):
        url = unscraped_urls.popleft()
        scraped_urls.add(url)

        parts = urlsplit(url)

        print(parts)

        base_url = "{0.scheme}://{0.netloc}".format(parts)
        if '/' in parts.path:
            path = url[:url.rfind('/')+1]
        else:
            path = url

        print("Crawling URL %s" % url)
        # Make a get request to the page
        try:
            response = requests.get(url, timeout=5)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
            continue

        # Parse emails from the page
        new_emails = set(re.findall(
            r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.com", response.text, re.I))
        emails.update(new_emails)

        # Get links from the page
        soup = BeautifulSoup(response.text, 'html.parser')
        for anchor in soup.find_all("a"):
            if "href" in anchor.attrs:
                link = anchor.attrs["href"]
            else:
                link = ''

            if link.startswith('/'):
                link = base_url + link

            elif not link.startswith('http'):
                link = path + link

            #
            if not link.endswith(".gz"):
                if not link in unscraped_urls and not link in scraped_urls:
                    unscraped_urls.append(link)

    return emails


emails = parse_url_for_email('https://vidalingua.com/')
print(emails)
