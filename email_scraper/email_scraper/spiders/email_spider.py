# scrapy crawl gather_emails -a domain=vidalingua.com -o emails.json

import scrapy
import re
from tld import get_tld
from ..items import EmailScraperItem

class GatherEmailsSpider(scrapy.Spider):
    name = 'gather_emails'

    # Continue after crawling the homepage
    greedy = True
    email_regex = re.compile(r"((?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\]))")

    # used to skip specific URLs that include these keys, such as images, documents, or email links.
    forbidden_keys = ['tel:', 'mailto:', '.jpg', '.pdf', '.png']
    bad_sites = ['facebook', 'instagram', 'youtube', 'twitter', 'wiki', 'linkedin']

    # allowed_domains = ['vidalingua.com']

    def __init__(self, domain, **kwargs):
        self.start_urls = [f'https://{domain}']
        super().__init__(**kwargs)

    def parse(self, response):
        items = EmailScraperItem()

        try:
            html = response.body.decode('utf-8')
        except UnicodeDecodeError:
            return
        
        body_emails = self.email_regex.findall(html)

        # trying to exclude texts that look like emails but are not (e.g. image@home.png).
        emails = set([email for email in body_emails if
                get_tld('https://' + email.split('@')[-1], fail_silently=True)])
        
        # Get all h1 text, concatenate it into one string
        h1_text_string = ""
        try:
            h1_text_list = response.css('h1::text').extract()
            for h1_tag in h1_text_list:
                # Get rid of whitespace/newlines and append
                h1_text_string += h1_tag.strip().replace('\n', ' ') + " (end tag) "

        except Exception as e:
            pass
        
        # If emails were found, yield
        if len(emails) > 0:
            for email in emails:
                # For some reason the email regex allows for /'s in emails.  Let's actually toss those...
                if '/' not in email:
                    items['email'] = email
                    items['h1_text'] = h1_text_string
                    items['url'] = response.request.url
                    
                    yield items
            
        # If greedy, get all the links from the page and scrape those
        if self.greedy:
            
            # Get all links on the page
            links = response.xpath("//a/@href").getall()
            for link in links:
                skip = False
                
                # Check to make sure link is not an image, doc or email
                for key in self.forbidden_keys: 
                    if key in link:
                        skip = True
                        break
                
                # Check to make sure link is not a forbidden site
                for site in self.bad_sites:
                    if site in link:
                        skip = True
                        break

                if skip:
                    continue
                
                # we need scrapy.Request for absolute URLs and response.follow for relative URLs.
                try:
                    yield scrapy.Request(link, callback=self.parse)
                except ValueError:
                    try:
                        yield response.follow(link, callback=self.parse)
                    except:
                        pass

