# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

# Extracted data -> Temporary Containers (items) -> Database

import scrapy


class EmailScraperItem(scrapy.Item):
    # define the fields for your item here like:
    email = scrapy.Field()
    page_title = scrapy.Field()
    h1_text =  scrapy.Field()
    url = scrapy.Field()
