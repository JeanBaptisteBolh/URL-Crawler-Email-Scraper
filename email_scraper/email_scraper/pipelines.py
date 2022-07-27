# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import socket
import geocoder
from urllib import parse
import sqlite3

# Extracted data -> Temporary Containers (items) -> Pipelines -> Database


class EmailScraperPipeline:

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = sqlite3.connect('scraped_emails.db')
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("DROP TABLE IF EXISTS emails")
        self.curr.execute("""
            CREATE TABLE EMAILS(
                email text UNIQUE,
                h1_text text,
                urls text,
                host_country text
            )""")

    # Items yielded in the parse method of our spider will arive here.
    def process_item(self, item, spider):
        self.store_db(item)

        try:
            print("PIPELINE:", adapter.get('email'))
        except Exception as e:
            pass
        return item

    def store_db(self, item):
        adapter = ItemAdapter(item)
        email = adapter.get('email')
        h1_text = adapter.get('h1_text')
        url = adapter.get('url')

        url_host_country = self.get_host_country_for_url(url)

        # Check if email is in the db already
        result = self.email_exists(email)
        
        # If record already exists, append if there is text to append.
        if result:
            if h1_text != "":
                self.append_to_h1_text(email, h1_text)
            if url != "":
                self.append_to_urls(email, url, url_host_country)

        # Otherwise add new email and h1 text
        else:
            self.store_new_email(email, h1_text, url, url_host_country)

    def email_exists(self, email):
        self.curr.execute(
            'SELECT email FROM emails WHERE email=(?)', (email,))
        return self.curr.fetchone()

    def store_new_email(self, email, h1_text, url, url_host_country):
        self.curr.execute('INSERT INTO emails VALUES (?,?,?,?)',(
            email,
            h1_text,
            url,
            url_host_country,
        ))
        self.conn.commit()

    def append_to_h1_text(self, email, h1_text):
        self.curr.execute('UPDATE emails SET h1_text = h1_text || (?) WHERE email = (?)', (
            h1_text,
            email,
        ))

    def append_to_urls(self, email, url, url_host_country):
        self.curr.execute("""
            UPDATE emails 
            SET urls = urls || (?), host_country = (?)
            WHERE email = (?)""",(
            url,
            url_host_country,
            email,
        ))

    def get_host_country_for_url(self, url):
        try:
            split_url = parse.urlsplit(url)
            netloc = split_url.netloc
            ip = geocoder.ip(socket.gethostbyname(netloc))
            country = ip.country
            return country

        except Exception as e:
            return None

