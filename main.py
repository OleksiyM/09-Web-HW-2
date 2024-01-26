import json
import logging
import re
import time

import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
from scrapy.utils.project import get_project_settings


class QuoteItem(Item):
    tags = Field()
    author = Field()
    quote = Field()


class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()


class DataPipeline:
    quotes = []
    authors = []

    def close_spider(self, _):
        self.check_quotes_integrity()
        logging.info(f'Writing data to the files: \n- quotes: {len(self.quotes)}\n- authors: {len(self.authors)}')
        with open('quotes.json', 'w', encoding='utf-8') as f:
            json.dump(self.quotes, f, indent=2)
        with open('authors.json', 'w', encoding='utf-8') as f:
            json.dump(self.authors, f, indent=2)

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if 'fullname' in adapter.keys():
            adapter['fullname'] = self.clean_text(adapter['fullname'])
            adapter['born_date'] = self.clean_text(adapter['born_date'])
            adapter['born_location'] = self.clean_text(adapter['born_location'])
            adapter['description'] = self.clean_text(adapter['description'])
            self.authors.append(dict(adapter))
        if 'quote' in adapter.keys():
            adapter['quote'] = self.clean_text(adapter['quote'])
            adapter['author'] = self.clean_text(adapter['author'])
            adapter['tags'] = [self.clean_text(tag) for tag in adapter['tags']]
            adapter['tags'] = [tag for tag in adapter['tags'] if tag]  # Remove empty tags
            adapter['tags'] = list(set(adapter['tags']))  # Remove duplicate tags
            adapter['tags'].sort()  # Sort tags alphabetically
            self.quotes.append(dict(adapter))

    @staticmethod
    def clean_text(text):
        text = text.strip()
        text = re.sub(r"\n", " ", text)  # Replace newlines with spaces
        text = re.sub(r"[^\x00-\x7F]+", "", text)  # Remove control characters
        text = re.sub(r"[^\w\-.,\s]", "", text)  # Remove special characters except those allowed
        text = re.sub(r"\s+", " ", text)  # Replace multiple spaces with a single space
        return text

    def check_quotes_integrity(self):
        for quote in self.quotes:
            author = quote['author']
            if author not in [a['fullname'] for a in self.authors]:
                self.quotes.remove(quote)
                logging.info(
                    f'    Error: Author "{author}" not found in quote:\n    {quote["quote"]}\n    This quote was removed')


class QuotesSpider(scrapy.Spider):
    name = "get_quotes"
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['https://quotes.toscrape.com']
    custom_settings = {'ITEM_PIPELINES': {DataPipeline: 300}}

    def parse(self, response, **kwargs):
        for q in response.xpath("/html//div[@class='quote']"):
            tags = q.xpath('div[@class="tags"]/a/text()').getall()
            author = q.xpath('span/small/text()').get()
            quote = q.xpath('span[@class="text"]/text()').get()
            yield QuoteItem(tags=tags, author=author, quote=quote)
            yield response.follow(url=self.start_urls[0] + q.xpath('span/a/@href').get(), callback=self.parse_author)

        next_page_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_page_link:
            yield scrapy.Request(url=self.start_urls[0] + next_page_link)

    def parse_author(self, response):
        content = response.xpath("/html//div[@class='author-details']")
        fullname = content.xpath("h3[@class='author-title']/text()").get()
        born_date = content.xpath("p/span[@class='author-born-date']/text()").get()
        born_location = content.xpath("p/span[@class='author-born-location']/text()").get()
        born_location = born_location[3:] if born_location.startswith('in') else born_location
        description = content.xpath("div[@class='author-description']/text()").get().strip()
        logging.debug(f'fullname: {fullname}')
        yield AuthorItem(fullname=fullname, born_date=born_date, born_location=born_location, description=description)


if __name__ == '__main__':
    start_time = time.time()

    # add DEFAULT_REQUEST_ENCODING = 'utf-8'
    process = CrawlerProcess(get_project_settings())
    # process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()

    end_time = time.time()
    logging.info(f'Total execution time: {end_time - start_time:.2f} sec')
