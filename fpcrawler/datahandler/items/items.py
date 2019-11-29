# -*- coding: utf-8 -*-
# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FpCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    date = scrapy.Field()
    comparison = scrapy.Field()
    end_price = scrapy.Field()
    start_price = scrapy.Field()
    highest_price = scrapy.Field()
    lowest_price = scrapy.Field()
    transactions = scrapy.Field()
