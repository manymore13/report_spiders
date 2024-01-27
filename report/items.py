# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ReportItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    org_name = scrapy.Field()
    publish_date = scrapy.Field()
    industry_name = scrapy.Field()
    pdf_url = scrapy.Field()
    pass
