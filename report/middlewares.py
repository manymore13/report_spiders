# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import json
import logging
import re
import sqlite3

import scrapy.http
from scrapy import signals
# useful for handling different item types with a single interface
from scrapy.http import TextResponse
from scrapy.settings import Settings

from report.utils import create_industry_view


class ReportSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ReportDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response: scrapy.http.Response, spider):
        # Called with the response returned from the downloader.
        logging.debug("process_response ----------{}".format(response.url))
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ReportPdfExitMiddleware:

    def __init__(self, table_name):
        self.table_name = table_name
        self.cursor = None
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        table_name = getattr(crawler.spider, 'table_name', 'table_name')

        s = cls(table_name)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_response(self, request, response: scrapy.http.Response, spider):
        # Called with the response returned from the downloader.
        # logging.debug("process_response ----------{}".format(response.url))
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()
        url = 'reportapi.eastmoney.com'
        if url not in response.url:
            return response
        data_list = re.findall(r"\((.*)\)", response.text)
        if len(data_list) == 0:
            return response
        try:
            report_content = data_list[0]
            report_json = json.loads(report_content)
            report_list = report_json['data']
        except:
            return response

        new_report_list = []
        for report_data in report_list:
            info_code = report_data['infoCode']
            if not self.exit_data_db(info_code):
                new_report_list.append(report_data)
        report_json['data'] = new_report_list
        modified_json_str = json.dumps(report_json)
        modified_response = TextResponse(
            url=response.url,
            status=response.status,
            headers=response.headers,
            body=modified_json_str.encode('utf-8')
        )
        return modified_response

    def exit_data_db(self, info_code):
        self.cursor.execute('SELECT * FROM {} WHERE info_code = ?'.format(self.table_name), (info_code,))
        existing_data = self.cursor.fetchone()
        return existing_data is not None

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
        settings: Settings = spider.settings
        db_name = settings.get("SQLITE_DB_NAME", "report.db")
        # 连接到 SQLite 数据库
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute(create_industry_view)

    def close_spider(self, spider):
        # 断开数据库连接
        self.conn.close()
