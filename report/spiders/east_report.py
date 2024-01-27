import json
import logging
import os
import re
import time
from typing import Iterable

import scrapy
from scrapy import Request
from scrapy.http import Response

from report.items import ReportItem


def load_industry() -> dict:
    current_directory = os.getcwd()
    print(current_directory)
    file_name = './industry.json'
    with open(file_name, 'r', encoding='utf-8') as f:
        industry_name_list = json.load(f)
    industry_code_dic = {}
    for industry in industry_name_list:
        industry_code_dic[industry['industry_code']] = industry['industry_name']

    return industry_code_dic


class EastReportSpider(scrapy.Spider):

    """东方财富行业研报"""

    name = "east_report"
    allowed_domains = ["eastmoney.com"]

    east_money_url: str = 'https://reportapi.eastmoney.com/report/list?cb=datatable1808538&industryCode={' \
                          'industry_code}&pageSize={page_size}&industry=*&rating=*&ratingChange=*&beginTime={' \
                          'begin_time}&endTime={end_time}&pageNo={page_no}&fields=&qType=1&orgCode=&rcode=&_={time}'

    report_info_url = 'https://data.eastmoney.com/report/zw_industry.jshtml?infocode='

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.industry_code_list = getattr(self, "codes", '*,738').split(',')
        self.page_size: int = getattr(self, "page_size", '50')
        self.begin_time = getattr(self, "begin_time", '2023-11-26')
        self.end_time: str = getattr(self, "end_time", '2023-11-26')
        self.page_no: str = getattr(self, "page_no", 1)
        self.industry_code_dic = load_industry()
        self.check_input_code()

    def check_input_code(self):
        for input_code in self.industry_code_list:
            if input_code not in self.industry_code_dic:
                raise RuntimeError("非法行业code:{}".format(input_code))

    def start_requests(self) -> Iterable[Request]:
        logging.debug('start_requests')
        cur_time = int(time.time())
        for industry_code in self.industry_code_list:
            final_report_url = self.east_money_url.format(industry_code=industry_code, page_size=self.page_size,
                                                          begin_time=self.begin_time, end_time=self.end_time,
                                                          page_no=self.page_no, time=cur_time)
            yield Request(url=final_report_url, meta={'req_industry_code': industry_code}, callback=self.parse_report)
            logging.debug('final_url =' + final_report_url)

    def parse_report(self, response: Response):
        # print("response: " + response.text + "   # " + response.meta['req_industry_code'])
        report_content = re.findall(r"\((.*?)\)", response.text)[0]
        report_json = json.loads(report_content)
        report_list = report_json['data']
        total_page = report_json['TotalPage']
        size = report_json['size']
        # print("data={},{},{}".format(report_list, total_page, size))
        for report in report_list:
            reportItem = ReportItem()
            reportItem['title'] = report['title']
            reportItem['org_name'] = report['orgSName']
            reportItem['publish_date'] = re.findall('\\d+-\\d+-\\d+', report['publishDate'])[0]
            reportItem['industry_name'] = report['industryName']
            pdf_info_url = self.report_info_url + report['infoCode']
            meta = {'pdf_info_url': pdf_info_url, 'report_item': reportItem}
            # logging.debug('pdf_url= ' + pdf_info_url)
            yield Request(url=pdf_info_url, meta=meta, callback=self.parse_pdf_url)

    def parse_pdf_url(self, response: Response):
        reportItem = response.meta['report_item']
        pdf_url = response.xpath('//span[@class="to-link"]/a[@class="pdf-link"]/@href')[0].get()
        reportItem['pdf_url'] = pdf_url
        yield reportItem
