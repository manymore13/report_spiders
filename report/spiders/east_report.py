import json
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

    GITHUB_PIPELINES = {
        "report.pipelines.ReportSqlitePipeline": 300,
        # "report.pipelines.ReportPdfPipeline": 301,
        "report.github_pipelines.ReportCsvPipeline": 302,
        "report.github_pipelines.TodayReportPipeline": 303,
    }

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "report.middlewares.ReportPdfExitMiddleware": 543,
        },

        "ITEM_PIPELINES": GITHUB_PIPELINES

        # "ITEM_PIPELINES": {
        #     "report.pipelines.ReportSqlitePipeline": 300,
        #     # "report.pipelines.ReportPdfPipeline": 301,
        #     "report.pipelines.ReportCsvPipeline": 302,
        #
        # }
    }

    name = "east_report"
    allowed_domains = ["eastmoney.com"]

    east_money_url: str = 'https://reportapi.eastmoney.com/report/list?cb=datatable1808538&industryCode={' \
                          'industry_code}&pageSize={page_size}&industry=*&rating=*&ratingChange=*&beginTime={' \
                          'begin_time}&endTime={end_time}&pageNo={page_no}&fields=&qType=1&orgCode=&rcode=&_={time}'

    report_info_url = 'https://data.eastmoney.com/report/zw_industry.jshtml?infocode='

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.industry_code_list = getattr(self, "codes", '*').split(',')
        self.count: int = getattr(self, "count", '50')
        self.begin_time = getattr(self, "begin_time", '2023-11-26')
        self.end_time: str = getattr(self, "end_time", '2023-11-26')
        self.industry_code_dic = load_industry()
        self.check_input_code()
        self.is_gened_url_codes = set()
        self.table_name = 'eastmoney'

    def check_input_code(self):
        for input_code in self.industry_code_list:
            if input_code not in self.industry_code_dic:
                raise RuntimeError("非法行业code:{}".format(input_code))

    def start_requests(self) -> Iterable[Request]:
        """起始抓取"""
        self.log('start_requests')
        page_no = 1
        for industry_code in self.industry_code_list:
            report_req_url = self.gene_report_req_url(industry_code=industry_code, page_size=self.count,
                                                      page_no=page_no)
            yield Request(url=report_req_url, meta={'industry_code': industry_code, "page_no": page_no},
                          callback=self.parse_report)

    def gene_report_req_url(self, industry_code, page_size, page_no) -> str:
        cur_time = int(time.time())
        return self.east_money_url.format(industry_code=industry_code, page_size=page_size,
                                          begin_time=self.begin_time, end_time=self.end_time,
                                          page_no=page_no, time=cur_time)

    def add_other_report_req_url(self, industry_code, total_page) -> Iterable[Request]:
        self.is_gened_url_codes.add(industry_code)

        for page_no in list(range(2, total_page + 1)):
            req_url = self.gene_report_req_url(industry_code, self.count, page_no)
            yield Request(url=req_url, meta={'industry_code': industry_code, 'page_no': page_no},
                          callback=self.parse_report)

    def parse_report(self, response: Response):
        report_json = json.loads(response.text)
        report_list = report_json['data']
        total_page = report_json['TotalPage']
        size = report_json['size']
        page_no = response.meta['page_no']
        hits = report_json['hits']
        industry_code = response.meta['industry_code']
        if industry_code not in self.is_gened_url_codes:
            self.log(
                "industry_code= {}, hits = {} total_page={}, size = {}, data={}".format(industry_code, hits, total_page,
                                                                                        size, len(report_list)))
            if int(page_no) < int(total_page):
                self.is_gened_url_codes.add(industry_code)
                for next_page_no in list(range(2, total_page + 1)):
                    req_url = self.gene_report_req_url(industry_code, self.count, next_page_no)
                    yield Request(url=req_url, meta={'industry_code': industry_code, 'page_no': next_page_no},
                                  callback=self.parse_report)

        for report in report_list:
            reportItem = ReportItem()
            reportItem['title'] = self.clean_filename(report['title'])
            reportItem['org_name'] = report['orgSName']
            reportItem['publish_date'] = re.findall('\\d+-\\d+-\\d+', report['publishDate'])[0]
            reportItem['industry_name'] = report['industryName']
            pdf_info_url = self.report_info_url + report['infoCode']
            meta = {'pdf_info_url': pdf_info_url, 'report_item': reportItem}
            reportItem['info_code'] = report['infoCode']
            reportItem['pdf_url'] = pdf_info_url
            # yield reportItem
            # self.log('pdf_url= ' + pdf_info_url)
            yield Request(url=pdf_info_url, meta=meta, callback=self.parse_pdf_url)

    def parse_pdf_url(self, response: Response):
        reportItem = response.meta['report_item']
        pdf_url = response.xpath('//span[@class="to-link"]/a[@class="pdf-link"]/@href')[0].get()
        reportItem['pdf_url'] = pdf_url
        yield reportItem

    def clean_filename(self, filename):
        # 使用正则表达式去除非法字符
        cleaned_filename = re.sub(r'[\/:*?"<>|]', '_', filename)

        # 可以添加其他需要去除的特殊字符
        # cleaned_filename = re.sub(r'[...]', '_', cleaned_filename)

        return cleaned_filename
