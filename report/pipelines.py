# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import mimetypes
import os.path
from pathlib import Path

import scrapy
# useful for handling different item types with a single interface
from scrapy.pipelines.files import FilesPipeline


class ReportPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        yield scrapy.Request(url=item['pdf_url'], meta={"item": item})

    def file_path(self, request, response=None, info=None, *, item=None):
        report_item = request.meta['item']
        title = report_item['title']
        parent_name = report_item['industry_name']
        media_ext = Path(request.url).suffix
        # Handles empty and wild extensions by trying to guess the
        # mime type then extension or default to empty string otherwise
        if media_ext not in mimetypes.types_map:
            media_ext = ""
            media_type = mimetypes.guess_type(request.url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        return f"{parent_name}/{title}{media_ext}"

    # def process_item(self, item, spider):
    #     return item


class CsvPipeline:

    def __init__(self):
        self.industry_report_dict: dict = {}
        self.title = ['研报名称', '机构名称', '发布时间', '行业', '研报地址']

    def process_item(self, item, spider):
        need_add_item = [item['title'], item['org_name'], item['publish_date'], item['industry_name'], item['pdf_url']]
        if item['industry_name'] in self.industry_report_dict:
            report_item_list = self.industry_report_dict[item['industry_name']]
            report_item_list.append(need_add_item)
        else:
            self.industry_report_dict[item['industry_name']] = [need_add_item]

    def close_spider(self, spider: scrapy.Spider):
        settings = spider.settings
        file_store = settings.get("FILES_STORE")
        for industry_name, report_list in self.industry_report_dict.items():
            csv_path = os.path.join(".", file_store, industry_name)
            self.write_csv(csv_path, industry_name + ".csv", report_list)

    def write_csv(self, csv_path, csv_name, report_list):
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        csv_real_path = os.path.join(csv_path, csv_name)
        with open(csv_real_path, 'w', encoding='utf-8') as file:
            write = csv.writer(file, delimiter=',')
            write.writerow(self.title)
            write.writerows(report_list)
