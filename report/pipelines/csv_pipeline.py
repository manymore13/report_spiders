# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import os

import scrapy


class ReportCsvPipeline:
    """csv pipeline"""

    def __init__(self):
        self.industry_report_dict: dict = {}
        self.title = ['研报名称', '机构名称', '发布时间', '行业', '研报地址']

    def process_item(self, item, spider):

        if item['industry_name'] in self.industry_report_dict:
            report_item_list = self.industry_report_dict[item['industry_name']]
            report_item_list.append(item)
        else:
            self.industry_report_dict[item['industry_name']] = [item]
        return item

    def close_spider(self, spider: scrapy.Spider):
        settings = spider.settings
        file_store = settings.get("FILES_STORE")
        for industry_name, report_item_list in self.industry_report_dict.items():
            csv_path = os.path.join(".", file_store, industry_name)
            self.write_csv(csv_path, industry_name + ".csv", report_item_list)

    def write_csv(self, csv_path, csv_name, report_item_list):
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        csv_real_path = os.path.join(csv_path, csv_name)
        with open(csv_real_path, 'w', encoding='utf-8', newline='') as file:
            write = csv.writer(file, delimiter=',')
            write.writerow(self.title)
            for item in report_item_list:
                write.writerow(
                    [item['title'], item['org_name'], item['publish_date'], item['industry_name'], item['pdf_url']])