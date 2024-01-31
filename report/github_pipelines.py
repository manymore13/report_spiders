import csv
import logging
import os
import sqlite3
from sqlite3 import Cursor, Connection

from scrapy.settings import Settings

from report.items import ReportItem
from report.spiders.east_report import EastReportSpider

"""
    github 专用管道

"""


class ReportCsvPipeline:
    """csv pipeline"""

    def __init__(self):
        self.cursor: Cursor = None
        self.conn: Connection = None
        self.table_name = "report_pdf"

        self.industry_report_dict: dict = {}
        self.title = ['研报名称', '机构名称', '发布时间', '行业', '研报地址']

    def process_item(self, item, spider):

        if item['industry_name'] in self.industry_report_dict:
            report_item_list = self.industry_report_dict[item['industry_name']]
            report_item_list.append(item)
        else:
            self.industry_report_dict[item['industry_name']] = [item]
        return item

    def open_spider(self, spider):
        settings: Settings = spider.settings
        sqlite_db_path = settings.get("SQLITE_DB_PATH")
        if not os.path.exists(sqlite_db_path):
            os.makedirs(sqlite_db_path)
        db_name = settings.get("SQLITE_DB_NAME", "report.db")
        logging.debug("ReportCsvPipeline: github db_name " + db_name)
        # 连接到 SQLite 数据库
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider: EastReportSpider):
        start_date = spider.begin_time
        end_date = spider.end_time
        industry_name_list = self.get_all_industry_name(start_date, end_date)
        settings = spider.settings
        file_store = settings.get("FILES_STORE")
        if not os.path.exists(file_store):
            os.makedirs(file_store)
        for industry_name in industry_name_list:
            report_item_list = self.get_report_info_from_db(industry_name, start_date, end_date)
            self.write_csv(file_store, industry_name + ".csv", report_item_list)
        self.conn.close()

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

    def get_report_info_from_db(self, industry_name: str, start_date, end_date):

        """从数据库获取研报信息"""

        select = "title,org_name,publish_date,industry_name,pdf_url"
        where = "industry_name = '{}' AND publish_date between '{}' AND '{}' ORDER BY publish_date DESC".format(
            industry_name, start_date, end_date)

        sql = "select {} from report_pdf where {}".format(select, where)

        self.cursor.execute(sql)
        data_items = self.cursor.fetchall()
        report_list = []
        logging.debug("industry_name: {}, report size :{}".format(industry_name, data_items))
        for data_item in data_items:
            report = ReportItem()
            report['title'] = data_item[0]
            report['org_name'] = data_item[1]
            report['publish_date'] = data_item[2]
            report['industry_name'] = data_item[3]
            report['pdf_url'] = data_item[4]
            report_list.append(report)
        return report_list

    def get_all_industry_name(self, start_date, end_date) -> list:

        """从数据库获取行业类别"""

        sql = ("select DISTINCT industry_name from report_pdf where publish_date between '{}' AND '{}' ORDER BY "
               "publish_date DESC").format(
            start_date, end_date)
        self.cursor.execute(sql)
        db_industry_name_list = self.cursor.fetchall()
        result_industry_name_list = []
        for db_item in db_industry_name_list:
            result_industry_name_list.append(db_item[0])
        logging.debug("get_all_industry_name: {}".format(result_industry_name_list))
        return result_industry_name_list
