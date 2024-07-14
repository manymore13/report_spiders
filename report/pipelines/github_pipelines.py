import csv
import json
import logging
import os
import sqlite3
from datetime import datetime
from sqlite3 import Cursor, Connection

from scrapy.settings import Settings

from report.items import ReportItem
from report.spiders.east_report import EastReportSpider
from report.utils import getNumStr

"""
    github 专用管道

"""


def get_today_time_str():
    time_str = datetime.today().strftime("%Y-%m-%d")
    return time_str


class TodayReportPipeline:
    """今日日报"""

    def __init__(self):
        self.cursor = None
        self.conn = None
        self.table_name = None
        self.file_name = "today.md"

    def open_spider(self, spider):
        pass

    def check_db(self, spider):
        if hasattr(spider, 'table_name'):
            self.table_name = getattr(spider, 'table_name')

        settings: Settings = spider.settings
        sqlite_db_path = settings.get("SQLITE_DB_PATH")
        if not os.path.exists(sqlite_db_path):
            os.makedirs(sqlite_db_path)
        db_name = settings.get("SQLITE_DB_NAME", "report.db")
        logging.debug("TodayReportPipeline: github db_name " + db_name)
        # 连接到 SQLite 数据库
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider: EastReportSpider):
        self.check_db(spider)
        report_list = self.get_today_reports()
        self.conn.close()
        if len(report_list) == 0:
            return
        settings = spider.settings
        file_store = settings.get("FILES_STORE")
        root_path = os.path.join(file_store, self.table_name)
        self.writeRecord(root_path, report_list)

    def writeRecord(self, root_path, report_item_list):
        """增量记录"""
        # 外面目录
        self.write_markdown_file(root_path, 'today.md', report_item_list)
        self.write_json_file(root_path, "today.json", report_item_list)

        # 按年月路径
        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day
        month_str = getNumStr(month)
        day_str = getNumStr(day)
        path_components = [root_path, str(year), month_str]
        path = os.path.join(*path_components)
        if not os.path.exists(path):
            os.makedirs(path)
        print(f"Path: {path}")
        file_name = day_str
        # self.write_markdown_file(path, f"{file_name}.md", report_item_list)
        self.write_json_file(path, f"{file_name}.json", report_item_list)

    def write_markdown_file(self, md_path, file_name, report_item_list):
        if not os.path.exists(md_path):
            os.makedirs(md_path)
        md_real_path = os.path.join(md_path, file_name)
        with open(md_real_path, 'w', encoding='utf-8', newline='') as f:
            f.write("| 研报名称 | 行业 | 机构名称 |\n")
            f.write("|------|----------|--------------|\n")
            for report in report_item_list:
                f.write(
                    f"| [{report['title']}]({report['pdf_url']}) | {report['industry_name']} | {report['org_name']}| \n")

    def write_json_file(self, path, file_name, report_item_list):
        if not os.path.exists(path):
            os.makedirs(path)
        real_path = os.path.join(path, file_name)
        items_dicts = [dict(item) for item in report_item_list]
        json_data = json.dumps(items_dicts, ensure_ascii=False)
        with open(real_path, "w", encoding='utf-8', newline='') as f:
            f.write(json_data)

    def get_today_reports(self):
        """从数据库获取研报信息"""
        today_time_str = get_today_time_str()
        print(f"today: {today_time_str}")
        select = "title,org_name,publish_date,industry_name,pdf_url"
        where = "publish_date = '{}'".format(today_time_str)

        sql = "select {} from {} where {}".format(select, self.table_name, where)
        print(sql)
        self.cursor.execute(sql)
        data_items = self.cursor.fetchall()
        report_list = []
        for data_item in data_items:
            report = ReportItem()
            report['title'] = data_item[0]
            report['org_name'] = data_item[1]
            report['publish_date'] = data_item[2]
            report['industry_name'] = data_item[3]
            report['pdf_url'] = data_item[4]
            report_list.append(report)
        return report_list


class ReportCsvPipeline:
    """csv pipeline"""

    def __init__(self):
        self.cursor: Cursor = None
        self.conn: Connection = None
        self.table_name = "report"

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

        if hasattr(spider, 'table_name'):
            self.table_name = getattr(spider, 'table_name')

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
        cvs_path = os.path.join(file_store, self.table_name)
        if not os.path.exists(cvs_path):
            os.makedirs(cvs_path)
        for industry_name in industry_name_list:
            report_item_list = self.get_report_info_from_db(industry_name, start_date, end_date)
            self.write_csv(cvs_path, industry_name + ".csv", report_item_list)
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

        sql = "select {} from {} where {}".format(select, self.table_name, where)

        self.cursor.execute(sql)
        data_items = self.cursor.fetchall()
        report_list = []
        logging.debug("industry_name: {}, report size :{}".format(industry_name, len(data_items)))
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

        sql = ("select DISTINCT industry_name from {} where publish_date between '{}' AND '{}' ORDER BY "
               "publish_date DESC").format(
            self.table_name, start_date, end_date)
        self.cursor.execute(sql)
        db_industry_name_list = self.cursor.fetchall()
        result_industry_name_list = []
        for db_item in db_industry_name_list:
            result_industry_name_list.append(db_item[0])
        logging.debug("get_all_industry_name: {}".format(result_industry_name_list))
        return result_industry_name_list


if __name__ == '__main__':
    ss = datetime.now().strftime("%Y-%m-%d")
    print(ss)
    pass
