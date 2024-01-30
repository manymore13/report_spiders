# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import logging
import mimetypes
import os.path
import sqlite3
from pathlib import Path
from sqlite3 import Connection, Cursor

import scrapy
# useful for handling different item types with a single interface
from scrapy.pipelines.files import FilesPipeline
from scrapy.settings import Settings


class ReportPdfPipeline(FilesPipeline):
    """下载研报PDF文件"""

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


class ReportSqlitePipeline:

    """研报插入sqlite数据库"""

    def __init__(self):
        self.cursor: Cursor = None
        self.conn: Connection = None
        self.table_name = "report_pdf"

    def open_spider(self, spider):
        settings: Settings = spider.settings
        db_name = settings.get("SQLITE_DB_NAME", "report.db")
        logging.debug("---db_name " + db_name)
        # 连接到 SQLite 数据库
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        # self.cursor.execute("DELETE FROM {}".format(self.table_name))
        # 创建表（如果不存在）
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS {} (
                title TEXT NOT NULL,
                org_name TEXT NOT NULL,
                publish_date TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                info_code TEXT PRIMARY KEY NOT NULL,
                pdf_url TEXT NOT NULL
            )
        '''.format(self.table_name))
        self.conn.commit()

    def close_spider(self, spider):
        # 断开数据库连接
        self.conn.close()

    def process_item(self, item, spider):
        # 插入或更新数据
        self.cursor.execute('SELECT * FROM {} WHERE info_code = ?'.format(self.table_name), (item['info_code'],))
        existing_data = self.cursor.fetchone()

        if existing_data is None:
            # 插入数据
            self.insert_item(item)

        #     # 更新数据
        #     self.cursor.execute('UPDATE mytable SET email = ? WHERE info_code = ?', (item['email'], item['info_code']))
        # else:
        #     # 插入数据
        #     self.cursor.execute('INSERT INTO mytable (name, email) VALUES (?, ?)', (item['name'], item['email']))

        return item

    def insert_item(self, data):
        # 列的字段
        keys = ', '.join(data.keys())
        # 行字段
        values = ', '.join(['?'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=self.table_name, keys=keys, values=values)
        logging.debug("insert item " + sql)
        # 将字段的value转化为元祖存入
        self.cursor.execute(sql, tuple(data.values()))
        self.conn.commit()

    def delete_item(self, item, spider):
        # 删除数据
        self.cursor.execute('DELETE FROM mytable WHERE name = ?', (item['name'],))
        self.conn.commit()
        return item
