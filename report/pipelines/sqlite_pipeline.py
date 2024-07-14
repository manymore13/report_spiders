# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import os
import sqlite3
from sqlite3 import Cursor, Connection

import scrapy
from scrapy.settings import Settings


class ReportSqlitePipeline:
    """研报插入sqlite数据库"""

    def __init__(self):
        self.cursor: Cursor = None
        self.conn: Connection = None
        self.table_name = "report"

    def open_spider(self, spider: scrapy.Spider):
        if hasattr(spider, 'table_name'):
            self.table_name = getattr(spider,'table_name')
        settings: Settings = spider.settings
        sqlite_db_path = settings.get("SQLITE_DB_PATH")
        if not os.path.exists(sqlite_db_path):
            os.makedirs(sqlite_db_path)

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