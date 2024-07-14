# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import mimetypes
from pathlib import Path

import scrapy
from scrapy.pipelines.files import FilesPipeline


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