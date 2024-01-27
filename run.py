from scrapy import cmdline

cmdline.execute('scrapy crawl east_report -a codes=538,738 -a page_size=5 -a begin_time=2024-01-20 -a '
                'end_time=2024-01-27 -a page_no=1'.split())
