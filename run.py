from scrapy import cmdline

cmdline.execute('scrapy crawl east_report -a codes=538 -a count=300 -a begin_time=2024-01-28 -a '
                'end_time=2024-01-28'.split())


# cmdline.execute('scrapy crawl east_report -a codes=538 -a count=100 -a begin_time=2024-01-28 -a '
#                 'end_time=2024-01-28'.split())