import datetime

from scrapy import cmdline

now = datetime.datetime.now()
end_date = now.strftime("%Y-%m-%d")
start_date = (now - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

cmdline.execute('scrapy crawl east_report -a codes=* -a count=101 -a begin_time={} -a end_time={}'.format(start_date,
                                                                                                          end_date).split())
