from scrapy import cmdline
import subprocess

from report.utils import get_start_end_date

start_date, end_date = get_start_end_date(180)

subprocess.run('python index_data.py', shell=True)
subprocess.run('python bond_data.py', shell=True)
cmdline.execute('scrapy crawl east_report -a codes=* -a count=101 -a begin_time={} -a end_time={}'.format(start_date,
                                                                                                          end_date).split())


