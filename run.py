from scrapy import cmdline
import os

from report.utils import get_start_end_date

start_date, end_date = get_start_end_date(180)

scripts = ['index_data.py', 'bond_data.py']
for script in scripts:
    os.system(f'python {script}')

cmdline.execute('scrapy crawl east_report -a codes=* -a count=101 -a begin_time={} -a end_time={}'.format(start_date,
                                                                                                          end_date).split())


