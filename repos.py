import os
import subprocess

from report import utils
from report.utils import get_start_end_date, delete_directory_contents

start_date, end_date = get_start_end_date(30)
current_dir = os.getcwd()
print("current_dir = " + current_dir)
target = 'target'
if not os.path.exists(target):
    os.makedirs(target)

delete_directory_contents(target)

if not os.path.exists(target):
    os.makedirs(target)

subprocess.call("cd {} & git clone https://github.com/manymore13/report.git".format(target), shell=True)
subprocess.call("cd " + current_dir, shell=True)
utils.copy_directory_contents(os.path.join(target, "report"), "gen_east_report")
print("done")

# cmdline.execute('scrapy crawl east_report -a codes=* -a count=101 -a begin_time={} -a end_time={}'.format(start_date,
#                                                                                                           end_date).split())
