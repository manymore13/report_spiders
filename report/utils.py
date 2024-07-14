import datetime
import os
import shutil
import stat


def getNumStr(num):
    num_str = ''
    if num < 10:
        num_str = f"0{num}"
    else:
        num_str = str(num)
    return num_str


def get_start_end_date(days: int = 30):
    now = datetime.datetime.now()
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    return start_date, end_date


def delete_directory_contents(directory_path):
    if not os.path.exists(directory_path):
        return

    def delete(func, path, execinfo):
        os.chmod(path, stat.S_IWUSR)
        func(path)

    shutil.rmtree(directory_path, onerror=delete)


def copy_directory_contents(source_path, destination_path):
    try:
        delete_directory_contents(destination_path)
        # if not os.path.exists(destination_path):
        #     os.makedirs(destination_path)
        # 拷贝源目录到目标目录，保留目录结构
        shutil.copytree(source_path, destination_path)
        print(f"Contents of {source_path} successfully copied to {destination_path}.")
    except Exception as e:
        print(f"An error occurred: {e}")
