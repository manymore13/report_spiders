import datetime


def get_start_end_date(days: int = 30):
    now = datetime.datetime.now()
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    return start_date, end_date
