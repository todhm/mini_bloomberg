from datetime import timedelta, date
from datetime import datetime as dt
import time
from workalendar.asia import SouthKorea



def timestamp(datett=None):
    if not datett:
        datett = dt.now()
    return int(time.mktime(datett.timetuple()))

def daterange(start_date, end_date, diffrange):
    date_diff = (end_date - start_date).days
    for n in range(0, int(date_diff), diffrange):
        start = start_date + timedelta(n)
        if n + (diffrange - 1) < date_diff:
            end = start_date + timedelta(n+(diffrange - 1))
        else:
            end = start_date + timedelta(date_diff)
        yield start, end


def get_kbd_diff(start_date,end_date):
    get_diff_sum = 0 
    cal = SouthKorea()
    for (start,end) in daterange(start_date,end_date,1):
        if cal.is_working_day(start):
            get_diff_sum += 1
    return get_diff_sum


