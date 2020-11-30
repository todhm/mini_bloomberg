from datetime import datetime
from typing import List
import pendulum


def chunkIt(seq: List, num: int):
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def current_time() -> datetime:
    local_tz = pendulum.timezone("Asia/Seoul")
    thisYear = datetime.now().year 
    thisMonth = datetime.now().month
    thisDate = datetime.now().day
    return datetime(thisYear, thisMonth, thisDate, tzinfo=local_tz).now()