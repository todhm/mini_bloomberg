from datetime import datetime as dt
from pytz import timezone


def get_now_time():
    KST = dt.now(timezone('Asia/Seoul'))
    return KST
