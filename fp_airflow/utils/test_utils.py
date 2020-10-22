import numpy as np
from datetime import datetime as dt


def create_market_data(**kwargs):
    default_data = {
        "Code": "224020",
        "Name": "에스케이씨에스", 
        "Close": 450, 
        "Changes": 0, 
        "ChagesRatio": 0, 
        "Volume": 2107, 
        "Amount": 1000800, 
        "Open": 475, 
        "High": 475, 
        "Low": 450, 
        "Marcap": 1215000000, 
        "MarcapRatio": 0, 
        "Stocks": 2700000, 
        "ForeignShares": np.nan,
        "ForeignRatio": np.nan,
        "Rank": 2479,
        "Date": dt.now()
    }
    default_data.update(kwargs)
    return default_data