import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from datahandler.bokstathandler import BokStatHandler

db_name='fp_data'
bsh = BokStatHandler(db_name)
stats_list = [
    '060Y001','028Y001','028Y022',
    '028Y007','027Y601','036Y001',
    '036Y002','036Y003','036Y004',
    '036Y005','036Y006','085Y013',
    '085Y014','102Y053','102Y037'
    ]
bsh.insert_stats_assigned(stats_list)