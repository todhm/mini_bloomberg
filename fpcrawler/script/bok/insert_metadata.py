import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from datahandler.metadatahandler import MetaDataHandler

db_name='fp_data'
mdh = MetaDataHandler(db_name)
mdh.insert_all_data()