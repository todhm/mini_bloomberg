# For details on Makefiles, see the section notes.
NAME:=ml_api
REPO:=UPDATEME
PASSWORD:=UPDATEME
.PHONY: preparedata makeindex

preparedata:
	PYTHONPATH=. python script/prepare_data.py 

makeindex:
	PYTHONPATH=. python script/create_index.py 