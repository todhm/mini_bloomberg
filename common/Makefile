# For details on Makefiles, see the section notes.
NAME:=fpcommon
REPO:=UPDATEME
PASSWORD:=UPDATEME
.PHONY: build deploy

build:
	PYTHONPATH=. python3 setup.py sdist bdist_wheel
deploy:
	PYTHONPATH=. python -m twine upload --skip-existing --repository pypi dist/*