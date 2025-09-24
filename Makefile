PY=python3
VENV=.venv
PIP=/bin/pip
PYBIN=/bin/python

:
	 -m venv 
	 install -U pip
	 install -r requirements.txt

install: 

run-csv: install
	 scripts/hh_fetch.py --text 'Product Manager' --areas 1,2 --max-pages 0 --delay 0.5

run-parquet: install
	 scripts/hh_fetch.py --text 'Product Manager' --areas 1 --max-pages 0 --delay 0.5 --parquet --details
