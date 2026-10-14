install:
	pip install -r requirements.txt
run:            ## полный цикл: загрузка + staging + marts
	python -m pipeline run
load:
	python -m pipeline run --stage load
transform:
	python -m pipeline run --stage transform
status:
	python -m pipeline status
test:
	pytest -q
