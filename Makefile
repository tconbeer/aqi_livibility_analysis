.PHONY: test
test:
	pytest -m "not (slow or gcs)"
	isort .
	black .
	flake8 .
	mypy .

.PHONY: test_all
test_all:
	pytest