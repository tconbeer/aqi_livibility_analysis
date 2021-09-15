.PHONY: test
test:
	pytest
	isort .
	black .
	flake8 .
	mypy .
