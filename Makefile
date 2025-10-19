.PHONY: help run test

PY ?= python

help:
	@echo "Available targets:"
	@echo "  make install - Install dependencies"
	@echo "  make run   - Run main application"
	@echo "  make test  - Run all unit tests"
	@echo "  make test-all - Run analyzer + exception tests"

run:
	$(PY) main.py

install:
	pip install -r requirements.txt

test-analyzer:
	$(PY) -m unittest -v tests.test_traffic_analyzer

test-all:
	$(PY) -m unittest discover -s tests -p 'test_*.py' -v

