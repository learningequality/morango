.PHONY: help clean clean-pyc release dist

define BROWSER_PYSCRIPT
import os, webbrowser, sys
try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "release - package and upload a release"
	@echo "dist - package"

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr dist-packages-cache/
	rm -fr dist-packages-temp/
	rm -fr *.egg-info
	rm -fr .eggs
	rm -fr .cache

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

clean-docs:
	$(MAKE) -C docs clean

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint:
	flake8 morango

test:
	pytest tests/testapp/tests/

test-all:
	tox

coverage: ## check code coverage quickly with the default Python
		coverage run --source morango setup.py test
		coverage report -m
		coverage html
		$(BROWSER) htmlcov/index.html

docs: clean-docs
	$(MAKE) -C docs html

browserdocs: docs
	$(BROWSER) docs/_build/html/index.html

servedocs: browserdocs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release:
	ls -l dist/
	echo "\nPress enter to upload everything in dist (CTRL+C to exit).\n" && read yes
	twine upload -s dist/*

dist: clean
	python setup.py sdist --format=gztar
	python setup.py bdist_wheel
	ls -l dist
