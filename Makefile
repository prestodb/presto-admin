.PHONY: clean-pyc clean-build docs clean

help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "test-all - run tests on every Python version with tox"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "release - package and upload a release"
	@echo "dist - package"
	@echo "wheel - build wheel only"
	@echo "install - install the package to the active Python's site-packages"

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr tmp/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -type f -exec rm -rf {} +
	find . -name '*.egg' -type d -exec rm -rf {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint:
	flake8 prestoadmin packaging tests

system-test:
	python setup.py test -s tests.product

test:
	python setup.py test -s tests.unit
	python setup.py test -s tests.integration

test-all:
	tox

coverage:
	coverage run --source prestoadmin setup.py test -s tests.unit
	coverage report -m
	coverage html
	echo `pwd`/htmlcov/index.html

docs:
	rm -f docs/prestoadmin.rst
	rm -f docs/prestoadmin.util.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ prestoadmin
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

open-docs:
	xdg-open docs/_build/html/index.html

release: clean
	python setup.py sdist upload
	python setup.py bdist_wheel upload

dist: clean
	python setup.py bdist_prestoadmin
	ls -l dist

wheel: clean
	python setup.py bdist_wheel
	ls -l dist

install: clean
	python setup.py install
