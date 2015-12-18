.PHONY: clean-pyc clean-build docs clean

help:
	@echo "clean-all - clean everything; effectively resets repo as if it was just checked out"
	@echo "clean - remove build, test, coverage and Python artifacts except for the cache Presto RPM"
	@echo "clean-eggs - remove *.egg and *.egg-info files and directories"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "smoke - run tests annotated with attr smoke using nosetests"
	@echo "test - run tests quickly with Python 2.6 and 2.7"
	@echo "test-all - run tests on every Python version with tox. Specify TEST_SUITE env variable to run only a given suite."
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "release - package and upload a release"
	@echo "dist - package and build installer that can be used offline"
	@echo "dist-online - package and build installer that requires an Internet connection"
	@echo "wheel - build wheel only"
	@echo "install - install the package to the active Python's site-packages"

clean-all: clean
	rm -f presto*.rpm

clean: clean-build clean-pyc clean-test clean-eggs

clean-eggs:
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -type f -exec rm -rf {} +
	find . -name '*.egg' -type d -exec rm -rf {} +

clean-build:
	rm -fr build/
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test-containers:
	 for c in $$(docker ps --format "{{.ID}} {{.Image}}" | awk '/teradatalabs\/pa_test/ { print $$1 }'); do docker kill $$c; done

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr tmp
	-for image in $$(docker images | awk '/teradatalabs\/pa_test/ {print $$1}'); do docker rmi -f $$image ; done
	@echo "\n\tYou can kill running containers that caused errors removing images by running \`make clean-test-containers'\n"

lint:
	flake8 prestoadmin packaging tests

smoke: clean-test
	tox -e py26 -- -a smoketest,'!quarantine'

test: clean-test
	tox -- -s tests.unit
	tox -- -s tests.integration

TEST_SUITE?=tests.product

test-all: clean-test
	tox -- -s tests.unit
	tox -- -s tests.integration
	tox -e py26 -- -s ${TEST_SUITE} -a '!quarantine'

test-rpm: clean-test
	tox -e py26 -- -s tests.rpm -a '!quarantine'

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
	python setup.py sdist upload -r pypi_internal
	python setup.py bdist_wheel upload -r pypi_internal

release-builds: clean-build clean-pyc dist dist-online

dist: clean-build clean-pyc
	python setup.py bdist_prestoadmin
	ls -l dist

dist-online: clean-build clean-pyc
	python setup.py bdist_prestoadmin --online-install
	ls -l dist

wheel: clean
	python setup.py bdist_wheel
	ls -l dist

install: clean
	python setup.py install
