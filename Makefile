.PHONY: clean-all clean clean-eggs clean-build clean-pyc clean-test-containers clean-test \
	clean-docs lint smoke test test-all test-images test-rpm docker-images coverage docs \
	open-docs release release-builds dist dist-online dist-offline wheel install precommit \
	clean-test-all smoke-configurable-cluster test-all-configurable-cluster _clean_tmp

help:
	@echo "precommit - run \`quick' tests and tasks that should pass or succeed prior to pushing"
	@echo "clean-all - clean everything; effectively resets repo as if it was just checked out"
	@echo "clean - remove build, test, coverage and Python artifacts except for the cache Presto RPM"
	@echo "clean-eggs - remove *.egg and *.egg-info files and directories"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test-containers - remove Docker containers used during tests"
	@echo "clean-test - remove test and coverage artifacts for unit and integration tests"
	@echo "clean-test-all - remove test and coverage artifacts for all tests"
	@echo "clean-docs - remove doc artifacts"
	@echo "lint - check style with flake8"
	@echo "smoke - run tests annotated with attr smoke using nosetests"
	@echo "smoke-configurable-cluster - same target as smoke but doesn't build the Docker images as the tests will run on a configurable cluster"
	@echo "test - run tests quickly with Python 2.6 and 2.7"
	@echo "test-all - run tests on every Python version with tox. Specify TEST_SUITE env variable to run only a given suite."
	@echo "test-all-configurable-cluster - same target as test-all but doesn't build the Docker images as the tests will run on a configurable cluster"
	@echo "test-images - create product test image(s). Specify IMAGE_NAMES env variable to create only certain images."
	@echo "test-rpm - run tests for the RPM package"
	@echo "docker-images - pull docker image(s). Specify DOCKER_IMAGE_NAME env variable for specific image."
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "open-docs - open the root document (index.html) using xdg-open"
	@echo "release - package and upload a release"
	@echo "release-builds - run all targets associated with a release (clean-build clean-pyc dist dist-online docs)"
	@echo "dist - package and build installer that requires an Internet connection"
	@echo "dist-online - package and build installer that requires an Internet connection"
	@echo "dist-offline - package and build installer that does not require an Internet connection"
	@echo "wheel - build wheel only"
	@echo "install - install the package to the active Python's site-packages"

precommit: clean dist lint docs test

clean-all: clean
	rm -f presto*.rpm

clean: clean-build clean-pyc clean-test-all clean-eggs clean-docs

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

clean-test-all: clean-test _clean_tmp
	for image in $$(docker images | awk '/teradatalabs\/pa_test/ {print $$1}'); do docker rmi -f $$image ; done
	@echo "\n\tYou can kill running containers that caused errors removing images by running \`make clean-test-containers'\n"

_clean_tmp:
	rm -rf tmp

clean-docs:
	rm -rf docs/prestoadmin.*
	rm -f docs/modules.rst
	rm -rf docs/_build

lint:
	flake8 prestoadmin packaging tests

TEST_PRESTO_RPM_URL?=https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=com.facebook.presto&a=presto-server-rpm&e=rpm&v=RELEASE

presto-server-rpm.rpm:
	if echo '${TEST_PRESTO_RPM_URL}' | grep -q '^http'; then       \
		echo "Downloading presto-rpm from ${TEST_PRESTO_RPM_URL}";   \
		wget -q '${TEST_PRESTO_RPM_URL}' -O $@;                      \
	else                                                           \
		echo "Using local presto-rpm from ${TEST_PRESTO_RPM_URL}";   \
		cp '${TEST_PRESTO_RPM_URL}' $@;                              \
	fi

smoke: clean-test-all test-images _smoke

# Configurable cluster requires the base Docker images to build the
# presto-admin installer
smoke-configurable-cluster: clean-test _clean_tmp docker-images _smoke

_smoke:
	tox -e py26 -- -a smoketest,'!quarantine'

test: clean-test
	tox -- -s tests.unit
	tox -- -s tests.integration

TEST_SUITE?=tests.product

test-all: clean-test-all test-images _test-all

# Configurable cluster requires the base Docker images to build the
# presto-admin installer
test-all-configurable-cluster: clean-test _clean_tmp docker-images _test-all

_test-all:
	tox -- -s tests.unit
	tox -- -s tests.integration
	tox -e py26 -- -s ${TEST_SUITE} -a '!quarantine'

# Can take any space-separated combination of:
# standalone_presto, standalone_presto_admin, standalone_bare,
# yarn_slider_presto_admin, all
IMAGE_NAMES?="all"

#
# The build process and product tests rely on several base Docker images.
# Teradata builds and releases a number of Docker images from the same
# repository, all versioned together. This makes it simple to verify that your
# test environment is sane: if all of the images are the same version, they
# should work together.
#
# As part of the process of releasing those images, we tag all of the images
# with the version number of the release. This means that anything that uses
# the images can reference them as `teradatalabs/image_name:version'. The
# Makefile needs to know that to pull the images, and the python code needs to
# know that for various reasons.
#
# base-images-tag.json is the canonical source of the tag information for the
# repository. The python code parses it properly with the json module, and the
# Makefile parses it adequately with awk ;-)
#
BASE_IMAGES_TAG := $(shell awk '/base_images_tag/ \
	{split($$NF, a, "\""); print a[2]}' base-images-tag.json)

test-images: docker-images presto-server-rpm.rpm
	python tests/product/image_builder.py $(IMAGE_NAMES)

DOCKER_IMAGES := \
	teradatalabs/centos6-ssh-oj8:$(BASE_IMAGES_TAG)

docker-images:
	for image in $(DOCKER_IMAGES); do docker pull $$image || exit 1; done

test-rpm: clean-test-all test-images
	tox -e py26 -- -s tests.rpm -a '!quarantine'

coverage:
	coverage run --source prestoadmin setup.py test -s tests.unit
	coverage report -m
	coverage html
	echo `pwd`/htmlcov/index.html

docs: clean-docs
	sphinx-apidoc -o docs/ prestoadmin
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

open-docs:
	xdg-open docs/_build/html/index.html

release: clean
	python setup.py sdist upload -r pypi_internal
	python setup.py bdist_wheel upload -r pypi_internal

release-builds: clean-build clean-pyc dist dist-offline docs

dist: dist-online

dist-online: clean-build clean-pyc
	python setup.py bdist_prestoadmin --online-install
	ls -l dist

dist-offline: clean-build clean-pyc
	python setup.py bdist_prestoadmin
	ls -l dist

wheel: clean
	python setup.py bdist_wheel
	ls -l dist

install: clean
	python setup.py install
