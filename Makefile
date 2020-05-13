# Makefile for packaging and testing pytest_pgsql
#
# This Makefile has the following targets:
#
# pyenv - Sets up pyenv and a virtualenv that is automatically used
# deactivate_pyenv - Deactivates the pyenv setup
# dependencies - Installs all dependencies for a project (including mac dependencies)
# setup - Sets up the entire development environment (pyenv and dependencies)
# clean_docs - Clean the documentation folder
# clean - Clean any generated files (including documentation)
# open_docs - Open any docs generated with "make docs"
# docs - Generated sphinx docs
# validate - Run code validation
# test - Run tests
# run - Run any services for local development (databases, CSS compiliation, airflow, etc)
# version - Show the version of the package

OS = $(shell uname -s)

PACKAGE_NAME=pytest_pgsql
MODULE_NAME=pytest_pgsql

ifdef CIRCLECI
TOX_POSARGS=-- --junitxml={env:CIRCLE_TEST_REPORTS}/pytest_pgsql/junit.xml
# Use CircleCIs version
PYTHON_VERSION=
# Dont log pip install output since it can print the private repo url
PIP_INSTALL_CMD=pip install -q
# Do local installs without editable mode because of issues with CircleCI's venv
PIP_LOCAL_INSTALL_CMD=pip install -q .
else
TOX_POSARGS=
PIP_INSTALL_CMD=pip install
PIP_LOCAL_INSTALL_CMD=pip install -e .
endif


# Print usage of main targets when user types "make" or "make help"
help:
	@echo "Please choose one of the following targets: \n"\
	      "    setup: Setup your development environment and install dependencies\n"\
	      "    test: Run tests\n"\
	      "    validate: Validate code and documentation\n"\
	      "    docs: Build Sphinx documentation\n"\
	      "    open_docs: Open built documentation\n"\
	      "\n"\
	      "View the Makefile for more documentation about all of the available commands"
	@exit 2


# Sets up pyenv and the virtualenv that is managed by pyenv
.PHONY: pyenv
pyenv:
ifeq (${OS}, Darwin)
	brew install pyenv pyenv-virtualenv 2> /dev/null || true
# Ensure we remain up to date with pyenv so that new python versions are available for installation
	brew upgrade pyenv pyenv-virtualenv 2> /dev/null || true
endif

	# Install all supported Python versions. There are more recent patch releases
	# for most of these but CircleCI doesn't have them preinstalled. Installing a
	# version of Python that isn't preinstalled slows down the build significantly.
	#
	# If you don't have these installed yet it's going to take a long time, but
	# you'll only need to do it once.
	pyenv install -s 3.6.2
	pyenv install -s 3.5.2
	pyenv install -s 3.4.4

	# Set up the environments for Tox
	pyenv local 3.6.2 3.5.2 3.4.4


# Deactivates pyenv and removes it from auto-using the virtualenv
.PHONY: deactivate_pyenv
deactivate_pyenv:
	rm .python-version


# Builds all dependencies for a project
.PHONY: dependencies
dependencies:
	${PIP_INSTALL_CMD} -U -r dev_requirements.txt  # Use -U to ensure requirements are upgraded every time
	${PIP_INSTALL_CMD} -r test_requirements.txt
	${PIP_LOCAL_INSTALL_CMD}
	pip check


# Performs the full development environment setup
.PHONY: setup
setup: pyenv dependencies


# Clean the documentation folder
.PHONY: clean_docs
clean_docs:
	cd docs && make clean


# Clean any auto-generated files
.PHONY: clean
clean: clean_docs
	python setup.py clean
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg*/
	rm -rf __pycache__/
	rm -f MANIFEST
	find ${PACKAGE_NAME} -type f -name '*.pyc' -delete
	rm -rf coverage .coverage .coverage*


# Open the build docs (only works on Mac)
.PHONY: open_docs
open_docs:
	open docs/_build/html/index.html


# Build Sphinx autodocs
.PHONY: docs
docs: clean_docs  # Ensure docs are clean, otherwise weird render errors can result
	sphinx-apidoc -f -e -M -o docs/ pytest_pgsql 'pytest_pgsql/tests' 'pytest_pgsql/version.py' && cd docs && make html

# Run code validation
.PHONY: validate
validate:
	flake8 -v ${MODULE_NAME}/
	pylint ${MODULE_NAME}
	make docs  # Ensure docs can be built during validation


# Run tests
.PHONY: test
test:
	tox ${TOX_POSARGS}
	coverage report

.PHONY: test_single_version
test_single_version:
	coverage run -a -m pytest --pg-conf-opt="track_commit_timestamp=True" --pg-extensions=btree_gin,,btree_gist pytest_pgsql/tests


# Run any services for local development. For example, docker databases, CSS compilation watching, etc
.PHONY: run
run:
	@echo "No services need to be running for local development"


# Distribution helpers for determining the version of the package
VERSION=$(shell python setup.py --version | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\).*$$/\1/')

.PHONY: version
version:
	@echo ${VERSION}
