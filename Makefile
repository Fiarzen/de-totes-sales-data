#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-project-totesys
REGION ?= eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}:${WD}/src
SHELL := /bin/bash
PROFILE ?= default
PIP := pip

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source ./venv/bin/activate

## Create Python interpreter environment
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> Checking Python3 version"
	$(PYTHON_INTERPRETER) --version
	@echo ">>> Setting up VirtualEnv."
	@if [ ! -d "./venv" ]; then \
		$(PIP) install -q virtualenv virtualenvwrapper; \
		virtualenv venv --python=$(PYTHON_INTERPRETER); \
	fi

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)
layer-dependencies:
	$(PIP) install -r ./layer-requirements.txt -t dependencies/python
	$(PIP) install pyarrow -t pyarrow/python

################################################################################################################
# Set Up

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Set up dev requirements (bandit, safety, black)
dev-setup: black coverage safety

################################################################################################################
# Build / Run

## Run the black code check
run-black:
	$(call execute_in_env, black --line-length=110 ./src/*)

## Run the coverage check
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run -m pytest tests -vvv)
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage report -m --omit=tests/* --fail-under=0)

flake-8:
	$(call execute_in_env, flake8 src --max-line-length=110)

bandit:
	$(call execute_in_env, bandit -r src/*)

## Run all checks
run-checks: flake-8 run-black bandit unit-test
