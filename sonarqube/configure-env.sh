#!/bin/bash


	##########################
	# Setup build environment
	##########################
	SetupEnv(){
	  echo "current place :" pwd
	  sudo apt-get -y install python3-pip libpq-dev python3-dev
    sudo python3 -m pip install --upgrade pip setuptools wheel

	  echo "Install dependencies"
	  sudo python3 -m pip install -r tests/requirements_dev.txt
	  sudo python3 -m pip install pytest-cov
	}


	##################
	# Add build steps
	##################
	GenerateReports(){
    echo "Create Coverage report"
    python3 -m py.test --cov barman --cov-report xml:coverage-reports/coverage.xml --junitxml=coverage-reports/results.xml
	}


	########
	# Main
	########
	SetupEnv
	GenerateReports
