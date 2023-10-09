# Description

A collection of tools to interact with the European Environment Agency (EEA) Central Data Repository (CDR) API  
in a programmatic way. The collection inlcudes helper functions to make API calls as well as CLI tools to perform   
more complex tasks


## Installation


Available in PyPI.

It can be easily installed as a standalone tool locally using [pipx](https://pypa.github.io/pipx).  

First install pipx 

on macOS

	brew install pipx
	pipx ensurepath
	brew update && brew upgrade pipx


otherwise

	python3 -m pip install --user pipx
	python3 -m pipx ensurepath

then

	pipx install "git+https://github.com/libertil/cdr-tools"


at this point cdrtools is a available at command line. You can get an overview of the functionalities by issuing

	cdrtools --help