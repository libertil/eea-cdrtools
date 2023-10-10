# Description

A collection of tools to interact with the European Environment Agency (EEA) Central Data Repository (CDR) API  
in a programmatic way. The collection inlcudes helper functions to make API calls as well as CLI tools to perform   
more complex tasks


## Installation


Available in PyPI.

To start, ensure you have Python 3.8 or newer. To use the package, run:

	pip install --upgrade eea-cdrtools

After installing the package, import it:

	import cdr_tools

It can also be used as a CLI tool by issuing the command:

	cdrtools

in a terminal.

It can also be easily installed as a standalone tool locally using [pipx](https://pypa.github.io/pipx).  

First install pipx 

on macOS

	brew install pipx
	pipx ensurepath
	brew update && brew upgrade pipx


otherwise

	python3 -m pip install --user pipx
	python3 -m pipx ensurepath

then

	pipx install "git+https://github.com/libertil/eea-cdrtools"


Once installed via pip or pipcdrtools is a available at command line. You can get an overview of the functionalities by issuing

	cdrtools --help