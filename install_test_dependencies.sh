#!/bin/bash

# Install test and system-level runtime dependencies here.

set -e

if ! dpkg -l python-setuptools > /dev/null 2>&1; then
	sudo apt-get install -y python-setuptools
fi
