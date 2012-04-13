#!/bin/bash

# Install test and system-level runtime dependencies here.

set -e

sudo apt-get install -y python-setuptools
sudo pip install pytest
sudo pip install coverage
sudo pip install pytest_cov
