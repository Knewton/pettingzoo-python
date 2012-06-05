#!/bin/bash

set -e

virtualenv_name=pettingzoo
package_name=pettingzoo

usage()
{
	cat << EOF
	usage: $0 options

	Run tests for this service.

	OPTIONS:
		-h  Show this message
EOF
}

unit=1
while getopts "hi" opt;
do
	case $opt in
		h)
			usage
			exit 1
			;;
		?)
			usage
			exit 1
	esac
done

. /opt/virtualenvs/${virtualenv_name}/bin/activate

find . -name '*.pyc' -exec rm {} \;

python ./runtests.py --cov-report html --cov-report term --cov ${package_name}
deactivate
