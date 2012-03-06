#!/bin/bash
export KNEWTON_URI=$1
py.test integration_tests/
exit $?
