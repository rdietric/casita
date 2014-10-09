#!/bin/bash

#
# This file is part of the CASITA software
#
# Copyright (c) 2014,
# Technische Universitaet Dresden, Germany
#
# This software may be modified and distributed under the terms of
# a BSD-style license. See the COPYING file in the package base
# directory for details.
#

# set some variables
EXE=casita
TRACE_INPUT_DIR=traces
NUM_TESTS=0
NUM_TOTAL_TESTS=0

# functions

function check_setup () {
    # find casita
    command -v $EXE &> /dev/null || { echo "Could not find CASITA executable, abort." >&2; return 1; }

    # try to run casita
    $EXE --help | grep "casita" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "CASITA does not seem to work, abort." >&2
        return 1
    fi

    # check that we have traces to test
    if [ ! -d "traces" ]; then
        echo "Did not find test traces directory, abort." >&2
        return 1
    fi

    return 0
}

function run_tests () {
    return 0
}


# main
if [[ $(check_setup) -ne 0 ]]; then
    exit 1
fi

# create tmp dir for tests
TRACE_OUTPUT_DIR=`mktemp -d`
echo "Created temporary dir '$TRACE_OUTPUT_DIR' for tests" >&2

# run tests

res=$(run_tests)
echo "Tests: $NUM_TESTS/$NUM_TOTAL_TESTS" >&2
if [[ $res -ne 0 ]]; then
    echo "FAILED" >&2
else
    echo "SUCCESS" >&2
fi

# cleanup
rm -rf $TRACE_OUTPUT_DIR
