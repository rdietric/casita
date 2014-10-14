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
PERL=perl
TEST_SCRIPT=test_trace.pl
TRACE_INPUT_DIR=traces
NUM_TESTS_SUCCESS=0
NUM_TOTAL_TESTS=0
TRACE_OUTPUT_DIR=

# functions

function check_setup {
    # find casita
    command -v $EXE &> /dev/null || { echo "Could not find CASITA executable, abort." >&2; return 1; }
    command -v $PERL &> /dev/null || { echo "Could not find perl executable, abort." >&2; return 1; }

    # try to run casita
    $EXE --help | grep "casita" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "CASITA does not seem to work, abort." >&2
        return 1
    fi

    # try to run perl
    $PERL --help | grep "perl" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "perl does not seem to work, abort." >&2
        return 1
    fi

    # find perl test script
    if [ ! -f "$TEST_SCRIPT" ]; then
        echo "Did not find perl test script '$TEST_SCRIPT', abort." >&2
        return 1
    fi

    # check that we have traces to test
    if [ ! -d "traces" ]; then
        echo "Did not find test traces directory, abort." >&2
        return 1
    fi

    return 0
}

function run_single_test {
    echo "Testing '$1'" >&2

    $PERL $TEST_SCRIPT $1 $TRACE_OUTPUT_DIR
}

function run_tests {
    for dir in $(ls -d traces/*/); do
        ((NUM_TOTAL_TESTS += 1))
        run_single_test $dir
        if [ $? -eq 0 ]; then
            ((NUM_TESTS_SUCCESS += 1))
        fi
    done

    return 0
}


# main
check_setup
if [ $? -ne 0 ]; then
    exit 1
fi

# create tmp dir for tests
TRACE_OUTPUT_DIR=`mktemp -d`
echo "Created temporary dir '$TRACE_OUTPUT_DIR' for tests" >&2

# run tests

run_tests
echo "Tests: $NUM_TESTS_SUCCESS/$NUM_TOTAL_TESTS" >&2
if [ $NUM_TESTS_SUCCESS -ne $NUM_TOTAL_TESTS ]; then
    echo "FAILED" >&2
else
    echo "SUCCESS" >&2
fi

# cleanup
rm -rf $TRACE_OUTPUT_DIR
