#!/usr/bin/perl -w

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

use strict;
use warnings;

my $num_args = $#ARGV + 1;

sub test_trace
{
    my $full_trace_dir = $ARGV[0];
    my $tmp_dir        = $ARGV[1];

    if (not ($full_trace_dir =~ /traces\/(\d+)_(\w+)\//))
    {
        print "Could not match trace directory name";
        return 1;
    }

    my $nprocs = $1;
    my $trace_name = $2;
    print "Executing 'mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2'\n";
    my @output = qx(mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2);
    my $status = $? >> 8;

    # test that reading OTF2 trace succeeded
    my @running_analysis= grep (/\[(\d+)\] Running analysis/, @output);
    if (not ($#running_analysis + 1 >= 1))
    {
        print "CASITA did not run analysis\n";
        print "@output \n\n";
        return $status;
    }

    # test that the critical path is computed
    my @critical_path= grep (/\[(\d+)\] Critical path length/, @output);
    if (not ($#critical_path + 1 == 1))
    {
        print "CASITA did not compute the critical path\n";
        print "@output \n\n";
        return $status;
    }

    return $status;
}

sub main
{
    if ($num_args != 2)
    {
        print "Invalid number of arguments.\n";
        print "Usage: test_trace.pl trace-dir tmp-dir\n";
        exit 1;
    }

    return test_trace();
}

main();