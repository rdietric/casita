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
        print "Error: Could not match trace directory name";
        return 1;
    }

    my $nprocs = $1;
    my $trace_name = $2;
    print "Executing 'mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2'\n";
    my @output = qx(mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2);
    my $status = $? >> 8;

    if (not ($status == 0))
    {
        print "Error: CASITA returned error ${status}\n";
        print "@output \n\n";
        return $status;
    }

    # test that reading OTF2 trace succeeded
    my @running_analysis = grep (/\[(\d+)\] Running analysis/, @output);
    if (not ($#running_analysis + 1 >= 1))
    {
        print "Error: CASITA did not run analysis\n";
        print "@output \n\n";
        return 1;
    }

    # test that the critical path is computed
    my @critical_path = grep (/\[(\d+)\] Critical path length/, @output);
    if (not ($#critical_path + 1 == 1))
    {
        print "Error: CASITA did not compute the critical path\n";
        print "@output \n\n";
        return 1;
    }

    # check that a optimization report is found
    my @profile = grep (/(\w+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)%\s+(\d+\.\d+)%\s+(\d+\.\d+)/, @output);
    if (not ($#profile + 1 > 0))
    {
        print "Error: Could not find optimization guidance report\n";
        print "@output \n\n";
        return 1;
    }

    # check each line of the optimization report for problems
    foreach (@output)
    {
        my $oline = $_;
        if ($oline =~ /(\w+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)%\s+(\d+\.\d+)%\s+(\d+\.\d+)/)
        {
            my $fname  = $1;
            my $occ    = $2;
            my $time   = $3;
            my $tcp    = $4;
            my $fcp    = $5;
            my $fgb    = $6;
            my $rating = $7;

            if ($time < $tcp)
            {
                print "Error: Invalid profile: time ($time) < time on cp ($tcp)\n";
                print "@output \n\n";
                return 1;
            }

            if ($fcp > 100.0)
            {
                print "Error: Invalid profile: fraction cp > 100% ($fcp)\n";
                print "@output \n\n";
                return 1;
            }

            if ($fgb > 100.0)
            {
                print "Error: Invalid profile: fraction blame > 100% ($fgb)\n";
                print "@output \n\n";
                return 1;
            }

            if ($rating > 2.0)
            {
                print "Error: Invalid profile: rating > 2.0 ($rating)\n";
                print "@output \n\n";
                return 1;
            }
        }
    }

    return $status;
}

sub main
{
    if ($num_args != 2)
    {
        print "Error: Invalid number of arguments.\n";
        print "Usage: test_trace.pl trace-dir tmp-dir\n";
        exit 1;
    }

    my $result = test_trace();
    if (not ($result == 0))
    {
        exit $result;
    }
}

main();