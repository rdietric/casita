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
    my $casita         = $ARGV[1];
    my $tmp_dir        = $ARGV[2];
    my $otf2_print;

    if ($num_args > 3)
    {
        $otf2_print = $ARGV[3];
    }

    if (not ($full_trace_dir =~ /traces\/(\d+)_([-\w]+)\//))
    {
        print "Error: Could not match trace directory name for ${full_trace_dir}\n";
        return 1;
    }

    my $nprocs = $1;
    my $trace_name = $2;
    print "Executing 'mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2 --verbose=1'\n";
    my @output = qx(mpirun -n $nprocs casita ${full_trace_dir}/traces.otf2 -o $tmp_dir/${trace_name}.otf2 --verbose=1 2>&1);
    my $status = $? >> 8;

    if (not ($status == 0))
    {
        print "@output \n\n";
        print "Error: CASITA returned error ${status}\n";
        return $status;
    }

    # test that reading OTF2 trace succeeded
    my @running_analysis = grep (/Running CASITA/, @output);
    if (not ($#running_analysis + 1 >= 1))
    {
        print "@output \n\n";
        print "Error: CASITA did not run analysis\n";
        return 1;
    }

    # check that a optimization report is found
    my @profile = grep (/(\w+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)/, @output);
    
    if (not ($#profile + 1 > 0))
    {
        print "@output \n\n";
        print "Error: Could not find optimization guidance report\n";
        return 1;
    }

    my $fcp_total = 0.0;
    my $fgb_total = 0.0;

    # check each line of the optimization report for problems
    foreach (@output)
    {
        my $oline = $_;
        
        if ($oline =~ /(.+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)/)
        {
            my $fname  = $1;
            my $occ    = $2;
            my $time   = $3;
            my $tcp    = $4;
            my $fcp    = $5;  # CP ratio
            my $fgb    = $6;  # blame ratio
            my $rating = $7;  # blame + CP
            my $bcp    = $8;  # blame on CP
            
            #print "$fname,$occ,$time,$tcp,$fcp,$fgb,$rating\n";

            if ($time < $tcp)
            {
                print "@output \n\n";
                print "Error: Invalid profile: time ($time) < time on cp ($tcp, $fname)\n";
                return 1;
            }

            if ($fcp > 100.5)
            {
                print "@output \n\n";
                print "Error: Invalid profile: fraction cp > 100% ($fcp, $fname)\n";
                return 1;
            }

            if ($fgb > 100.5)
            {
                print "@output \n\n";
                print "Error: Invalid profile: fraction blame > 100% ($fgb, $fname)\n";
                return 1;
            }

            if ($rating > 2.0)
            {
                print "@output \n\n";
                print "Error: Invalid profile: rating > 2.0 ($rating, $fname)\n";
                return 1;
            }

            $fcp_total += $fcp;
            $fgb_total += $fgb;
        }
    }

    if ($fcp_total > 105.0)
    {
        print "@output \n\n";
        print "Warning: Invalid profile: total fraction cp > 105% ($fcp_total)\n";
        #return 1;
    }

    if ($fcp_total < 70.0)
    {
        print "@output \n\n";
        print "Warning: total printed fraction cp < 70% ($fcp_total)\n";
        if( $fcp_total == 0 )
        {
           return 1;
        }
    }

    if ($fgb_total > 100.5)
    {
        print "@output \n\n";
        print "Error: Invalid profile: total fraction blame > 100% ($fgb_total)\n";
        return 1;
    }

    if ($fgb_total < 70.0)
    {
        print "@output \n\n";
        print "Warning: total printed fraction blame < 70% ($fgb_total)\n";
        if( $fgb_total == 0 )
        {
           return 1;
        }
    }

    # run otf2-print on trace
    if ($num_args > 3 && length $otf2_print > 0)
    {
        # check trace file size
        my $otf2_size = qx(du -s ${full_trace_dir});
        if ( $otf2_size =~ m/^(\d+)*/ ) {
          if( $1 > 300000 ){
            print "Input trace ($1 K) is too large  for validation check!\n";
            return 0;
          }
        }
        
    
        my @otf2_output = qx($otf2_print $tmp_dir/${trace_name}.otf2 2>&1);
        my $otf2_status = $? >> 8;
        if (not ($otf2_status == 0))
        {
            print "Error: Could not run otf2-print on output trace\n";
            return $status;
        }

        # check that output trace contains metrics
        my @metrics = grep (/^METRIC/, @otf2_output);
        if (not ($#metrics + 1 > 0))
        {
            print "@otf2_output \n\n";
            print "Error: Could not find any METRICs in output trace\n";
            return 1;
        }
    } else
    {
        print "Warning: No otf2-print, skipping output trace tests\n";
    }

    return 0;
}

sub main
{
    if ($num_args < 3)
    {
        print "Error: Invalid number of arguments.\n";
        print "Usage: test_trace.pl <casita-binary> <trace-dir> <tmp-dir> [<otf2-binary>]\n";
        exit 1;
    }

    my $result = test_trace();
    if (not ($result == 0))
    {
        exit $result;
    }
}

main();
