/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016-2017,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

// This class collects simple statistics

#pragma once

#include <inttypes.h>

// CUDA stats have always a count and in the following field the time
#define STATS_OFFLOADING 11
enum StatsOffloading
{
   OFLD_STAT_BLOCKING_COM = 0,       // number of blocking communications
   OFLD_STAT_BLOCKING_COM_TIME  = 1, // accumulated blocking communication time
   OFLD_STAT_EARLY_BLOCKING_WAIT = 2,   // number of early blocking waits
   OFLD_STAT_EARLY_BLOCKING_WTIME  = 3, // accumulated early blocking wait time
   OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL = 4, // accumulated early blocking wait time
   OFLD_STAT_EARLY_TEST = 5,       // number of early tests
   OFLD_STAT_EARLY_TEST_TIME  = 6, // accumulated time of early tests
   OFLD_STAT_IDLE_TIME = 7,         // time an offloading device is idle
   OFLD_STAT_COMPUTE_IDLE_TIME = 8, // compute idle time
   OFLD_STAT_MULTIPLE_COM = 9,      // multiple consecutive communication count
   OFLD_STAT_MULTIPLE_COM_TIME = 10  // multiple consecutive communication time
};

namespace casita
{
  class Statistics
  {
    public:
      Statistics();
      
      virtual
      ~Statistics();
     
    private:
      /////// important metrics ////////
      
      // MPI
      uint64_t mpi_blocking_communication;
      
      // communication roofline
      uint64_t fastest_communication; //\todo: per type (size)
      uint64_t avg_communication; //\todo: per type (size)
      
      // offloading
      uint64_t offloading_stats[STATS_OFFLOADING];
      //uint64_t launch_overhead;
      //uint64_t launch_distance;      
      
      // OpenMP
      uint64_t fork_parallel_overhead;
      uint64_t barrier_overhead;
      
    public:
      
      void
      addStatWithCountOffloading( StatsOffloading statType, uint64_t time, 
                                  uint64_t count = 1 )
      {
        offloading_stats[ statType ] += count;
        offloading_stats[ statType + 1 ] += time;
      }
      
      void
      addStatValueOffloading( StatsOffloading statType, uint64_t value )
      {
        offloading_stats[ statType ] += value;
      }
      
      void
      addAllStatsOffloading( uint64_t* stats )
      {
        //cuda_stats = stats;
        int i;
        for( i = 0; i < STATS_OFFLOADING; ++i )
        {
          offloading_stats[ i ] += stats[ i ];
        }
      }
      
      uint64_t*
      getStatsOffloading()
      {
        return offloading_stats;
      }
  };
}
