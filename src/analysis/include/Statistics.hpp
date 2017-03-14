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
#define STATS_CUDA 6
enum StatsCUDA
{
   CUDA_STAT_BLOCKING_COMM = 0,
   CUDA_STAT_BLOCKING_COMM_TIME  = 1,
   CUDA_STAT_EARLY_BLOCKING_SYNC = 2,
   CUDA_STAT_EARLY_BLOCKING_SYNC_TIME  = 3,
   CUDA_STAT_EARLY_QUERY = 4,
   CUDA_STAT_EARLY_QUERY_TIME  = 5
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
      
      // CUDA
      uint64_t cuda_stats[STATS_CUDA];
      //uint64_t launch_overhead;
      //uint64_t launch_distance;      
      
      // OpenMP
      uint64_t fork_parallel_overhead;
      uint64_t barrier_overhead;
      
    public:
      
      void
      addStatCUDA( StatsCUDA statType, uint64_t waiting_time, uint64_t count = 1 )
      {
        cuda_stats[ statType ] += count;
        cuda_stats[ statType + 1 ] += waiting_time;
      }
      
      void
      addAllStatsCUDA( uint64_t* stats )
      {
        //cuda_stats = stats;
        int i;
        for( i = 0; i < STATS_CUDA; ++i )
        {
          cuda_stats[ i ] += stats[ i ];
        }
      }
      
      uint64_t*
      getStatsCUDA()
      {
        return cuda_stats;
      }
  };
}
