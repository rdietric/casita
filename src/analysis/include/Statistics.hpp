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

#define STATS_NUMBER 24
enum StatMetric
{
  // offloading
   OFLD_STAT_BLOCKING_COM = 0,       // number of blocking communications
   OFLD_STAT_BLOCKING_COM_TIME  = 1, // accumulated blocking communication time
   OFLD_STAT_EARLY_BLOCKING_WAIT = 2,   // number of early blocking waits
   OFLD_STAT_EARLY_BLOCKING_WTIME  = 3, // accumulated early blocking wait time
   OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL = 4, // accumulated early blocking wait time
   OFLD_STAT_EARLY_TEST = 5,       // number of early tests
   OFLD_STAT_EARLY_TEST_TIME  = 6, // accumulated time of early tests
   OFLD_STAT_IDLE_TIME = 7,         // time an offloading device is idle
   OFLD_STAT_COMPUTE_IDLE_TIME = 8, // compute idle time
   OFLD_STAT_OFLD_TIME = 9, // duration of the offloading interval
   OFLD_STAT_MULTIPLE_COM = 10,      // multiple consecutive communication count
   OFLD_STAT_MULTIPLE_COM_TIME = 11,  // multiple consecutive communication time
     
   //MPI (are written in rules, could also be evaluated in OTF2TraceWriter by 
   //     reading leave node counter values)
   MPI_STAT_LATE_SENDER = 12,       // number of late senders
   MPI_STAT_LATE_SENDER_WTIME = 13, // late sender waiting time
   MPI_STAT_LATE_RECEIVER = 14,       // number of late receivers
   MPI_STAT_LATE_RECEIVER_WTIME = 15, // late receiver waiting time
   MPI_STAT_SENDRECV = 16,
   MPI_STAT_SENDRECV_WTIME = 17,
   MPI_STAT_COLLECTIVE = 18,       // number of (imbalanced) collectives
   MPI_STAT_COLLECTIVE_WTIME = 19, // waiting time in collectives
   MPI_STAT_WAITALL = 20,
   MPI_STAT_WAITALL_WTIME = 21,
   
   //OpenMP
   OMP_STAT_BARRIER = 22,      // OpenMP barriers
   OMP_STAT_BARRIER_WTIME = 23 // waiting time in OpenMP barriers
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
      uint64_t stats[STATS_NUMBER];
      //uint64_t launch_overhead;
      //uint64_t launch_distance;      
      
      // OpenMP
      uint64_t fork_parallel_overhead;
      uint64_t barrier_overhead;
      
    public:
      
      void
      addStatWithCount( StatMetric statType, uint64_t time, 
                                  uint64_t count = 1 )
      {
        stats[ statType ] += count;
        stats[ statType + 1 ] += time;
      }
      
      void
      addStatValue( StatMetric statType, uint64_t value )
      {
        stats[ statType ] += value;
      }
      
      void
      addAllStats( uint64_t* stats )
      {
        //cuda_stats = stats;
        int i;
        for( i = 0; i < STATS_NUMBER; ++i )
        {
          stats[ i ] += stats[ i ];
        }
      }
      
      uint64_t*
      getStats()
      {
        return stats;
      }
  };
}
