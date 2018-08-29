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

#define STATS_NUMBER 26
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
   OFLD_STAT_KERNEL_START_DELAY = 12,
   OFLD_STAT_KERNEL_START_DELAY_TIME = 13,
     
   //MPI (are written in rules, could also be evaluated in OTF2TraceWriter by 
   //     reading leave node counter values)
   MPI_STAT_LATE_SENDER = 14,       // number of late senders
   MPI_STAT_LATE_SENDER_WTIME = 15, // late sender waiting time
   MPI_STAT_LATE_RECEIVER = 16,       // number of late receivers
   MPI_STAT_LATE_RECEIVER_WTIME = 17, // late receiver waiting time
   MPI_STAT_SENDRECV = 18,
   MPI_STAT_SENDRECV_WTIME = 19,
   MPI_STAT_COLLECTIVE = 20,       // number of (unbalanced) collectives
   MPI_STAT_COLLECTIVE_WTIME = 21, // waiting time in collectives
   MPI_STAT_WAITALL_LATEPARTNER = 22,
   MPI_STAT_WAITALL_LATEPARTNER_WTIME = 23,
   
   //OpenMP
   OMP_STAT_BARRIER = 24,      // OpenMP barriers
   OMP_STAT_BARRIER_WTIME = 25 // waiting time in OpenMP barriers
};

#define ACTIVITY_TYPE_NUMBER 10
enum ActivityType
{
  // offloading
   OFLD_KERNEL = 0,
   OFLD_SYNC = 1,
   OFLD_EVT_RECORD = 2,
   OFLD_EVT_QUERY = 3,
   OFLD_EVT_SYNC = 4,
   MPI_SEND  = 5,
   MPI_RECV = 6,
   MPI_WAIT = 7,
   OMP_FORK = 8,
   OMP_BARRIER = 9
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
      
      // communication roofline
      uint64_t fastest_communication; //\todo: per type (size)
      uint64_t avg_communication; //\todo: per type (size)
      
      // inefficiencies
      uint64_t stats[ STATS_NUMBER ];
      
      // occurrences 
      uint64_t occurrences[ ACTIVITY_TYPE_NUMBER ];
      
      // OpenMP
      uint64_t fork_parallel_overhead;
      uint64_t barrier_overhead;
      
    public:
      
      uint64_t mpiBlockingComCounter;
      uint64_t mpiNonBlockingComCounter;
      uint64_t mpiBlockingCollectiveCounter;
      uint64_t ofldKernelCounter;
      uint64_t ofldTransferCounter;
      uint64_t ofldSyncCounter;
      
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
          this->stats[ i ] += stats[ i ];
        }
      }
      
      uint64_t*
      getStats()
      {
        return stats;
      }
  };
}
