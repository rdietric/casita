/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016-2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

// This class collects simple statistics

#pragma once

#include <inttypes.h>

namespace casita
{
  enum StatMetric
  {
    // offloading
     STAT_OFLD_BLOCKING_COM = 0,       // number of blocking communications
     STAT_OFLD_BLOCKING_COM_TIME  = 1, // accumulated blocking communication time
     OFLD_STAT_BLOCKING_COM_EXCL_TIME = 2, // time a blocking communication is communicating
     OFLD_STAT_EARLY_BLOCKING_WAIT = 3,   // number of early blocking waits
     OFLD_STAT_EARLY_BLOCKING_WTIME = 4,  // accumulated early blocking wait time
     OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL = 5, // accumulated early blocking wait time on kernel
     OFLD_STAT_EARLY_TEST = 6,       // number of early tests
     OFLD_STAT_EARLY_TEST_TIME  = 7, // accumulated time of early tests
     OFLD_STAT_IDLE_TIME = 8,         // time an offloading device is idle
     OFLD_STAT_COMPUTE_IDLE_TIME = 9, // compute idle time
     OFLD_STAT_OFLD_TIME = 10, // duration of the offloading interval
     OFLD_STAT_MULTIPLE_COM = 11,      // multiple consecutive communication count
     OFLD_STAT_MULTIPLE_COM_TIME = 12,  // multiple consecutive communication time
     OFLD_STAT_MULTIPLE_COM_SD = 13,      // multiple consecutive communication count
     OFLD_STAT_MULTIPLE_COM_SD_TIME = 14,  // multiple consecutive communication time
     OFLD_STAT_KERNEL_START_DELAY = 15,
     OFLD_STAT_KERNEL_START_DELAY_TIME = 16,
     STAT_OFLD_COMPUTE_OVERLAP_TIME = 17,

     //MPI (are written in rules, could also be evaluated in OTF2TraceWriter by 
     //     reading leave node counter values)
     MPI_STAT_LATE_SENDER = 18,       // number of late senders
     MPI_STAT_LATE_SENDER_WTIME = 19, // late sender waiting time
     MPI_STAT_LATE_RECEIVER = 20,       // number of late receivers
     MPI_STAT_LATE_RECEIVER_WTIME = 21, // late receiver waiting time
     MPI_STAT_SENDRECV = 22,
     MPI_STAT_SENDRECV_WTIME = 23,
     MPI_STAT_COLLECTIVE = 24,       // number of (unbalanced) collectives
     MPI_STAT_COLLECTIVE_WTIME = 25, // waiting time in collectives
     MPI_STAT_WAITALL_LATEPARTNER = 26,
     MPI_STAT_WAITALL_LATEPARTNER_WTIME = 27,

     //OpenMP
     OMP_STAT_BARRIER = 28,      // OpenMP barriers
     OMP_STAT_BARRIER_WTIME = 29, // waiting time in OpenMP barriers
     STAT_NUMBER = 30
  };

  enum ActivityType
  {
    // MPI
     STAT_MPI_COLLECTIVE = 0, // MPI collectives (only blocking)
     STAT_MPI_P2P = 1,       // MPI send/recv/sendrecv (only blocking)
     STAT_MPI_WAIT = 2,       // MPI wait, including waitall
     
     // OpenMP (OMPT not yet considered)
     STAT_OMP_JOIN = 3,    // OpenMP fork
     STAT_OMP_BARRIER = 4, // OpenMP barrier
     
    // offloading
     STAT_OFLD_KERNEL = 5,      // offload kernels
     STAT_OFLD_SYNC = 6,        // any offload synchronization, except for events
     STAT_OFLD_SYNC_EVT = 7,    // offload event synchronization
     STAT_OFLD_TEST_EVT = 8,    // offload test operation

     STAT_ACTIVITY_TYPE_NUMBER = 9
  };
  
  typedef struct
  {
    ActivityType type;
    const char*  str;
  } TypeStrActivity;
  
  static const TypeStrActivity typeStrTableActivity[ STAT_ACTIVITY_TYPE_NUMBER ] =
  {
    { STAT_MPI_COLLECTIVE, "MPI (blocking) collectives" },
    { STAT_MPI_P2P, "MPI (blocking) p2p" },
    { STAT_MPI_WAIT, "MPI wait[all]" },
    { STAT_OMP_JOIN, "OpenMP fork/join" },
    { STAT_OMP_BARRIER, "OpenMP barriers" },
    { STAT_OFLD_KERNEL, "Ofld. kernels" },
    { STAT_OFLD_SYNC, "Ofld. stream/device synchr." },
    { STAT_OFLD_SYNC_EVT, "Ofld. event synchr." },
    { STAT_OFLD_TEST_EVT, "Ofld. event queries" }
  };
  
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
      uint64_t stats[ STAT_NUMBER ];
      
      // activity occurrences 
      uint64_t activity_count[ STAT_ACTIVITY_TYPE_NUMBER ];
      
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
      addStatWithCount( StatMetric statType, uint64_t time, uint64_t count = 1 );
      
      void
      addStatValue( StatMetric statType, uint64_t value );
      
      void
      addAllStats( uint64_t* stats );
      
      uint64_t*
      getStats();
      
      void
      countActivity( ActivityType activityType );
      
      void
      setActivityCount( ActivityType activityType, uint64_t count );
      
      void
      setActivityCounts( uint64_t *count );
      
      uint64_t*
      getActivityCounts();
      
      void
      addActivityCounts( uint64_t* counts );
  };
}
