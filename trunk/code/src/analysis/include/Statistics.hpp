/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016,
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
  class Statistics
  {
    public:
      Statistics( );
      
      virtual
      ~Statistics( );
     
    private:
      /////// important metrics ////////
      
      // MPI
      uint64_t mpi_blocking_communication; //\todo: per process
      
      // communication roofline
      uint64_t fastest_communication; //\todo: per type (size)
      uint64_t avg_communication; //\todo: per type (size)
      
      // CUDA
      uint64_t cuda_blocking_communication; //\todo: per process
      uint64_t launch_overhead;
      uint64_t launch_distance;
      
      // OpenMP
      uint64_t fork_parallel_overhead;
      uint64_t barrier_overhead;
      
  };
}
