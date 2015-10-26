/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <string>
#include <stdint.h>
#include "graph/Node.hpp"
#include "CounterTable.hpp"
#include "EventStream.hpp"
#include "graph/Graph.hpp"

namespace casita
{
 namespace io
 {

  class ITraceWriter
  {
    public:

      enum FunctionGroup
      {
        FG_APPLICATION = 1, FG_CUDA_API, FG_KERNEL, FG_WAITSTATE, FG_MPI
      };

      enum ProcessGroup
      {
        PG_HOST        = 1, PG_DEVICE, PG_DEVICE_NULL
      };

      enum MarkerGroup
      {
        MG_Marker      = 1
      };

      ITraceWriter( )
      {

      }

      virtual
      ~ITraceWriter( )
      {
      }

      virtual void
      open( const std::string otfFilename, uint32_t maxFiles,
            uint32_t numStreams )        = 0;

      virtual void
      close( ) = 0;
      
      virtual void
      reset( ) = 0;

      virtual void
      writeDefProcess( uint64_t     id,
                       uint64_t     parentId,
                       const char*  name,
                       ProcessGroup pg ) = 0;

      virtual void
      writeDefFunction( uint64_t id, const char* name, FunctionGroup fg ) = 0;

      virtual void
      writeDefCounter( uint32_t id, const char* name, OTF2_MetricMode metricMode ) = 0;

  };
 }
}
