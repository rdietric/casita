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
#include "AnalysisMetric.hpp"
#include "ITraceWriter.hpp"

namespace casita
{
 namespace io
 {

  class IParallelTraceWriter :
    public ITraceWriter
  {
    public:

      typedef struct
      {
        uint32_t functionId;
        uint32_t numInstances;
        uint32_t numUnifyStreams;
        uint64_t totalDuration;
        uint64_t totalDurationOnCP;
        uint64_t totalBlame;
        double   fractionCP;
        double   fractionBlame;
        uint64_t lastEnterTime;
      } ActivityGroup;

      // key: OTF2 region reference, value: activity group
      typedef std::map< uint32_t, ActivityGroup > ActivityGroupMap;

      typedef struct
      {

        bool
        operator()( const ActivityGroup& g1, const ActivityGroup& g2 ) const
        {
          double rating1 = g1.fractionBlame + g1.fractionCP;
          double rating2 = g2.fractionBlame + g2.fractionCP;

          if ( rating1 == rating2 )
          {
            return g1.functionId > g2.functionId;
          }
          else
          {
            return rating1 > rating2;
          }
        }

      } ActivityGroupCompare;

      IParallelTraceWriter( uint32_t mpiRank,
                            uint32_t mpiSize ) :
        mpiRank( mpiRank ),
        mpiSize( mpiSize )
      {

      }

      virtual
      ~IParallelTraceWriter( )
      {
      }
      
      virtual void
      setupAttributeList( void ) = 0;
      
      virtual void
      setupEventReader( uint64_t streamId ) = 0;
      
      virtual bool
      writeStream(  EventStream*   stream,
                    Graph*         graph,
                    uint64_t*      events_read ) = 0;

      ActivityGroupMap*
      getActivityGroupMap( )
      {
        return &activityGroupMap;
      }

    protected:
      uint32_t mpiRank, mpiSize;
      
      // maps OTF2 region references to activity groups to collect a global profile
      ActivityGroupMap activityGroupMap;

    private:

      void
      writeDefFunction( uint64_t id, const char* name, FunctionGroup fg )
      {
      }
  };
 }
}
