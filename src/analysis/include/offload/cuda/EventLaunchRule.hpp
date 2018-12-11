/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015, 2017,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "../IOffloadRule.hpp"
#include "../AnalysisParadigmOffload.hpp"

namespace casita
{
 namespace offload
 {
  class EventLaunchRule :
    public IOffloadRule
  {
    public:

      /**
       * Uses pendingKernelLaunchMap.
       * 
       * @param priority
       */
      EventLaunchRule( int priority ) :
        IOffloadRule( "EventLaunchRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* analysis, GraphNode* evtRecLeave )
      {
        if ( !evtRecLeave->isCUDAEventLaunch() || !evtRecLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getAnalysisEngine();

        //GraphNode* evtRecEnter = evtRecLeave->getGraphPair().first;
        EventStream* refProcess = commonAnalysis->getStream(
          evtRecLeave->getReferencedStreamId() );
        
        if ( !refProcess )
        {
          RTException(
            "Event launch %s (%f) does not reference any stream (id = %u)",
            evtRecLeave->getUniqueName( ).c_str( ),
            commonAnalysis->getRealTime( evtRecLeave->getTime( ) ),
            evtRecLeave->getReferencedStreamId( ) );
        }

        if ( refProcess->isHostStream() )
        {
          RTException(
            "Process %s referenced by event launch %s is a host stream",
            refProcess->getName(), evtRecLeave->getUniqueName().c_str() );
        }

        analysis->addEventProcessId(
          ( (EventNode*)evtRecLeave )->getEventId(), refProcess->getId() );

        // if event is not on NULL stream, set link to 
        /*if ( !( refProcess->isDeviceNullStream() ) )
        {
          GraphNode* kernelLaunchLeave = analysis->getLastKernelLaunchLeave(
                              evtRecEnter->getTime(), refProcess->getId() );

          if ( kernelLaunchLeave )
          {
            evtRecLeave->setLink( (GraphNode*)kernelLaunchLeave );
          }
        }*/
        
        // set link to mark this node as not handled
        //evtRecLeave->setLink( evtRecLeave );
        
        analysis->setLastEventLaunch( (EventNode*)( evtRecLeave ) );

        return true;
      }
  };
 }
}
