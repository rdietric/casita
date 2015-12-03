/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "ICUDARule.hpp"
#include "AnalysisParadigmCUDA.hpp"

namespace casita
{
 namespace cuda
 {
  class EventLaunchRule :
    public ICUDARule
  {
    public:

      EventLaunchRule( int priority ) :
        ICUDARule( "EventLaunchRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* evtRecLeave )
      {
        if ( !evtRecLeave->isCUDAEventLaunch( ) || !evtRecLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis     = analysis->getCommon( );

        // get the complete execution
        GraphNode::GraphNodePair& evRecord = evtRecLeave->getGraphPair( );
        EventStream*    refProcess         = commonAnalysis->getStream(
          evtRecLeave->getReferencedStreamId( ) );
        
        if ( !refProcess )
        {
          RTException(
            "Event launch %s (%f) does not reference any stream (id = %u)",
            evtRecLeave->getUniqueName( ).c_str( ),
            commonAnalysis->getRealTime( evtRecLeave->getTime( ) ),
            evtRecLeave->getReferencedStreamId( ) );
        }

        if ( refProcess->isHostStream( ) )
        {
          RTException(
            "Process %s referenced by event launch %s is a host stream",
            refProcess->getName( ), evtRecLeave->getUniqueName( ).c_str( ) );
        }

        analysis->setEventProcessId(
          ( (EventNode*)evtRecLeave )->getEventId( ), refProcess->getId( ) );

        GraphNode* kernelLaunchLeave = NULL;

        // if event is on NULL stream, test if any kernel launch can be found
        if ( refProcess->isDeviceNullStream( ) )
        {
          /*Allocation::ProcessList deviceProcs;
          commonAnalysis->getAllDeviceStreams(deviceProcs);

          for (Allocation::ProcessList::const_iterator iter = deviceProcs.begin();
                  iter != deviceProcs.end(); ++iter)
          {
              kernelLaunchLeave = analysis->getLastLaunchLeave(
                      evLaunch.first->getTime(), (*iter)->getId());

              if (kernelLaunchLeave)
                  break;
          }

          if (kernelLaunchLeave)
          {*/
          analysis->setLastEventLaunch( (EventNode*)( evRecord.second ) );
          return true;
          /* } */
        }
        else
        {
          // otherwise, test on its stream only
          kernelLaunchLeave = analysis->getLastLaunchLeave(
            evRecord.first->getTime( ), refProcess->getId( ) );

          if ( kernelLaunchLeave )
          {
            evRecord.second->setLink( (GraphNode*)kernelLaunchLeave );
          }

          analysis->setLastEventLaunch( (EventNode*)( evRecord.second ) );
          return true;
          /* } */
        }

        return false;
      }
  };
 }
}
