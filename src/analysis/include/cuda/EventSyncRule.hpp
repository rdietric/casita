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

#include "ICUDARule.hpp"
#include "AnalysisParadigmCUDA.hpp"

namespace casita
{
 namespace cuda
 {
  class EventSyncRule :
    public ICUDARule
  {
    public:

      EventSyncRule( int priority ) :
        ICUDARule( "EventSyncRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {

        if ( !node->isCUDAEventSync( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis   = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair& sync   = node->getGraphPair( );

        EventNode*      eventLaunchLeave = analysis->getLastEventLaunchLeave(
          ( (EventNode*)sync.second )->getEventId( ) );

        if ( !eventLaunchLeave )
        {
          UTILS_DBG_MSG( true, " * Ignoring event sync %s without matching event record",
                         node->getUniqueName( ).c_str( ) );
          return false;
        }

        GraphNode*   eventLaunchEnter    = eventLaunchLeave->getGraphPair( ).first;
        EventStream* refProcess          = commonAnalysis->getStream(
          eventLaunchLeave->getReferencedStreamId( ) );
        EventStreamGroup::EventStreamList deviceProcs;

        if ( refProcess->isDeviceNullStream( ) )
        {
          commonAnalysis->getAllDeviceStreams( deviceProcs );
        }
        else
        {
          deviceProcs.push_back( refProcess );
        }

        bool ruleResult = false;
        for ( EventStreamGroup::EventStreamList::const_iterator iter =
                deviceProcs.begin( );
              iter != deviceProcs.end( ); ++iter )
        {
          /* last kernel launch before event record for this stream */
          GraphNode* kernelLaunchLeave           = analysis->getLastLaunchLeave(
            eventLaunchEnter->getTime( ), ( *iter )->getId( ) );
          if ( !kernelLaunchLeave )
          {
            continue;
          }

          GraphNode::GraphNodePair& kernelLaunch =
            ( (GraphNode*)kernelLaunchLeave )->getGraphPair( );

          GraphNode* kernelEnter =
            (GraphNode*)kernelLaunch.first->getLink( );
          if ( !kernelEnter )
          {
            throw RTException(

                    "Event sync %s (%f) returned but kernel from %s (%f) on stream [%u, %s] did not start/finish yet",
                    node->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( node->getTime( ) ),
                    kernelLaunch.first->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( kernelLaunch.first->
                                                 getTime( ) ),
                    kernelLaunch.first->getReferencedStreamId( ),
                    commonAnalysis->getStream( kernelLaunch.first->
                                               getReferencedStreamId( ) )->
                    getName( ) );
          }

          GraphNode* kernelLeave = kernelEnter->getGraphPair( ).second;
          if ( !kernelLeave || kernelLeave->getTime( ) > sync.second->getTime( ) )
          {
            throw RTException(

                    "Event sync %s (%f) returned but kernel from %s (%f) on stream [%u, %s] did not finish yet",
                    node->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( node->getTime( ) ),
                    kernelLaunch.first->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( kernelLaunch.first->
                                                 getTime( ) ),
                    kernelLaunch.first->getReferencedStreamId( ),
                    commonAnalysis->getStream( kernelLaunch.first->
                                               getReferencedStreamId( ) )->
                    getName( ) );
          }

          uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( );

          if ( ( sync.first->getTime( ) < kernelLeave->getTime( ) ) &&
               ( sync.second->getTime( ) - kernelLeave->getTime( ) <=
                 syncDeltaTicks ) )
          {
            commonAnalysis->getEdge( sync.first, sync.second )->makeBlocking( );

            /* set counters */
            sync.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                      CTR_WAITSTATE ),
                                    sync.second->getTime( ) -
                                    std::max( sync.first->getTime( ),
                                              kernelEnter->getTime( ) ) );
            kernelLeave->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                       CTR_BLAME ),
                                     sync.second->getTime( ) -
                                     std::max( sync.first->getTime( ),
                                               kernelEnter->getTime( ) ) );
          }

          commonAnalysis->newEdge( kernelLeave,
                                   sync.second,
                                   EDGE_CAUSES_WAITSTATE );
          ruleResult = true;
        }

        return ruleResult;
      }
  };
 }
}
