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

      /**
       * This rule is triggered by a cuEventSynchronize leave event.
       * 
       * Step:
       * 1) Get the cuEventRecord leave node using the ID of the given node.
       * 2) Get the event stream the cuEventRecord (leave node) is referencing
       * 3) For all referenced streams (can be all device streams if NULL stream was referenced)
       *   3.1) Get last kernel launch leave event before eventRecordEnter time
       *   3.2) Get link from kernel launch to the kernel node
       *   3.3) Make the cuEventSynchronize a blocking wait state, if it 
       *        started before the kernel ended
       *   3.4) Create a dependency edge from the kernel leave to the 
       *        cuEventSynchronize leave
       * 
       * @param analysis the CUDA analysis
       * @param node a node object
       * @return true, if the rule could be applied, otherwise false
       */
      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {

        if ( !node->isCUDAEventSync( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis   = analysis->getCommon( );

        // get the enter and leave event pair
        GraphNode::GraphNodePair& sync   = node->getGraphPair( );

        // get cuEventRecord leave node
        EventNode* eventRecordLeave = analysis->getEventRecordLeave(
          ( (EventNode*)sync.second )->getEventId( ) );

        if ( !eventRecordLeave )
        {
          UTILS_MSG( true, " * Ignoring event sync %s without matching event record",
                         node->getUniqueName( ).c_str( ) );
          return false;
        }

        // get the event stream of CUDA event
        EventStream* refProcess       = commonAnalysis->getStream(
          eventRecordLeave->getReferencedStreamId( ) );
        EventStreamGroup::EventStreamList deviceProcs;

        // put all device streams in the list, if we are synchronizing with the NULL stream
        if ( refProcess->isDeviceNullStream( ) )
        {
          commonAnalysis->getAllDeviceStreams( deviceProcs );
        }
        else
        {
          deviceProcs.push_back( refProcess );
        }
        
        //GraphNode* eventRecordEnter = eventRecordLeave->getGraphPair( ).first;
        uint64_t eventRecordEnterTime = 
                (eventRecordLeave->getGraphPair( ).first)->getTime();

        bool ruleResult = false;
        for ( EventStreamGroup::EventStreamList::const_iterator iter =
                deviceProcs.begin( );
              iter != deviceProcs.end( ); ++iter )
        {
          // get last kernel launch leave event before event record for this stream
          GraphNode* kernelLaunchLeave = analysis->getLastLaunchLeave(
                  eventRecordEnterTime/*eventRecordEnter->getTime( )*/, 
                  ( *iter )->getId( ) );
          if ( !kernelLaunchLeave )
          {
            continue;
          }

          GraphNode::GraphNodePair& kernelLaunch =
            ( (GraphNode*)kernelLaunchLeave )->getGraphPair( );

          // the kernel launch enter event has a link to the kernel it launches
          GraphNode* kernelEnter =
            (GraphNode*)kernelLaunch.first->getLink( );
          if ( !kernelEnter )
          {
            throw RTException(
                    //UTILS_MSG(true, 
                    "[%u] Event sync %s (%f) on stream %s returned but kernel "
                    "from %s (%f) on stream [%u, %s] did not start/finish yet",
                    commonAnalysis->getMPIRank( ),
                    node->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( node->getTime( ) ),
                    commonAnalysis->getStream( node->getStreamId( ) )->getName( ),
                    kernelLaunch.first->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( kernelLaunch.first->
                                                 getTime( ) ),
                    kernelLaunch.first->getReferencedStreamId( ),
                    commonAnalysis->getStream( kernelLaunch.first->
                                               getReferencedStreamId( ) )->
                    getName( ) );
            return false;
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

          /* ignore delta ticks for now until we have a better heuristic */
          /* uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( ); */

          if ( ( sync.first->getTime( ) < kernelLeave->getTime( ) ) )
          {
            commonAnalysis->getEdge( sync.first, sync.second )->makeBlocking( );

            // set counters
            //\todo: write counters to enter nodes
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
