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
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {

        if ( !syncLeave->isCUDAEventSync( ) || !syncLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis   = analysis->getCommon( );

        // get the enter and leave event pair
        GraphNode::GraphNodePair& sync   = syncLeave->getGraphPair( );

        // get cuEventRecord leave node
        EventNode* eventRecordLeave = analysis->getEventRecordLeave(
          ( (EventNode*)syncLeave )->getEventId( ) );

        if ( !eventRecordLeave )
        {
          UTILS_MSG( true, " * Ignoring event sync %s without matching event record",
                           syncLeave->getUniqueName( ).c_str( ) );
          return false;
        }

        // get the event stream of CUDA event
        EventStream* refProcess = commonAnalysis->getStream(
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
                deviceProcs.begin( ); iter != deviceProcs.end( ); ++iter )
        {
          // get last kernel launch leave node of the given device stream 
          // that started before event record enter time
          GraphNode* kernelLaunchLeave = analysis->getLastLaunchLeave(
                  eventRecordEnterTime, ( *iter )->getId( ) );
          if ( !kernelLaunchLeave )
          {
            continue;
          }

          GraphNode* kernelLaunchEnter = kernelLaunchLeave->getGraphPair( ).first;

          // the kernel launch enter event has a link to the kernel it launches
          GraphNode* kernelEnter = ( GraphNode* )kernelLaunchEnter->getLink( );
          if ( !kernelEnter )
          {
            // if this happens, the KernelExecutionRule has not been applied,
            // probably due to time displacement (inaccuracy in Score-P time conversion)
            
            // analysis->printDebugInformation( ( (EventNode*)syncLeave )->getEventId( ) );
            
            //throw RTException(
            UTILS_MSG(true, 
                    "[%u] Event sync %s (%f) on stream %s returned but kernel "
                    "from %s (%f) on stream [%u, %s] did not start/finish yet",
                    commonAnalysis->getMPIRank( ),
                    syncLeave->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( syncLeave->getTime( ) ),
                    commonAnalysis->getStream( syncLeave->getStreamId( ) )->getName( ),
                    kernelLaunchEnter->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( kernelLaunchEnter->getTime( ) ),
                    kernelLaunchEnter->getReferencedStreamId( ),
                    commonAnalysis->getStream( kernelLaunchEnter->
                                               getReferencedStreamId( ) )->
                    getName( ) );
            
            //\todo: store the node in a pending list and process it later
            
            return false;
          }

          GraphNode* kernelLeave = kernelEnter->getGraphPair( ).second;
          if ( !kernelLeave || kernelLeave->getTime( ) > syncLeave->getTime( ) )
          {
            throw RTException(
                    "Event sync %s (%f) returned but kernel from %s (%f) on "
                    "stream [%u, %s] did not finish yet",
                    syncLeave->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( syncLeave->getTime( ) ),
                    kernelLaunchEnter->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( kernelLaunchEnter->
                                                 getTime( ) ),
                    kernelLaunchEnter->getReferencedStreamId( ),
                    commonAnalysis->getStream( kernelLaunchEnter->
                                               getReferencedStreamId( ) )->
                    getName( ) );
          }

          /* ignore delta ticks for now until we have a better heuristic */
          /* uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( ); */

          // sync enter has to be before kernel leave
          if ( ( sync.first->getTime( ) < kernelLeave->getTime( ) ) )
          {
            commonAnalysis->getEdge( sync.first, syncLeave )->makeBlocking( );

            // set counters
            syncLeave->incCounter( WAITING_TIME,
                                     syncLeave->getTime( ) -
                                     std::max( sync.first->getTime( ),
                                               kernelEnter->getTime( ) ) );
            kernelLeave->incCounter( BLAME,
                                     syncLeave->getTime( ) -
                                     std::max( sync.first->getTime( ),
                                               kernelEnter->getTime( ) ) );
          }

          commonAnalysis->newEdge( kernelLeave,
                                   syncLeave,
                                   EDGE_CAUSES_WAITSTATE );
          ruleResult = true;
        }
        
        // clear list of device processes
        deviceProcs.clear();

        return ruleResult;
      }
  };
 }
}
