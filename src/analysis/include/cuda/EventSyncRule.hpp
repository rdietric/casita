/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
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

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        // use the CUDA event ID to get cuEventRecord leave node
        EventNode* eventRecordLeave = analysis->getEventRecordLeave(
          ( (EventNode*)syncLeave )->getEventId() );

        // the event record might have been deleted in intermediate flush
        if ( !eventRecordLeave )
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                     " * Ignoring event sync %s without matching event record",
                     syncLeave->getUniqueName().c_str() );
          return false;
        }

        // set the link to NULL as this event has been synchronized and the node
        // can be deleted in intermediate flush
        eventRecordLeave->setLink( NULL );
        
        // get the event stream of CUDA event
        EventStream* refProcess = commonAnalysis->getStream(
          eventRecordLeave->getReferencedStreamId() );
        
        if( refProcess == NULL )
        {
          UTILS_MSG( true, "[%"PRIu32"] EventSyncRule: Referenced stream (%"PRIu64
                           ") %s (%f) on stream %s  not found!",
                     commonAnalysis->getMPIRank(),
                     eventRecordLeave->getReferencedStreamId(),
                     syncLeave->getUniqueName().c_str(),
                     commonAnalysis->getRealTime( syncLeave->getTime() ),
                     commonAnalysis->getStream( syncLeave->getStreamId() )->getName());
          
          return false;
        }
        
        EventStreamGroup::EventStreamList deviceProcs;

        // put all device streams in the list, if we are synchronizing with the NULL stream
        if ( refProcess->isDeviceNullStream() )
        {
          commonAnalysis->getAllDeviceStreams( deviceProcs );
        }
        else
        {
          deviceProcs.push_back( refProcess );
        }
        
        //GraphNode* eventRecordEnter = eventRecordLeave->getGraphPair().first;
        uint64_t eventRecordEnterTime = 
                (eventRecordLeave->getGraphPair().first)->getTime();

        bool ruleResult = false;
        
        for ( EventStreamGroup::EventStreamList::const_iterator iter =
                deviceProcs.begin(); iter != deviceProcs.end(); ++iter )
        {
          uint64_t strmId = ( *iter )->getId( );
          // get last kernel launch leave node of the given device stream 
          // that started before event record enter time
          GraphNode* kernelLaunchLeave = analysis->getLastKernelLaunchLeave(
                  eventRecordEnterTime, strmId );
          if ( !kernelLaunchLeave )
          {
            continue;
          }

          GraphNode* kernelLaunchEnter = kernelLaunchLeave->getGraphPair().first;

          // the kernel launch enter event has a link to the kernel it launches
          GraphNode* kernelEnter = ( GraphNode* )kernelLaunchEnter->getLink();
          if ( !kernelEnter )
          {
            // if this happens, the KernelExecutionRule has not been applied,
            // probably due to time displacement (inaccuracy in Score-P time conversion)
            
            // analysis->printDebugInformation( ( (EventNode*)syncLeave )->getEventId( ) );

            UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_TIME, 
              "[%"PRIu32"] EventSyncRule on %s (%f) failed.\n"
              "Synchronize returned before kernel %s (%f) on stream "
              "[%u, %s] finished. Deferring node ...",
              commonAnalysis->getMPIRank(),
              syncLeave->getUniqueName().c_str(),
              commonAnalysis->getRealTime( syncLeave->getTime() ),
              kernelLaunchEnter->getUniqueName().c_str(),
              commonAnalysis->getRealTime( kernelLaunchEnter->getTime() ),
              kernelLaunchEnter->getReferencedStreamId(),
              commonAnalysis->getStream( 
                kernelLaunchEnter->getReferencedStreamId() )->getName() );
            
            // Store the node in a pending list and process it later.
            // Walk forward does not work, because other rules have to be processed first.
            
            // Make sure to defer next KernelExecutionRule as well!!!
            //\todo: mark the kernelLaunch enter node 
            // (set the data field to syncEvtLeave node)
            kernelLaunchEnter->setData( syncLeave );
            
            // deferring does not work here
            //commonAnalysis->addDeferredNode( syncLeave );
            
            deviceProcs.clear();
            return false;
          }          

          GraphNode* kernelLeave = kernelEnter->getGraphPair().second;
          if ( !kernelLeave )
          {
            throw RTException(
                    "Event sync %s (%f) returned but kernel from %s (%f) on "
                    "stream [%u, %s] did not finish yet",
                    syncLeave->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( syncLeave->getTime( ) ),
                    kernelLaunchEnter->getUniqueName( ).c_str( ),
                    commonAnalysis->getRealTime( 
                      kernelLaunchEnter->getTime( ) ),
                    kernelLaunchEnter->getReferencedStreamId( ),
                    commonAnalysis->getStream( 
                      kernelLaunchEnter->getReferencedStreamId( ) )->getName( ) 
                    );
          }
          else if ( kernelLeave->getTime( ) > syncLeave->getTime( ) )
          {
            UTILS_MSG( true, "[%"PRIu32"] EventSyncRule on %s (%f) failed!", 
                       commonAnalysis->getMPIRank(),
                       syncLeave->getUniqueName().c_str(),
                       commonAnalysis->getRealTime( syncLeave->getTime() ) );
            // if the kernelLeave has been deleted in intermediate flush the 
            // following message creates a segmentation fault
            UTILS_MSG( true, "Host-Device time displacement: %s > %s (%llu s)",
                      kernelLeave->getUniqueName().c_str(), 
                      syncLeave->getUniqueName().c_str(),
                      commonAnalysis->getRealTime(
                        kernelLeave->getTime() - syncLeave->getTime() ));
          }

          /* ignore delta ticks for now until we have a better heuristic */
          /* uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( ); */

          GraphNode* syncEnter = syncLeave->getPartner();
        
          // sync enter has to be before kernel leave
          if ( syncEnter->getTime( ) < kernelLeave->getTime( ) )
          {
            commonAnalysis->getEdge( syncEnter, syncLeave )->makeBlocking( );

            // set counters
            uint64_t value = syncLeave->getTime( ) -
                     std::max( syncEnter->getTime( ), kernelEnter->getTime( ) );
            syncLeave->incCounter( WAITING_TIME, value );
            kernelLeave->incCounter( BLAME, value );
          }

          commonAnalysis->newEdge( kernelLeave, syncLeave, 
                                   EDGE_CAUSES_WAITSTATE );
          ruleResult = true;
          
          // consume a pending kernel
          //commonAnalysis->getStream( kernelEnter->getStreamId() )->consumePendingKernel( );
          
          // clear all pending kernels before that kernel
          commonAnalysis->getStream( kernelLeave->getStreamId() )
                                         ->consumePendingKernels( kernelLeave );
        }
        
        // clear list of device processes
        deviceProcs.clear();
        
        // mark this event record node as handled/synchronized
        eventRecordLeave->setData( syncLeave );

        return ruleResult;
      }
  };
 }
}
