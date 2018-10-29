/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
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
  class EventSyncRule :
    public IOffloadRule
  {
    public:

      EventSyncRule( int priority ) :
        IOffloadRule( "EventSyncRule", priority )
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
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* syncLeave )
      {

        if ( !syncLeave->isCUDAEventSync() || !syncLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* analysis = ofldAnalysis->getCommon();
        
        // count occurrence
        analysis->getStatistics().countActivity( STAT_OFLD_SYNC_EVT );

        // use the CUDA event ID to get cuEventRecord leave node
        EventNode* eventRecordLeave = ofldAnalysis->getEventRecordLeave(
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
        
        // get the event stream of the associated CUDA event
        DeviceStream* refProcess = analysis->getStreamGroup().getDeviceStream(
          eventRecordLeave->getReferencedStreamId() );
        
        if( refProcess == NULL )
        {
          UTILS_WARNING( "[%" PRIu32 "] EventSyncRule: Referenced stream (%" PRIu64
                          ") %s (%f) on stream %s  not found!",
            analysis->getMPIRank(),
            eventRecordLeave->getReferencedStreamId(),
            syncLeave->getUniqueName().c_str(),
            analysis->getRealTime( syncLeave->getTime() ),
            analysis->getStream( syncLeave->getStreamId() )->getName());
          
          return false;
        }
        
        EventStreamGroup::DeviceStreamList deviceProcs;

        // put all device streams in the list, if we are synchronizing with the NULL stream
        if ( refProcess->isDeviceNullStream() )
        {
          analysis->getStreamGroup().getDeviceStreams( deviceProcs );
        }
        else
        {
          deviceProcs.push_back( refProcess );
        }
        
        GraphNode* eventRecordEnter = eventRecordLeave->getGraphPair().first;
        if( !eventRecordEnter )
        {
          UTILS_WARNING( "[%" PRIu32 "] No event record enter event for %s found", 
                         analysis->getMPIRank(),
                         analysis->getNodeInfo( eventRecordLeave ).c_str() );
          return false;
        }
        
        uint64_t eventRecordEnterTime = eventRecordEnter->getTime();

        bool ruleResult = false;
        GraphNode* syncEnter = syncLeave->getGraphPair().first;
        
        // iterate over device streams (typically only one)
        for ( EventStreamGroup::DeviceStreamList::const_iterator iter =
                deviceProcs.begin(); iter != deviceProcs.end(); ++iter )
        {
          // get last kernel launch leave node of the given device stream 
          // that started before event record enter time
          uint64_t strmId = ( *iter )->getId();
          GraphNode* kernelLaunchLeave = ofldAnalysis->getLastKernelLaunchLeave(
                  eventRecordEnterTime, strmId );
                    
          // if the stream has no kernel launch leave, the kernel has already 
          // been synchronized, hence continue with next stream
          if ( !kernelLaunchLeave )
          {
            //UTILS_WARNING("EventSyncRule: no kernel launch leave for stream %lu", strmId);
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
              "[%" PRIu32 "] EventSyncRule on %s (%f) failed.\n"
              "Synchronize returned before kernel %s (%f) on stream "
              "[%u, %s] finished. Deferring node ...",
              analysis->getMPIRank(),
              syncLeave->getUniqueName().c_str(),
              analysis->getRealTime( syncLeave->getTime() ),
              kernelLaunchEnter->getUniqueName().c_str(),
              analysis->getRealTime( kernelLaunchEnter->getTime() ),
              kernelLaunchEnter->getReferencedStreamId(),
              analysis->getStream( 
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
                    syncLeave->getUniqueName().c_str(),
                    analysis->getRealTime( syncLeave->getTime() ),
                    kernelLaunchEnter->getUniqueName().c_str(),
                    analysis->getRealTime( 
                      kernelLaunchEnter->getTime() ),
                    kernelLaunchEnter->getReferencedStreamId(),
                    analysis->getStream( 
                      kernelLaunchEnter->getReferencedStreamId() )->getName() );
          }
          else if ( kernelLeave->getTime() > syncLeave->getTime() )
          {
            UTILS_OUT( "[%" PRIu32 "] EventSyncRule on %s (%f) failed!", 
                       analysis->getMPIRank(),
                       syncLeave->getUniqueName().c_str(),
                       analysis->getRealTime( syncLeave->getTime() ) );
            // if the kernelLeave has been deleted in intermediate flush the 
            // following message creates a segmentation fault
            UTILS_OUT( "Host-Device time displacement: kernel %s > evtSync %s"
                       ", kernel launch leave: %s",
                       analysis->getNodeInfo( kernelLeave ).c_str(), 
                       analysis->getNodeInfo( syncLeave ).c_str(),
                       analysis->getNodeInfo( kernelLaunchLeave ).c_str() );
          }

          /* ignore delta ticks for now until we have a better heuristic */
          /* uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( ); */

          // if sync enter is before kernel leave it is a blocking wait state
          if ( syncEnter && ( syncEnter->getTime() < kernelLeave->getTime() ) )
          {
            Edge* syncEdge = analysis->getEdge( syncEnter, syncLeave );
            
            if( syncEdge )
            {
              if( !syncEdge->isBlocking() )
              {
                syncEdge->makeBlocking();
                
                // early blocking wait statistics
                analysis->getStatistics().addStatWithCount( 
                  OFLD_STAT_EARLY_BLOCKING_WAIT, 
                  syncLeave->getTime() - syncEnter->getTime() );
              }
            }
            else
            {
              analysis->newEdge( syncEnter, syncLeave, true );
              
              // early blocking wait statistics
              analysis->getStatistics().addStatWithCount( 
                OFLD_STAT_EARLY_BLOCKING_WAIT, 
                syncLeave->getTime() - syncEnter->getTime() );
            }
            
            // determine real waiting time (overlap of kernel and sync)
            uint64_t waitingTime = std::min( syncLeave->getTime(),
                                             kernelLeave->getTime() ) -
                                   std::max( syncEnter->getTime(),
                                             kernelEnter->getTime() );
            
            // count statistics
            analysis->getStatistics().addStatValue( 
              OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL, waitingTime );

            // attribute sync with waiting time
            syncLeave->incCounter( WAITING_TIME, waitingTime );
            //kernelLeave->incCounter( BLAME, waitingTime );
            
            // blame kernel for letting the host wait
            Edge* kernelEdge = analysis->getEdge( kernelEnter, kernelLeave );
            if( kernelEdge )
            {
              kernelEdge->addBlame( waitingTime );
            }
            else
            {
              UTILS_WARNING( "CUDA EventSyncRule: Could not find kernel edge %s -> %s",
                             analysis->getNodeInfo( kernelEnter ).c_str(),
                             analysis->getNodeInfo( kernelLeave ).c_str() );
            }
          }

          // add edge between kernel leave and syncLeave
          analysis->newEdge( kernelLeave, syncLeave );
          //UTILS_WARNING( "Create edge between %lf -> %s",
          //               analysis->getRealTime( kernelLeave->getTime() ),
          //               analysis->getNodeInfo( syncLeave ).c_str() );
          
          ruleResult = true;
          
          // create edges between previous kernels which are not on the same stream
          ofldAnalysis->createKernelDependencies( kernelEnter );
          
          // consume a pending kernel
          //commonAnalysis->getStream( kernelEnter->getStreamId() )->consumePendingKernel();
          
          // clear all pending kernels before this kernel (including this kernel)
          DeviceStream *kernelLeaveStream = 
            analysis->getStreamGroup().getDeviceStream( kernelLeave->getStreamId() );
          if( kernelLeaveStream )
          {
            kernelLeaveStream->consumePendingKernels( kernelLeave );
          }
          else
          {
            UTILS_WARNING( "Cannot consume pending kernels. Stream with ID %"
                           PRIu64 " not found!", kernelLeave->getStreamId() );
          }
        }
        
        // if the rule could no be applied successfully, assign this sync with
        // waiting time (we cannot blame it, as we currently do not consider 
        // device communication
        if( ruleResult == false )
        {
            uint64_t value = syncLeave->getTime() - syncEnter->getTime();
            syncLeave->incCounter( WAITING_TIME, value );
            //syncLeave->incCounter( BLAME, value );
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
