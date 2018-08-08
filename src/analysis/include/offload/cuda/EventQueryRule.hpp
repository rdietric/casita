/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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
  class EventQueryRule :
    public IOffloadRule
  {
    public:

      /**
       * Uses pendingKernels
       * 
       * @param priority
       */
      EventQueryRule( int priority ) :
        IOffloadRule( "EventQueryRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* analysis, GraphNode* queryLeave )
      {

        if ( !queryLeave->isCUDAEventQuery() || !queryLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();

        EventNode* evQueryLeave = (EventNode*)queryLeave;

        // link to previous matching event query
        analysis->linkEventQuery( evQueryLeave );

        // we can return, if the function result of the event query is unknown
        if ( evQueryLeave->getFunctionResult() == EventNode::FR_UNKNOWN )
        {
          return true;
        }
        
        // the query was successful -> event finished

        // consume mapping for this event ID
        analysis->removeEventQuery( evQueryLeave->getEventId() );

        // get the device stream ID this event is queued on
        uint64_t refDeviceProcessId = analysis->getEventProcessId(
                                                  evQueryLeave->getEventId() );
        if ( !refDeviceProcessId )
        {
          UTILS_WARNING( "Could not process CUDA event query %s. "
            "Device stream not found! No event with ID %" PRIu64 " recorded?",
            commonAnalysis->getNodeInfo(evQueryLeave).c_str(),
            evQueryLeave->getEventId() );
          return false;
        }

        // get the first kernel launch before eventLaunch enter
        EventNode* eventLaunchLeave = analysis->getEventRecordLeave(
                                                   evQueryLeave->getEventId() );
        if ( !eventLaunchLeave )
        {
          UTILS_WARNING( "Could not process CUDA event query %s. "
            "Event record not found! No event with ID %" PRIu64 " recorded?",
            commonAnalysis->getNodeInfo(evQueryLeave).c_str(),
            evQueryLeave->getEventId() );
          return false;
        }

        GraphNode* kernelLaunchLeave = analysis->getLastKernelLaunchLeave(
               eventLaunchLeave->getPartner()->getTime(), refDeviceProcessId );

        // get the linked kernel
        if ( kernelLaunchLeave )
        {
          GraphNode* kernelLaunchEnter = kernelLaunchLeave->getGraphPair().first;
          GraphNode* kernelEnter = (GraphNode*)kernelLaunchEnter->getLink();
          if ( !kernelEnter )
          {
            ErrorUtils::getInstance().throwError(
              "Event query %s (%f) returns success but kernel from %s (%f) did not finish yet",
              evQueryLeave->getUniqueName().c_str(),
              commonAnalysis->getRealTime( evQueryLeave->getTime() ),
              kernelLaunchEnter->getUniqueName().c_str(),
              commonAnalysis->getRealTime( kernelLaunchEnter->getTime() ) );
            return false;
          }

          GraphNode* kernelLeave = kernelEnter->getGraphPair().second;

          UTILS_ASSERT( kernelLeave, "EventQueryRule: No kernel leave!" );

          if ( queryLeave->getTime() < kernelLeave->getTime() )
          {
            throw RTException( "Incorrect timing between %s and %s\n",
                               queryLeave->getUniqueName().c_str(),
                               kernelLeave->getUniqueName().c_str() );
          }
          
          DeviceStream* devStrm = 
            ( DeviceStream* )commonAnalysis->getStream( kernelLeave->getStreamId() );

          // process all event query nodes and make blocking if they depend on 
          // the kernel
          EventNode* firstEventQueryLeave = evQueryLeave;
          while ( true )
          {
            EventNode* prev = (EventNode*)( firstEventQueryLeave->getLink() );
            if ( !prev )
            {
              break;
            }

            GraphNode* prevQueryEnter = prev->getGraphPair().first;
            GraphNode* prevQueryLeave = prev->getGraphPair().second;

            // if kernel is running during the query
            if ( kernelEnter->getTime() <= prevQueryEnter->getTime() )
            {
              //\todo: why?
              // make query edge blocking
              Edge* qEdge = 
                commonAnalysis->getEdge( prevQueryEnter, prevQueryLeave );
              if( qEdge )
              {
                qEdge->makeBlocking();
              }
              
              // compute waiting time
              uint64_t waitingTime = prevQueryLeave->getTime() 
                                   - prevQueryEnter->getTime();
              
              // attribute waiting time to (unnecessary) query leave node
              prevQueryLeave->incCounter( WAITING_TIME, waitingTime );
              //kernelLeave->incCounter( BLAME, waitingTime );
              
              // blame the kernel for the query waiting time
              Edge* kernelEdge = commonAnalysis->getEdge( kernelEnter, kernelLeave );
              if( kernelEdge )
              {
                kernelEdge->addBlame( waitingTime );
              }
              else
              {
                UTILS_WARNING( "CUDA EventQueryRule: Could not find kernel edge %s -> %s",
                               commonAnalysis->getNodeInfo( kernelEnter ).c_str(),
                               commonAnalysis->getNodeInfo( kernelLeave ).c_str() );
              }
              
              commonAnalysis->getStatistics().addStatWithCount( 
                OFLD_STAT_EARLY_TEST, waitingTime );

              // add a blocking dependency, so it cannot be used for critical path analysis
              // \todo: needed anymore?
              commonAnalysis->newEdge( kernelLeave,
                                       prevQueryLeave,
                                       EDGE_IS_BLOCKING );
            }

            firstEventQueryLeave = prev;
          }

          // add kernel/last event query leave dependency
          commonAnalysis->newEdge( kernelLeave, queryLeave );
          
          //commonAnalysis->getStream( kernelEnter->getStreamId() )->consumePendingKernel();
          // consume all pending kernels before this kernel
          devStrm->consumePendingKernels( kernelLeave );
        }
        else 
        // if no kernel launch leave was found (e.g. associated kernel already synchronized)
        {
          GraphNode* queryEnter = queryLeave->getGraphPair().first;
          
          uint64_t waitingTime = queryLeave->getTime() - queryEnter->getTime();
          
          // attribute waiting time to (redundant) query leave node
          queryLeave->incCounter( WAITING_TIME, waitingTime );
          //queryLeave->incCounter( BLAME, waitingTime );
          
          // blame this redundant event query
          Edge* queryEdge = commonAnalysis->getEdge( queryEnter, queryLeave );
          if( queryEdge )
          {
            queryEdge->addBlame( waitingTime );
          }
          else
          {
            UTILS_WARNING( "CUDA EventQueryRule: Could not find query edge %s -> %s",
                           commonAnalysis->getNodeInfo( queryEnter ).c_str(),
                           commonAnalysis->getNodeInfo( queryLeave ).c_str() );
          }
        }
        
        return true;
      }
  };
 }
}
