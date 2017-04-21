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
          ErrorUtils::getInstance().throwFatalError(
            "Could not find device stream ID for event %" PRIu64 " from %s",
            evQueryLeave->getEventId(),
            evQueryLeave->getUniqueName().c_str() );
        }

        // get the first kernel launch before eventLaunch enter
        EventNode* eventLaunchLeave = analysis->getEventRecordLeave(
                                                   evQueryLeave->getEventId() );
        if ( !eventLaunchLeave )
        {
          throw RTException( "Could not find event record for event %" PRIu64,
                             evQueryLeave->getEventId() );
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

          if ( queryLeave->getTime() < kernelLeave->getTime() )
          {
            throw RTException( "Incorrect timing between %s and %s\n",
                               queryLeave->getUniqueName().c_str(),
                               kernelLeave->getUniqueName().c_str() );
          }

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

            GraphNode::GraphNodePair& prevQuery = prev->getGraphPair();

            // if kernel is running during the query
            if ( kernelEnter->getTime() <= prevQuery.first->getTime() )
            {
              //\todo: why?
              // make query edge blocking
              Edge* qEdge = commonAnalysis->getEdge( prevQuery.first, prevQuery.second);
              if( qEdge )
              {
                qEdge->makeBlocking();
              }
              
              // set counters
              uint64_t waitingTime = 
                prevQuery.second->getTime() - prevQuery.first->getTime();
              prevQuery.second->incCounter( WAITING_TIME, waitingTime );
              kernelLeave->incCounter( BLAME, waitingTime );
              
              commonAnalysis->getStatistics().addStatWithCount( 
                OFLD_STAT_EARLY_TEST, waitingTime );

              // add a blocking dependency, so it cannot be used for critical path analysis
              // \todo: needed anymore?
              commonAnalysis->newEdge( kernelLeave,
                                       prevQuery.second,
                                       EDGE_IS_BLOCKING );
            }

            firstEventQueryLeave = prev;
          }

          // add kernel/last event query leave dependency
          commonAnalysis->newEdge( kernelLeave, queryLeave );
          
          //commonAnalysis->getStream( kernelEnter->getStreamId() )->consumePendingKernel();
          // consume all pending kernels before this kernel
          commonAnalysis->getStream( kernelLeave->getStreamId() )
                                         ->consumePendingKernels( kernelLeave );
        }
        else 
        // if no kernel launch leave was found (e.g. associated kernel already synchronized)
        {
          // blame this "useless" event query
          uint64_t waitingTime = 
            queryLeave->getTime() - queryLeave->getGraphPair().first->getTime();
          queryLeave->incCounter( WAITING_TIME, waitingTime );
          queryLeave->incCounter( BLAME, waitingTime );
        }
        
        return true;
      }
  };
 }
}
