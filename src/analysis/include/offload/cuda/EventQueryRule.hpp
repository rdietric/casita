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
          if ( !queryLeave->isCUDAEventQuery( ) || !queryLeave->isLeave( ) )
          {
            return false;
          }

          AnalysisEngine* analysisEngine = analysis->getAnalysisEngine( );

          EventNode*      evQueryLeave   = (EventNode*)queryLeave;

          /* link to previous matching event query */
          analysis->linkEventQuery( evQueryLeave );

          /* we can return, if the function result of the event query is unknown */
          if ( evQueryLeave->getFunctionResult( ) == EventNode::FR_UNKNOWN )
          {
            return true;
          }

          /* the query was successful -> event finished */

          /* consume mapping for this event ID */
          analysis->removeEventQuery( evQueryLeave->getEventId( ) );

          /* get the device stream ID this event is queued on */
          uint64_t refDeviceProcessId = analysis->getEventProcessId(
            evQueryLeave->getEventId( ) );
          if ( !refDeviceProcessId )
          {
            UTILS_WARNING( "[EventQueryRule] Could not process CUDA event query %s. "
                           "Device stream not found! No event with ID %" PRIu64 " recorded?",
                analysisEngine->getNodeInfo( evQueryLeave ).c_str( ),
                evQueryLeave->getEventId( ) );
            return false;
          }

          /* get the first kernel launch before eventLaunch enter */
          EventNode* eventLaunchLeave = analysis->getEventRecordLeave(
            evQueryLeave->getEventId( ) );
          if ( !eventLaunchLeave )
          {
            UTILS_WARNING( "[EventQueryRule] Could not process CUDA event query %s. "
                           "Event record not found! No event with ID %" PRIu64 " recorded?",
                analysisEngine->getNodeInfo( evQueryLeave ).c_str( ),
                evQueryLeave->getEventId( ) );
            return false;
          }

          GraphNode* kernelLaunchLeave = analysis->getLastKernelLaunchLeave(
            eventLaunchLeave->getPartner( )->getTime( ), refDeviceProcessId );

          /* get the linked kernel */
          if ( kernelLaunchLeave )
          {
            GraphNode* kernelLaunchEnter = kernelLaunchLeave->getGraphPair( ).first;
            GraphNode* kernelEnter       = (GraphNode*)kernelLaunchEnter->getLink( );
            if ( !kernelEnter )
            {
              UTILS_WARNING(
                "[EventQueryRule] %s returns success but kernel from %s is not linked!",
                analysisEngine->getNodeInfo( evQueryLeave ).c_str( ),
                analysisEngine->getNodeInfo( kernelLaunchEnter ).c_str( ) );
              return false;
            }

            GraphNode* kernelLeave       = kernelEnter->getGraphPair( ).second;

            UTILS_ASSERT( kernelLeave, "EventQueryRule: No kernel leave!" );

            if ( queryLeave->getTime( ) < kernelLeave->getTime( ) )
            {
              UTILS_WARNING( "[EventQueryRule] Successful event query %s ends"
                             " before kernel end %s.\n",
                  analysisEngine->getNodeInfo( queryLeave ).c_str( ),
                  analysisEngine->getNodeInfo( kernelLeave ).c_str( ) );
              /* return false; */
            }

            DeviceStream* devStrm =
                (DeviceStream*)analysisEngine->getStream( refDeviceProcessId );

            /* process all event query nodes and make blocking if they depend on */
            /* the kernel */
            EventNode*    firstEventQueryLeave = evQueryLeave;
            while ( true )
            {
              EventNode* prev           = (EventNode*)( firstEventQueryLeave->getLink( ) );
              if ( !prev )
              {
                break;
              }

              GraphNode* prevQueryEnter = prev->getGraphPair( ).first;
              GraphNode* prevQueryLeave = prev->getGraphPair( ).second;

              /* if kernel is running during the query */
              if ( kernelEnter->getTime( ) <= prevQueryEnter->getTime( ) )
              {
                /* make query edge blocking */
                Edge* qEdge          =
                    analysisEngine->getEdge( prevQueryEnter, prevQueryLeave );
                if ( qEdge )
                {
                  qEdge->makeBlocking( );
                }

                /* compute waiting time */
                uint64_t waitingTime = prevQueryLeave->getTime( )
                    - prevQueryEnter->getTime( );

                /* attribute waiting time to (unnecessary) query leave node */
                prevQueryLeave->incCounter( WAITING_TIME, waitingTime );
                /* kernelLeave->incCounter( BLAME, waitingTime ); */

                /* blame the kernel for the query waiting time */
                Edge* kernelEdge     =
                    analysisEngine->getEdge( kernelEnter, kernelLeave );
                if ( kernelEdge )
                {
                  kernelEdge->addBlame( waitingTime, REASON_OFLD_WAIT4DEVICE );
                }
                else
                {
                  UTILS_WARNING( "CUDA EventQueryRule: Could not find kernel edge %s -> %s",
                      analysisEngine->getNodeInfo( kernelEnter ).c_str( ),
                      analysisEngine->getNodeInfo( kernelLeave ).c_str( ) );
                }

                analysisEngine->getStatistics( ).addStatWithCount(
                  OFLD_STAT_EARLY_TEST, waitingTime );
              }

              firstEventQueryLeave = prev;
            }

            /* add kernel/last event query leave dependency */
            analysisEngine->newEdge( kernelLeave, queryLeave );

            /* commonAnalysis->getStream( kernelEnter->getStreamId() )->consumePendingKernel(); */
            /* consume all pending kernels before this kernel */
            devStrm->consumePendingKernels( kernelLeave );
          }
          else
          /* if no kernel launch leave was found (e.g. associated kernel already synchronized) */
          {
            GraphNode* queryEnter  = queryLeave->getGraphPair( ).first;

            uint64_t   waitingTime = queryLeave->getTime( ) - queryEnter->getTime( );

            /* attribute waiting time to (redundant) query leave node */
            queryLeave->incCounter( WAITING_TIME, waitingTime );
            /* queryLeave->incCounter( BLAME, waitingTime ); */

            /* blame this redundant event query */
            Edge* queryEdge        = analysisEngine->getEdge( queryEnter, queryLeave );
            if ( queryEdge )
            {
              queryEdge->addBlame( waitingTime );
            }
            else
            {
              UTILS_WARNING( "CUDA EventQueryRule: Could not find query edge %s -> %s",
                  analysisEngine->getNodeInfo( queryEnter ).c_str( ),
                  analysisEngine->getNodeInfo( queryLeave ).c_str( ) );
            }
          }

          return true;
        }
    };
  }
}
