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
  class EventQueryRule :
    public ICUDARule
  {
    public:

      EventQueryRule( int priority ) :
        ICUDARule( "EventQueryRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {

        if ( !node->isCUDAEventQuery( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        EventNode*      evQueryLeave   = (EventNode*)node;

        /* link to previous matching event query */
        analysis->linkEventQuery( evQueryLeave );

        if ( evQueryLeave->getFunctionResult( ) == EventNode::FR_UNKNOWN )
        {
          /* nothing to do here */
          return true;
        }

        /* get the complete execution */
        GraphNode::GraphNodePair& evQuery = evQueryLeave->getGraphPair( );

        /* consume mapping for this event ID */
        analysis->removeEventQuery( evQueryLeave->getEventId( ) );

        /* get the device stream ID this event is queued on */
        uint64_t refDeviceProcessId       = analysis->getEventProcessId(
          evQueryLeave->getEventId( ) );
        if ( !refDeviceProcessId )
        {
          ErrorUtils::getInstance( ).throwFatalError(
            "Could not find device stream ID for event %" PRIu64 " from %s",
            evQueryLeave->getEventId( ),
            evQueryLeave->getUniqueName( ).c_str( ) );
        }

        /* get the first kernel launch before eventLaunch/enter */
        EventNode* eventLaunchLeave  = analysis->getEventRecordLeave(
          evQueryLeave->getEventId( ) );
        if ( !eventLaunchLeave )
        {
          throw RTException( "Could not find event record for event %" PRIu64,
                             evQueryLeave->getEventId( ) );
        }

        GraphNode* kernelLaunchLeave = analysis->getLastLaunchLeave(
          eventLaunchLeave->getGraphPair( ).first->getTime( ),
          refDeviceProcessId );

        if ( kernelLaunchLeave )
        {
          GraphNode::GraphNodePair& kernelLaunch =
            ( (GraphNode*)kernelLaunchLeave )->getGraphPair( );
          GraphNode* kernelEnter =
            (GraphNode*)kernelLaunch.first->getLink( );
          if ( !kernelEnter )
          {
            ErrorUtils::getInstance( ).throwError(

              "Event query %s (%f) returns success but kernel from %s (%f) did not finish yet",
              evQueryLeave->getUniqueName( )
              .c_str( ),
              commonAnalysis->getRealTime(
                evQueryLeave->getTime( ) ),
              kernelLaunch.first->
              getUniqueName( ).c_str( ),
              commonAnalysis->getRealTime(
                kernelLaunch.first->getTime( ) ) );
            return false;
          }

          GraphNode* kernelLeave          = kernelEnter->getGraphPair( ).second;
          GraphNode::GraphNodePair kernel = kernelLeave->getGraphPair( );

          if ( evQuery.second->getTime( ) < kernelLeave->getTime( ) )
          {
            throw RTException( "Incorrect timing between %s and %s\n",
                               evQuery.second->getUniqueName( ).c_str( ),
                               kernelLeave->getUniqueName( ).c_str( ) );
          }

          /* walk all event query nodes and make blocking if they */
          /* depend on the kernel */
          EventNode* firstEventQueryLeave = evQueryLeave;
          while ( true )
          {
            EventNode* prev = (EventNode*)( firstEventQueryLeave->getLink( ) );
            if ( !prev )
            {
              break;
            }

            GraphNode::GraphNodePair& prevQuery = prev->getGraphPair( );

            if ( kernel.first->getTime( ) <= prevQuery.first->getTime( ) )
            {
              commonAnalysis->getEdge(
                prevQuery.first, prevQuery.second )->makeBlocking( );

              /* set counters */
              prevQuery.second->incCounter(
                commonAnalysis->getCtrTable( ).getCtrId( CTR_WAITSTATE ),
                prevQuery.second->getTime( ) - prevQuery.first->getTime( ) );
              kernel.second->incCounter(
                commonAnalysis->getCtrTable( ).getCtrId( CTR_BLAME ),
                prevQuery.second->getTime( ) - prevQuery.first->getTime( ) );

              /* add a blocking dependency, so it cannot be used */
              /* for critical path analysis */
              commonAnalysis->newEdge( kernelLeave,
                                       prevQuery.second,
                                       EDGE_IS_BLOCKING );
            }

            firstEventQueryLeave = prev;
          }

          /* add kernel/last event query leave dependency */
          commonAnalysis->newEdge( kernelLeave, evQuery.second );
          return true;
        }

        return false;
      }
  };
 }
}
