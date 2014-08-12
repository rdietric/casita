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

#include "AbstractRule.hpp"

namespace casita
{

 class EventSyncRule :
   public AbstractRule
 {
   public:

     EventSyncRule( int priority ) :
       AbstractRule( "EventSyncRule", priority )
     {

     }

     bool
     apply( AnalysisEngine* analysis, GraphNode* node )
     {

       if ( !node->isCUDAEventSync( ) || !node->isLeave( ) )
       {
         return false;
       }

       /* get the complete execution */
       GraphNode::GraphNodePair& sync = node->getGraphPair( );

       EventNode* eventLaunchLeave = analysis->getLastEventLaunchLeave(
         ( (EventNode*)sync.second )->getEventId( ) );

       if ( !eventLaunchLeave )
       {
         printf( " * Ignoring event sync %s without matching event record\n",
                 node->getUniqueName( ).c_str( ) );
         return false;
       }

       GraphNode* eventLaunchEnter = eventLaunchLeave->getGraphPair( ).first;
       EventStream* refProcess = analysis->getStream(
         eventLaunchLeave->getReferencedStreamId( ) );
       EventStreamGroup::EventStreamList deviceProcs;

       if ( refProcess->isDeviceNullStream( ) )
       {
         analysis->getAllDeviceStreams( deviceProcs );
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
         GraphNode* kernelLaunchLeave = analysis->getLastLaunchLeave(
           eventLaunchEnter->getTime( ), ( *iter )->getId( ) );
         if ( !kernelLaunchLeave )
         {
           continue;
         }

         GraphNode::GraphNodePair& kernelLaunch =
           ( (GraphNode*)kernelLaunchLeave )->getGraphPair( );

         GraphNode* kernelEnter = (GraphNode*)kernelLaunch.first->getLink( );
         if ( !kernelEnter )
         {
           throw RTException(

                   "Event sync %s (%f) returned but kernel from %s (%f) on stream [%u, %s] did not start/finish yet",
                   node->getUniqueName( ).c_str( ),
                   analysis->getRealTime( node->getTime( ) ),
                   kernelLaunch.first->getUniqueName( ).c_str( ),
                   analysis->getRealTime( kernelLaunch.first->
                                          getTime( ) ),
                   kernelLaunch.first->getReferencedStreamId( ),
                   analysis->getStream( kernelLaunch.first->
                                        getReferencedStreamId( ) )->getName( ) );
         }

         GraphNode* kernelLeave = kernelEnter->getGraphPair( ).second;
         if ( !kernelLeave || kernelLeave->getTime( ) > sync.second->getTime( ) )
         {
           throw RTException(

                   "Event sync %s (%f) returned but kernel from %s (%f) on stream [%u, %s] did not finish yet",
                   node->getUniqueName( ).c_str( ),
                   analysis->getRealTime( node->getTime( ) ),
                   kernelLaunch.first->getUniqueName( ).c_str( ),
                   analysis->getRealTime( kernelLaunch.first->
                                          getTime( ) ),
                   kernelLaunch.first->getReferencedStreamId( ),
                   analysis->getStream( kernelLaunch.first->
                                        getReferencedStreamId( ) )->getName( ) );
         }

         uint64_t syncDeltaTicks = analysis->getDeltaTicks( );

         if ( ( sync.first->getTime( ) < kernelLeave->getTime( ) ) &&
              ( sync.second->getTime( ) - kernelLeave->getTime( ) <=
                syncDeltaTicks ) )
         {
           analysis->getEdge( sync.first, sync.second )->makeBlocking( );

           /* set counters */
           sync.first->incCounter( analysis->getCtrTable( ).getCtrId(
                                     CTR_WAITSTATE ),
                                   sync.second->getTime( ) -
                                   std::max( sync.first->getTime( ),
                                             kernelEnter->getTime( ) ) );
           kernelEnter->incCounter( analysis->getCtrTable( ).getCtrId(
                                      CTR_BLAME ),
                                    sync.second->getTime( ) -
                                    std::max( sync.first->getTime( ),
                                              kernelEnter->getTime( ) ) );
         }

         analysis->newEdge( kernelLeave, sync.second, EDGE_CAUSES_WAITSTATE );
         ruleResult = true;
       }

       return ruleResult;
     }
 };

}
