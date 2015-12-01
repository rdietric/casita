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

#include "AnalysisEngine.hpp"

namespace casita
{

 typedef struct
 {
   GraphNode::GraphNodeList list;
   uint64_t                 waitStateTime;
   AnalysisEngine*          analysis;
 } StreamWalkInfo;

 /**
  * Distribute the blame by walking backwards from the given node.
  * Blame is assigned to all nodes in the blame interval and to edges between 
  * the nodes to blame CPU functions later.
  * 
  * @param analysis pointer to analysis engine
  * @param node start node of the stream walk back
  * @param totalBlame blame to be distributed
  * @param callback 
  */
 static void
 distributeBlame( AnalysisEngine*                 analysis,
                  GraphNode*                      node,
                  uint64_t                        totalBlame,
                  EventStream::StreamWalkCallback callback )
 {
   if ( totalBlame == 0 )
   {
     return;
   }

   // walk backwards from node using callback
   StreamWalkInfo walkListAndWaitTime;
   walkListAndWaitTime.waitStateTime = 0;
   walkListAndWaitTime.analysis      = analysis;
   analysis->getStream( node->getStreamId( ) )->walkBackward(
     node, callback, &walkListAndWaitTime );

   const GraphNode::GraphNodeList& walkList = walkListAndWaitTime.list;
   const uint32_t waitTime      = walkListAndWaitTime.waitStateTime;

   if ( walkList.size( ) < 2 )
   {
     ErrorUtils::getInstance( ).throwError( "Can't walk list back from %s",
                                            node->getUniqueName( ).c_str( ) );
   }

   GraphNode* start = walkList.front( );
   
   /*if ( false )
   {
     GraphNode* end   = walkList.back( );
     std::cout << "[" << analysis->getMPIRank( ) << "] Walking: " <<
     start->getUniqueName( )
               << " to " << end->getUniqueName( ) << " node " <<
     node->getUniqueName( ) << " Blame: " <<
     totalBlame << std::endl;
   }*/

   uint64_t   totalWalkTime = walkList.front( )->getTime( ) 
                            - walkList.back( )->getTime( );
   GraphNode* lastWalkNode  = walkList.front( );

   // if the start node has no caller, hence is first on the stack
   if ( start->getCaller( ) == NULL )
   {
     start->setCounter( BLAME, 0 );
   }

   // iterate over the walk list
   for ( GraphNode::GraphNodeList::const_iterator iter = ( ++walkList.begin( ) );
         iter != walkList.end( ); ++iter )
   {
     GraphNode* currentWalkNode = *iter;
     
     Edge* edge = analysis->getEdge( currentWalkNode, lastWalkNode );
     
     uint64_t cpuTimeDiff = lastWalkNode->getTime( ) 
                          - edge->getCPUNodesStartTime( );
     uint64_t timeDiff    = lastWalkNode->getTime( ) 
                          - currentWalkNode->getTime( ) - cpuTimeDiff;

     const uint64_t currentWaitingTime =
                                   lastWalkNode->getCounter( WAITING_TIME, NULL );

     uint64_t ratioBlame = (double)totalBlame 
                         * (double)( timeDiff - currentWaitingTime ) 
                         / (double)( totalWalkTime - waitTime );

     uint64_t cpuBlame   = ( cpuTimeDiff == 0 ) ? 0 : (double)totalBlame *
                                         (double)( cpuTimeDiff ) /
                                         (double)( totalWalkTime - waitTime );

     edge->addCPUBlame( cpuBlame );
     
     /*
     if ( cpuBlame > 0 && false )
     {
       std::cout << "Write cpuBlame " << cpuBlame << " (of " << totalBlame
                 << " and " << ratioBlame << " cpuTimeDiff: " << cpuTimeDiff
                 << " timediff: " << timeDiff << " current waiting time: "
                 << currentWaitingTime << " total walk time: "
                 << totalWalkTime << " waitTime: "
                 << waitTime << ") between "
                 << currentWalkNode->getUniqueName( ) << " and "
                 << lastWalkNode->getUniqueName( )
                 << " " << edge->getNumberOfCPUNodes( ) << " Nodes ("
                 << edge->getCPUNodesStartTime( ) << " - "
                 << edge->getCPUNodesEndTime( ) << ")" << std::endl;
     }*/

     if ( ratioBlame > 0 )
     {
       lastWalkNode->incCounter( BLAME, ratioBlame );
     }

     lastWalkNode = currentWalkNode;
   }
 }
}
