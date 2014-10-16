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

#include "AnalysisEngine.hpp"

namespace casita
{

 typedef struct
 {
   GraphNode::GraphNodeList list;
   uint64_t                 waitStateTime;
   AnalysisEngine*          analysis;
 } StreamWalkInfo;

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

   /* walk backwards from node using callback */
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

   GraphNode* start             = walkList.front( );
   GraphNode* end               = walkList.back( );
   if ( false )
   {
     std::cout << "[" << analysis->getMPIRank( ) << "]Walking: " <<
     start->getUniqueName( )
               << " to " << end->getUniqueName( ) << " node " <<
     node->getUniqueName( ) << " Blame: " <<
     totalBlame << std::endl;
   }

   uint64_t       totalWalkTime = walkList.front( )->getTime( ) -
                                  walkList.back( )->getTime( );
   GraphNode*     lastWalkNode  = walkList.front( );

   const uint32_t waitCtrId     = analysis->getCtrTable( ).getCtrId( CTR_WAITSTATE );
   const uint32_t blameCtrId    = analysis->getCtrTable( ).getCtrId( CTR_BLAME );

   if ( start->getCaller( ) == NULL )
   {
     start->setCounter( blameCtrId, 0 );
   }

   for ( GraphNode::GraphNodeList::const_iterator iter = ( ++walkList.begin( ) );
         iter != walkList.end( ); ++iter )
   {
     GraphNode*     currentWalkNode    = *iter;
     Edge*          edge = analysis->getEdge( currentWalkNode,
                                              lastWalkNode );
     uint64_t       cpuTimeDiff        = lastWalkNode->getTime( ) -
                                         edge->getCPUNodesStartTime( );
     uint64_t       timeDiff           = lastWalkNode->getTime( ) -
                                         currentWalkNode->getTime( )
                                         - cpuTimeDiff;

     const uint64_t currentWaitingTime =
       lastWalkNode->getCounter( waitCtrId, NULL );

     uint64_t       ratioBlame         = (double)totalBlame *
                                         (double)( timeDiff -
                                                   currentWaitingTime ) /
                                         (double)( totalWalkTime - waitTime );

     uint64_t       cpuBlame           = ( cpuTimeDiff == 0 ) ? 0 : (double)totalBlame *
                                         (double)( cpuTimeDiff ) /
                                         (double)( totalWalkTime - waitTime );

     edge->addCPUBlame( cpuBlame );
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
     }

     if ( ratioBlame > 0 )
     {
       lastWalkNode->incCounter( blameCtrId, ratioBlame );
     }

     lastWalkNode = currentWalkNode;
   }
 }
}
