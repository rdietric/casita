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
   uint64_t waitStateTime;
 } StreamWalkInfo;

 static void
 distributeBlame( AnalysisEngine* analysis,
                  GraphNode* node,
                  uint64_t totalBlame,
                  EventStream::StreamWalkCallback callback )
 {
   if ( totalBlame == 0 )
   {
     return;
   }

   StreamWalkInfo walkListAndWaitTime;

   walkListAndWaitTime.waitStateTime = 0;
   analysis->getStream( node->getStreamId( ) )->walkBackward(
     node, callback, &walkListAndWaitTime );

   GraphNode::GraphNodeList walkList = walkListAndWaitTime.list;
   uint32_t waitTime = walkListAndWaitTime.waitStateTime;

   if ( walkList.size( ) < 2 )
   {
     ErrorUtils::getInstance( ).throwError( "Can't walk list back from %s",
                                            node->getUniqueName( ).c_str( ) );
   }

   GraphNode* start = walkList.front( );
   GraphNode* end = walkList.back( );
   if ( false )
   {
     std::cout << "[" << analysis->getMPIRank( ) << "]Walking: " <<
     start->getUniqueName( )
               << " to " << end->getUniqueName( ) << " Blame: " <<
     totalBlame << std::endl;
   }

   uint64_t totalWalkTime = walkList.front( )->getTime( ) -
                            walkList.back( )->getTime( );
   GraphNode* lastWalkNode = walkList.front( );

   uint32_t waitCtrId = analysis->getCtrTable( ).getCtrId( CTR_WAITSTATE );
   uint32_t blameCtrId = analysis->getCtrTable( ).getCtrId( CTR_BLAME );
   uint32_t statBlameCtrId = analysis->getCtrTable( ).getCtrId(
     CTR_BLAME_STATISTICS );

   for ( GraphNode::GraphNodeList::const_iterator iter = ( ++walkList.begin( ) );
         iter != walkList.end( ); ++iter )
   {
     GraphNode* currentWalkNode = *iter;
     Edge* edge = analysis->getEdge( currentWalkNode, lastWalkNode );
     uint64_t cpuTimeDiff = lastWalkNode->getTime( ) -
                            edge->getCPUNodesStartTime( );
     uint64_t timeDiff = lastWalkNode->getTime( ) - currentWalkNode->getTime( )
                         - cpuTimeDiff;
     uint64_t ratioBlame = (double)totalBlame *
                           (double)( timeDiff -
                                     currentWalkNode->getCounter( waitCtrId,
                                                                  NULL ) ) /
                           (double)( totalWalkTime - waitTime );

     uint64_t cpuBlame = cpuTimeDiff == 0 ? 0 : (double)totalBlame *
                         (double)( cpuTimeDiff -
                                   currentWalkNode->getCounter( waitCtrId,
                                                                NULL ) ) /
                         (double)( totalWalkTime - waitTime );

     edge->addCPUBlame( cpuBlame );
     if ( cpuBlame > 0 && false )
     {
       std::cout << "Write cpuBlame " << cpuBlame << " (of " << totalBlame
                 << " and " << ratioBlame << " cpuTimeDiff: " << cpuTimeDiff
                 << " timediff: " << timeDiff << ") between "
                 << currentWalkNode->getUniqueName( ) << " and "
                 << lastWalkNode->getUniqueName( )
                 << " " << edge->getNumberOfCPUNodes( ) << " Nodes ("
                 << edge->getCPUNodesStartTime( ) << " - "
                 << edge->getCPUNodesEndTime( ) << std::endl;
     }

     if ( ratioBlame > 0 )
     {
       currentWalkNode->incCounter( blameCtrId, ratioBlame );
     }

     /*bool activityIsWaitstate = currentWalkNode->isWaitstate();
     if (!activityIsWaitstate && currentWalkNode->isEnter())
     {
         Edge *edge = analysis->getEdge(currentWalkNode, lastWalkNode);
         if (edge->isBlocking())
             activityIsWaitstate = true;
     }

     if (!activityIsWaitstate)
     {
         currentWalkNode->incCounter(blameCtrId, ratioBlame);
     } else
     {
         if (currentWalkNode->isEnter() &&
                 (lastWalkNode == currentWalkNode->getPartner()))
         {
             // find the ratio of blame that has already been attributed to
             // the root cause
             Graph &g = analysis->getGraph();
             const Graph::EdgeList &edges = g.getInEdges(lastWalkNode);
             for (Graph::EdgeList::const_iterator eIter = edges.begin();
                     eIter != edges.end(); ++eIter)
             {
                 Edge *e = *eIter;
                 if (e->causesWaitState())
                 {
                     GraphNode *rootEnter = NULL, *rootLeave = NULL;
                     uint64_t myBlameRatio = 0;

                     if (e->getStartNode()->isEnter())
                     {
                         rootEnter = e->getStartNode();
                         rootLeave = rootEnter->getPartner();
                     } else
                     {
                         rootLeave = e->getStartNode();
                         rootEnter = rootLeave->getPartner();
                     }

                     // get non-overlap part after my leave
                     if (rootLeave->getTime() > lastWalkNode->getTime())
                     {
                         myBlameRatio +=
                                 (double) ratioBlame *
                                 (double) (rootLeave->getTime() - lastWalkNode->getTime()) /
                                 (double) (lastWalkNode->getTime() - currentWalkNode->getTime()
                                 - waitTime);
                     }

                     // get non-overlap part before my enter
                     if (rootEnter->getTime() > currentWalkNode->getTime())
                     {
                         myBlameRatio +=
                                 (double) ratioBlame *
                                 (double) (rootEnter->getTime() - currentWalkNode->getTime()) /
                                 (double) (lastWalkNode->getTime() - currentWalkNode->getTime()
                                 - waitTime);
                     }

                     currentWalkNode->incCounter(blameCtrId, myBlameRatio);
                     break;
                 }
             }
         }
     }*/

     /* caller is always enter node */
     if ( currentWalkNode->isLeave( ) && currentWalkNode->getCaller( ) &&
          ratioBlame > 0 )
     {
       currentWalkNode->getCaller( )->incCounter( statBlameCtrId, ratioBlame );
     }

     lastWalkNode = currentWalkNode;
   }
 }
}
