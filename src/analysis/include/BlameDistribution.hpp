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

#include "AnalysisEngine.hpp"
#include "StreamWalk.h"

#include "mpi/MPIRulesCommon.hpp"
#include "omp/OMPRulesCommon.hpp"
#include "offload/OffloadRulesCommon.hpp"

namespace casita
{

//  typedef struct
//  {
//    GraphNode::GraphNodeList list; //<! list of nodes for the stream walk
//    uint64_t waitStateTime;        //<! accumulated waiting time of the list members, end node waiting time is not included
//    AnalysisEngine* analysis;
//  } StreamWalkInfo;
  
  //static void
  //propagateBlame( AnalysisEngine* analysis, GraphNode* node,
  //                double blame, BlameReason reason );

  /**
   * Distribute the blame by walking backwards from the given node.
   * Blame is assigned to edges in the blame interval.
   * 
   * @param analysis pointer to analysis engine
   * @param node start node of the stream walk back
   * @param totalBlame blame to be distributed
   * @param callback 
   * @param unaccounted time in this stream walkback
   * 
   * @return total time to blame
   */
  static uint64_t
  distributeBlame( AnalysisEngine* analysis,
                   GraphNode* node,
                   uint64_t totalBlame,
                   EventStream::StreamWalkCallback callback,
                   BlameReason reason = REASON_UNCLASSIFIED )
  {
    // return if there is no blame to distribute
    if ( totalBlame == 0 )
    {
      return 0;
    }

    // walk backwards from node using callback
    StreamWalkInfo walkListInfo;
    walkListInfo.waitStateTime = 0;
    walkListInfo.analysis = analysis;
    analysis->getStream( node->getStreamId())->walkBackward(
                                                node, callback, &walkListInfo );

    // the walk list includes the interval boundary nodes, e.g. MPI leave as 
    // start node (walklist.front()) and MPI enter as end node (walklist.back())
    const GraphNode::GraphNodeList& walkList = walkListInfo.list;

    // ensure that the walk list contains at least the two boundary nodes
    if( walkList.size() < 2 )
    {
      for ( GraphNode::GraphNodeList::const_iterator iter = walkList.begin();
          iter != walkList.end(); ++iter )
      {
        GraphNode* currentWalkNode = *iter;
        
        UTILS_OUT( " -> %s", analysis->getNodeInfo( currentWalkNode ).c_str() );
      }
    }
    
    UTILS_ASSERT( walkList.size() > 1,
                  "Walk list has %lu entries. Can't walk list back from %s",
                  walkList.size(), analysis->getNodeInfo( node ).c_str() );

    // total time interval for blame distribution
    const uint64_t totalWalkTime = 
      walkList.front()->getTime() - walkList.back()->getTime();

    // time within the interval that is a wait state itself
    const uint64_t waitTime = 0; //walkListInfo.waitStateTime;

    // total time for blame distribution 
    // (wait states in the interval are subtracted)
    const uint64_t totalTimeToBlame = totalWalkTime - waitTime;
    
    // debug walk list
    if( totalWalkTime < waitTime )
    {
      UTILS_OUT( "[%u] Debug walklist from %s, totalBlame: %llu sec (%lf)",
                 analysis->getMPIRank(), analysis->getNodeInfo(node).c_str(),
                 totalBlame, analysis->getRealTime( totalBlame ) );
    
      for( GraphNode::GraphNodeList::const_iterator iter = ++(walkList.begin());
           iter != walkList.end(); ++iter )
      {
        GraphNode* currentWalkNode = *iter;
       
        uint64_t wtime = currentWalkNode->getWaitingTime();
       
        UTILS_OUT( " -> %s with waiting time: %llu (%lf sec)", 
                   analysis->getNodeInfo( currentWalkNode ).c_str(), 
                   wtime, analysis->getRealTime( wtime ) );
      }
    }
    
    // total time to blame has to be greater than zero
    UTILS_ASSERT(totalWalkTime >= waitTime,
                 "[%u] Waiting time %llu (%lf sec) in the time interval [%s,%s] is "
                 "greater than its duration %llu (%lf sec)",
                 analysis->getMPIRank(), 
                 waitTime, analysis->getRealTime( waitTime ),
                 analysis->getNodeInfo( walkList.front() ).c_str(),
                 analysis->getNodeInfo( walkList.back() ).c_str(), 
                 totalWalkTime, analysis->getRealTime( totalWalkTime ) );

    GraphNode* lastWalkNode = walkList.front();

    // iterate (backwards in time) over the walk list which contains enter and leave events
    // (ignore the first node, as we work on edges)
    for ( GraphNode::GraphNodeList::const_iterator iter = ( ++walkList.begin() );
          iter != walkList.end(); ++iter )
    {
      GraphNode* currentWalkNode = *iter;

      // get (forward) edge (from timely last to earliest)
      Edge* edge = analysis->getEdge( currentWalkNode, lastWalkNode );

      UTILS_ASSERT( edge, "[%u] No edge found between %s and %s",
                    analysis->getMPIRank(),
                    analysis->getNodeInfo( currentWalkNode ).c_str(),
                    analysis->getNodeInfo( lastWalkNode ).c_str() );
      
      //UTILS_MSG( currentWalkNode->getId() == 19, "Current walk node: %s",
      //           currentWalkNode->getUniqueName().c_str() );
      
      // add partial blame to current edge
      double blame = (double) totalBlame
                   * (double) edge->getDuration()
                   / (double) totalTimeToBlame;
      
      //// propagate blame (not for MPI) ////
      if( Parser::getInstance().getProgramOptions().propagateBlame &&
          edge->isBlocking() && lastWalkNode->isLeave() && !lastWalkNode->isMPI() )
      {
        //propagateBlame( lastWalkNode, blame, reason );
        const Graph::EdgeList* inEdges = 
          analysis->getGraph().getInEdgesPtr( lastWalkNode );
        // make sure that there are edges
        if ( inEdges )
        {
          // iterate over the in edges of the node
          for ( Graph::EdgeList::const_iterator eIt = inEdges->begin();
                eIt != inEdges->end(); ++eIt )
          {
            Edge* interEdge = *eIt;
            
            // only for edges between streams (to avoid the wait state)
            if ( interEdge->isInterStreamEdge() )
            {
              if ( lastWalkNode->isOMP() )
              {
                distributeBlame( analysis, interEdge->getStartNode(), 
                                 (uint64_t)blame, ompHostStreamWalkCallback, 
                                 reason );
              }
              else if ( lastWalkNode->isOfld() )
              {
                distributeBlame( analysis, interEdge->getStartNode(), 
                                 (uint64_t)blame, ofldStreamWalkCallback, 
                                 reason );
              }
              else
              {
                edge->addBlame( blame, reason );
              }
            }
          }
        }
      }
      else
      {
//        UTILS_MSG( analysis->getMPIRank() == 0 && lastWalkNode->isLeave() && 
//                   strcmp( lastWalkNode->getName(), "!$omp implicit barrier @calculateTauMatrix.cpp:506" ) == 0, 
//                   "[%u] Blame %s (edge: %s, reason: %d. from %s)", 
//                   analysis->getMPIRank(), 
//                   analysis->getNodeInfo( lastWalkNode ).c_str(),
//                   edge->getName().c_str(),
//                   reason, analysis->getNodeInfo( node ).c_str());
        edge->addBlame( blame, reason );
      }

      lastWalkNode = currentWalkNode;
    }
    
    return totalTimeToBlame;
  }
  
  //static void
  //propagateBlame( AnalysisEngine* analysis, GraphNode* node,
  //                double blame, BlameReason reason )
  //{
  
  //}
}

  