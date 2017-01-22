/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
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
    GraphNode::GraphNodeList list; //<! list of nodes for the stream walk
    uint64_t waitStateTime;        //<! accumulated waiting time of the list members, end node waiting time is not included
    AnalysisEngine* analysis;
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
  distributeBlame( AnalysisEngine* analysis,
                   GraphNode* node,
                   uint64_t totalBlame,
                   EventStream::StreamWalkCallback callback )
  {
    // return if there is no blame to distribute
    if ( totalBlame == 0 )
    {
      return;
    }

    // walk backwards from node using callback
    StreamWalkInfo walkListInfo;
    walkListInfo.waitStateTime = 0;
    walkListInfo.analysis = analysis;
    analysis->getStream(node->getStreamId())->walkBackward(
                                                 node, callback, &walkListInfo);

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
        
        UTILS_MSG( true, " -> %s", 
                   analysis->getNodeInfo( currentWalkNode ).c_str() );
      }
    }
    
    
    UTILS_ASSERT( walkList.size() > 1,
                  "Walk list has %lu entries. Can't walk list back from %s",
                  walkList.size(), analysis->getNodeInfo( node ).c_str() );

    GraphNode* start = walkList.front();
    
    // if the start node has no caller, hence is first on the stack
    if ( start->getCaller() == NULL )
    {
      start->setCounter(BLAME, 0);
    }

    // total time interval for blame distribution
    const uint64_t totalWalkTime = walkList.front()->getTime()
      - walkList.back()->getTime();

    // time within the interval that is a wait state itself
    const uint64_t waitTime = walkListInfo.waitStateTime;

    // total time for blame distribution (wait states in the interval are subtracted)
    const uint64_t totalTimeToBlame = totalWalkTime - waitTime;
    
    // debug walk list
    if( totalWalkTime < waitTime 
       //&& strcmp( node->getName(), "MPI_Allreduce") == 0
       )
    {
      UTILS_MSG( true, "[%u] Debug walklist from %s, totalBlame: %llu sec (%lf)",
                analysis->getMPIRank(), analysis->getNodeInfo(node).c_str(),
                totalBlame, analysis->getRealTime( totalBlame ) );
    
      for ( GraphNode::GraphNodeList::const_iterator iter = ++(walkList.begin( ));
          iter != walkList.end( ); ++iter )
      {
        GraphNode* currentWalkNode = *iter;
       
        uint64_t wtime = currentWalkNode->getCounter( WAITING_TIME, NULL );
       
        UTILS_MSG( true, " -> %s with waiting time: %llu (%lf sec)", 
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
    for ( GraphNode::GraphNodeList::const_iterator iter = (++walkList.begin());
          iter != walkList.end(); ++iter )
    {
      GraphNode* currentWalkNode = *iter;

      Edge* edge = analysis->getEdge(currentWalkNode, lastWalkNode);

      UTILS_ASSERT(edge, "[%u] No edge found between %s and %s",
                   analysis->getMPIRank(),
                   currentWalkNode->getUniqueName().c_str(),
                   lastWalkNode->getUniqueName().c_str());
      
      //UTILS_MSG( currentWalkNode->getId() == 19, "Current walk node: %s",
      //           currentWalkNode->getUniqueName().c_str() );
      
      // blame distribution depends on the edge type
      // if edge is from enter to leave node of a region (represents a region)
      if ( edge->isRegion() ) 
      {
        uint64_t edgeCPURegionTime  = edge->getCPUNodesExclTime();
        uint64_t edgeNodeRegionTime = edge->getDuration() - edgeCPURegionTime;
        
        if( edgeNodeRegionTime > 0 )
        {
          //\todo: accuracy gets lost here, store result into double
          uint64_t ratioBlame = (double) totalBlame
                              * (double) edgeNodeRegionTime
                              / (double) totalTimeToBlame;
          
          // check for zero to avoid unnecessary find in incCounter()
          if ( ratioBlame > 0 )
          {
            lastWalkNode->incCounter(BLAME, ratioBlame);
          }
        }

        if( edge->getCPUNodesExclTime() )
        {
          double cpuBlame = (double) totalBlame
                          * (double) edgeCPURegionTime
                          / (double) totalTimeToBlame;
          
          edge->addCPUBlame( cpuBlame );
        }
      }
      else
      {
        // all blame on the edge, if it is not a region
        double cpuBlame = (double) totalBlame
          * (double) edge->getDuration()
          / (double) totalTimeToBlame;

        if( cpuBlame > 0 )
        {
          edge->addCPUBlame(cpuBlame);
          
          // enter node of following paradigm region
          //lastWalkNode->incCounter(BLAME, cpuBlame);
        }
      }

      lastWalkNode = currentWalkNode;
    }
  }
}
