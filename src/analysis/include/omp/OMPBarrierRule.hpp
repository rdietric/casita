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

#include "IOMPRule.hpp"
#include "AnalysisParadigmOMP.hpp"
#include "utils/ErrorUtils.hpp"

namespace casita
{
 namespace omp
 {

  class OMPBarrierRule :
    public IOMPRule
  {
    public:

      OMPBarrierRule( int priority ) :
        IOMPRule( "OMPBarrierRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* barrierLeave )
      {
        if ( !barrierLeave->isOMPSync() || !barrierLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();
        
        // count occurrence
        commonAnalysis->getStatistics().countActivity( STAT_OMP_BARRIER );
        
        GraphNode*      barrierEnter   = barrierLeave->getGraphPair().first;
        EventStream*    nodeStream     = commonAnalysis->getStream(
          barrierLeave->getStreamId() );

        // this rule ignores device streams (see target barrier rule)
        if ( nodeStream->isDeviceStream() )
        {
          return false;
        }
        
        // save barrier enter events to BarrierEventList on the host
        analysis->addBarrierEventToList( barrierEnter, false );

        const EventStreamGroup::EventStreamList& streams =
          commonAnalysis->getHostStreams();
        
        // get list with all barrier enter events on the host
        const GraphNode::GraphNodeList& barrierList =
          analysis->getBarrierEventList( false );

        // check if all barriers were passed
        //\todo: this does not work for nested parallel regions
        if ( streams.size() == barrierList.size() )
        {
          GraphNode::GraphNodeList::const_iterator iter = barrierList.begin();
        
          // keep enter event with latest enter timestamp
          GraphNode* latestEnterNode = *iter;                               
          
          // find last barrierEnter
          for (; iter != barrierList.end(); ++iter )
          {
            if ( ( *iter )->getTime() > latestEnterNode->getTime() )
            {
              latestEnterNode = *iter;
            }
          }

          // accumulate blame, set edges from latest enter to all other leaves
          uint64_t blame = 0;
          for ( iter = barrierList.begin(); iter != barrierList.end(); ++iter )
          {
            GraphNode::GraphNodePair& barrier = ( *iter )->getGraphPair();
            
            // for blocking barrier regions
            if ( barrier.first != latestEnterNode )
            {
              Edge* barrierEdge = commonAnalysis->getEdge( barrier.first,
                                                           barrier.second );
              
              if( barrierEdge )
              {
                barrierEdge->makeBlocking();
              }
              else
              {
                commonAnalysis->newEdge( barrier.first, barrier.second, true );
              }              
              
              uint64_t wtime = 
                latestEnterNode->getTime() - barrier.first->getTime();
              
              commonAnalysis->getStatistics().addStatWithCount( 
                OMP_STAT_BARRIER, wtime );
              
              // compute waiting time for this barrier region
              barrier.second->setCounter( WAITING_TIME, wtime );

              // create edge from latest barrier enter to other leaves
              // (non-blocking edge from blocking barrier leave node)
              commonAnalysis->newEdge( latestEnterNode, barrier.second );

              blame += latestEnterNode->getTime() - barrier.first->getTime();
            }
          }

          // set blame
          distributeBlame( commonAnalysis,
                           latestEnterNode,
                           blame,
                           streamWalkCallback,
                           REASON_OMP_BARRIER );

          // clear list of buffered barriers
          analysis->clearBarrierEventList( false );

          return true;
        }

        return false;
      }
  };

 }
}
