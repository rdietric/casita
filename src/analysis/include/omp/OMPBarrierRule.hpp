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
      apply( AnalysisParadigmOMP* ompAnalysis, GraphNode* barrierLeave )
      {
        if ( !barrierLeave->isOMPSync() || !barrierLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* analysisEngine = ompAnalysis->getAnalysisEngine();
        
        // count occurrence
        analysisEngine->getStatistics().countActivity( STAT_OMP_BARRIER );
        
        GraphNode*   barrierEnter = barrierLeave->getGraphPair().first;
        EventStream* nodeStream   = analysisEngine->getStream(
          barrierLeave->getStreamId() );

        // this rule ignores device streams (see target barrier rule)
        if ( nodeStream->isDeviceStream() )
        {
          return false;
        }
        
        // save barrier enter events to BarrierEventList on the host
        ompAnalysis->addBarrierEventToList( barrierEnter, false );
        
        // get list with all barrier enter events on the host
        const GraphNode::GraphNodeList& barrierList =
          ompAnalysis->getBarrierEventList( false );

        GraphNode *fork = ompAnalysis->getInnerMostFork();
        if( NULL == fork )
        {
          UTILS_WARNING( "[OMPBarrierRule] Could not find fork node." );
          return false;
        }
        
        // check if all barriers were passed
        //\todo: this might not work for nested parallel regions
        //if ( streams.size() == barrierList.size() )
        if( barrierList.size() == fork->getReferencedStreamId() )
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
              Edge* barrierEdge = analysisEngine->getEdge( barrierEnter,
                                                           barrier.second );
              
              if( barrierEdge )
              {
                barrierEdge->makeBlocking();
              }
              else
              {
                analysisEngine->newEdge( barrier.first, barrier.second, true );
              }              
              
              uint64_t wtime = 
                latestEnterNode->getTime() - barrier.first->getTime();
              
              analysisEngine->getStatistics().addStatWithCount( 
                OMP_STAT_BARRIER, wtime );
              
              // compute waiting time for this barrier region
              barrier.second->setCounter( WAITING_TIME, wtime );

              // create edge from latest barrier enter to other leaves
              // (non-blocking edge from blocking barrier leave node)
              analysisEngine->newEdge( latestEnterNode, barrier.second );

              blame += latestEnterNode->getTime() - barrier.first->getTime();
            }
          }

          // set blame
          distributeBlame( analysisEngine,
                           latestEnterNode,
                           blame,
                           streamWalkCallback,
                           REASON_OMP_BARRIER );

          // clear list of buffered barriers
          ompAnalysis->clearBarrierEventList( false );

          return true;
        }

        return false;
      }
  };

 }
}
