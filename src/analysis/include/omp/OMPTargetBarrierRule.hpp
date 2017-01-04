/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014-2015,
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

namespace casita
{
 namespace omp
 {

  class OMPTargetBarrierRule :
    public IOMPRule
  {
    public:

      OMPTargetBarrierRule( int priority ) :
        IOMPRule( "OMPTargetBarrierRule", priority )
      {

      }

    private:
      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* node )
      {
        if ( !node->isOMPSync( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );
        EventStream*    nodeStream     = commonAnalysis->getStream(
          node->getStreamId( ) );

        if ( !nodeStream->isDeviceStream( ) )
        {
          return false;
        }

        if ( node->isEnter( ) )
        {
          // enter
          bool     valid = false;
          uint64_t matchingId           = node->getCounter( OMP_REGION_ID, &valid );
          if ( !valid )
          {
            ErrorUtils::getInstance( ).throwError(
              "OMP Target Barrier enter without matching ID (%s)",
              node->getUniqueName( ).c_str( ) );
            return false;
          }

          /* save barrier enter events to BarrierEventList */
          analysis->addBarrierEventToList( node, true, matchingId );

          return true;
        }
        else
        {
          // leave
          GraphNode* enterEvent  = node->getPartner( );
          
          bool valid = false;
          
          uint64_t matchingId    = enterEvent->getCounter( OMP_REGION_ID, &valid );
          if ( !valid )
          {
            ErrorUtils::getInstance( ).throwError(
              "OMP Target Barrier leave without matching ID at partner (%s)",
              node->getUniqueName( ).c_str( ) );
            return false;
          }

          /* save barrier leave events to BarrierEventList, too */
          analysis->addBarrierEventToList( node, true, matchingId );

          const GraphNode::GraphNodeList& barrierList =
            analysis->getBarrierEventList( true, NULL, matchingId );

          size_t numLeaveNodesInList = 0;
          for ( GraphNode::GraphNodeList::const_reverse_iterator rIter =
                  barrierList.rbegin( );
                rIter != barrierList.rend( ); ++rIter )
          {
            if ( ( *rIter )->isEnter( ) )
            {
              break;
            }

            numLeaveNodesInList++;
          }

          /* not yet all barrier leaves found */
          if ( numLeaveNodesInList * 2 < barrierList.size( ) )
          {
            return false;
          }
          assert( numLeaveNodesInList * 2 == barrierList.size( ) );

          GraphNode::GraphNodeList tmpBarrierList;
          tmpBarrierList.assign( barrierList.begin( ), barrierList.end( ) );

          /* no wait states for single-stream barriers */
          if ( numLeaveNodesInList == 1 )
          {
            // ignore this non-blocking barrier for blame distribution
            tmpBarrierList.front( )->setCounter( OMP_BARRIER_IGNORE, 1 );
            tmpBarrierList.back( )->setCounter( OMP_BARRIER_IGNORE, 1 );

            analysis->clearBarrierEventList( true, NULL, matchingId );
            return false;
          }

          /* remove all leave nodes (which are at end of the list) */
          for ( size_t i = 0; i < numLeaveNodesInList; ++i )
          {
            tmpBarrierList.pop_back( );
          }
          GraphNode::GraphNodeList::const_iterator iter = tmpBarrierList.begin( );
          /* keep enter event with max enter timestamp */
          GraphNode* maxEnterTimeNode = *iter;
          uint64_t   blame = 0;

          /* find last barrierEnter */
          for (; iter != tmpBarrierList.end( ); ++iter )
          {
            if ( ( *iter )->getTime( ) > maxEnterTimeNode->getTime( ) )
            {
              maxEnterTimeNode = *iter;
            }
            /* accumulate blame, set edges from latest enter to all
             * other leaves */
          }
          for ( iter = tmpBarrierList.begin( );
                iter != tmpBarrierList.end( );
                ++iter )
          {
            GraphNode::GraphNodePair& barrier = ( *iter )->getGraphPair( );
            if ( barrier.first != maxEnterTimeNode )
            {
              Edge* barrierEdge = commonAnalysis->getEdge( barrier.first,
                                                           barrier.second );
              /* make this barrier a blocking waitstate */
              barrierEdge->makeBlocking( );
              barrier.first->setCounter( WAITING_TIME,
                                         maxEnterTimeNode->getTime( ) -
                                         barrier.first->getTime( ) );

              /* create edge from latest enter to other leaves */
              commonAnalysis->newEdge( maxEnterTimeNode,
                                       barrier.second,
                                       EDGE_CAUSES_WAITSTATE );

              blame += maxEnterTimeNode->getTime( ) - barrier.first->getTime( );
            }
          }

          distributeBlame( commonAnalysis,
                           maxEnterTimeNode,
                           blame,
                           streamWalkCallback );

          /* clear list of buffered barriers */
          analysis->clearBarrierEventList( true, NULL, matchingId );

          return true;
        }
      }

  };

 }
}
