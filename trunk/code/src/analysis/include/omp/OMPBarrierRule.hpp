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
      apply( AnalysisParadigmOMP* analysis, GraphNode* node )
      {
        if ( !node->isOMPSync( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode*      enterEvent     = node->getPartner( );
        EventStream*    nodeStream     = commonAnalysis->getStream(
          node->getStreamId( ) );

        if ( nodeStream->isDeviceStream( ) )
        {
          return false;
        }

        /* save barrier enter events to BarrierEventList */
        analysis->addBarrierEventToList( enterEvent, false );

        const EventStreamGroup::EventStreamList& streams =
          commonAnalysis->getHostStreams( );
        const GraphNode::GraphNodeList& barrierList      =
          analysis->getBarrierEventList( false );

        GraphNode::GraphNodeList::const_iterator iter    = barrierList.begin( );
        GraphNode* maxEnterTimeNode = *iter;                               /* keep enter
                                                      * event with max
                                                      * enter
                                                      * timestamp */

        uint64_t   blame = 0;

        /* check if all barriers were passed */
        if ( streams.size( ) == barrierList.size( ) )
        {
          uint32_t ctrIdWaitState = commonAnalysis->getCtrTable( ).getCtrId(
            CTR_WAITSTATE );

          /* find last barrierEnter */
          for (; iter != barrierList.end( ); ++iter )
          {
            if ( ( *iter )->getTime( ) > maxEnterTimeNode->getTime( ) )
            {
              maxEnterTimeNode = *iter;
            }
          }

          /* accumulate blame, set edges from latest enter to all
           * other leaves */
          for ( iter = barrierList.begin( ); iter != barrierList.end( ); ++iter )
          {
            GraphNode::GraphNodePair& barrier = ( *iter )->getGraphPair( );
            if ( barrier.first != maxEnterTimeNode )
            {
              Edge* barrierEdge = commonAnalysis->getEdge( barrier.first,
                                                           barrier.second );
              UTILS_ASSERT( barrierEdge, "no edge %s, %s",
                            barrier.first->getUniqueName( ).c_str( ),
                            barrier.second->getUniqueName( ).c_str( ) );

              /* make this barrier a blocking waitstate */
              barrierEdge->makeBlocking( );
              barrier.second->setCounter( ctrIdWaitState,
                                          maxEnterTimeNode->getTime( ) -
                                          barrier.first->getTime( ) );

              /* create edge from latest enter to other leaves */
              commonAnalysis->newEdge( maxEnterTimeNode,
                                       barrier.second,
                                       EDGE_CAUSES_WAITSTATE );

              blame += maxEnterTimeNode->getTime( ) - barrier.first->getTime( );
            }
          }

          /* set blame */
          distributeBlame( commonAnalysis,
                           maxEnterTimeNode,
                           blame,
                           streamWalkCallback );

          /* clear list of buffered barriers */
          analysis->clearBarrierEventList( false );

          return true;
        }

        return false;

      }

  };

 }
}
