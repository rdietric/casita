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
#include "OMPRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace casita
{
 namespace omp
 {

  class OMPBarrierRule :
    public AbstractRule
  {
    public:

      OMPBarrierRule( int priority ) :
        AbstractRule( "OMPBarrierRule", priority )
      {

      }

      bool
      apply( AnalysisEngine* analysis, GraphNode* node )
      {
        if ( !node->isOMPSync( ) || !node->isLeave( ) )
        {
          return false;
        }

        GraphNode* enterEvent = node->getPartner( );
        EventStream* nodeStream = analysis->getStream( node->getStreamId( ) );

        if ( nodeStream->isDeviceStream( ) )
        {
          return false;
        }

        /* save barrier enter events to BarrierEventList */
        analysis->addBarrierEventToList( enterEvent, false );

        const EventStreamGroup::EventStreamList& streams =
          analysis->getHostStreams( );
        const GraphNode::GraphNodeList& barrierList =
          analysis->getBarrierEventList( false );

        GraphNode::GraphNodeList::const_iterator iter = barrierList.begin( );
        GraphNode* maxEnterTimeNode = *iter;         /* keep enter
                                                      * event with max
                                                      * enter
                                                      * timestamp */

        uint64_t blame = 0;

        /* check if all barriers were passed */
        if ( streams.size( ) == barrierList.size( ) )
        {
          uint32_t ctrIdWaitState = analysis->getCtrTable( ).getCtrId(
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
              Edge* barrierEdge = analysis->getEdge( barrier.first,
                                                     barrier.second );

              /* make this barrier a blocking waitstate */
              barrierEdge->makeBlocking( );
              barrier.first->setCounter( ctrIdWaitState,
                                         maxEnterTimeNode->getTime( ) -
                                         barrier.first->getTime( ) );

              /* create edge from latest enter to other leaves */
              analysis->newEdge( maxEnterTimeNode,
                                 barrier.second,
                                 EDGE_CAUSES_WAITSTATE );

              blame += maxEnterTimeNode->getTime( ) - barrier.first->getTime( );
            }
          }

          /* set blame */
          distributeBlame( analysis,
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
