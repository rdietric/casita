/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016, 2017
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
    class OMPTTargetRule :
      public IOMPRule
    {
      public:

        OMPTTargetRule( int priority ) :
          IOMPRule( "OMPTTargetRule", priority )
        {

        }

      private:

        /**
         * Handle OpenMP target offloading.
         *
         * @param ompAnalysis
         * @param node
         * @return
         */
        bool
        apply( AnalysisParadigmOMP* ompAnalysis, GraphNode* node )
        {
          AnalysisEngine* analysis = ompAnalysis->getAnalysisEngine( );

          /* remember enter node */
          if ( node->isEnter( ) )
          {
            EventStream* nodeStream = analysis->getStream( node->getStreamId( ) );

            /* set target enter node on the node's host stream */
            if ( node->isOMPTarget( ) )
            {
              ompAnalysis->setTargetEnter( node );
            }
            /* if it is a device enter node */
            else
            if ( nodeStream->getStreamType( ) == EventStream::ES_DEVICE ) {
              /* if this is the first offload event of a target region */
              if ( ompAnalysis->isFirstTargetOffloadEvent( node->getStreamId( ) ) &&
                  node->getCaller( ) == NULL ) /* if the node has a caller it cannot be the first event */
              {
                ompAnalysis->setTargetOffloadFirstEvent( node );

                /* get corresponding target enter */
                GraphNode* targetEnter =
                    ompAnalysis->getTargetEnter( ( (DeviceStream*)nodeStream )->getDeviceId( ) );

                if ( targetEnter )
                {
                  /* add dependency edge */
                  analysis->newEdge( targetEnter, node, false );
                }
                else
                {
                  UTILS_WARNING( "Could not create edge from target enter to %s ",
                      node->getUniqueName( ).c_str( ) );
                  return false;
                }
              }
            }
          }
          /* start the analysis on the leave node */
          else
          if ( node->isLeave( ) ) {
            /* on target leave nodes */
            if ( node->isOMPTarget( ) )
            {
              /* \todo: this does not work for deprecated libmpti traces, as target */
              /* enter events do not carry the device id */
              /* for libmpti use all device streams */
              GraphNode* targetEnter            = node->getGraphPair( ).first;
              int deviceId                      = -1;
              if ( targetEnter->getData( ) )
              {
                deviceId = targetEnter->getReferencedStreamId( );
              }

              const EventStreamGroup::DeviceStreamList& deviceStreams =
                  analysis->getDeviceStreams( deviceId );

              GraphNode* firstTargetOffloadNode = NULL;
              GraphNode* lastTargetOffloadNode  = NULL;
              /* get last node on the device before target leave */
              for ( EventStreamGroup::DeviceStreamList::const_iterator pIter =
                  deviceStreams.begin( ); pIter != deviceStreams.end( ); ++pIter )
              {
                DeviceStream* deviceStream    = *pIter;

                /* find stream local last offload node */
                GraphNode*    lastOffloadNode =
                    GraphNode::findLastNodeBefore( node->getTime( ), deviceStream->getNodes( ) );

                /* UTILS_OUT( "Last offload node: %s < %lf", */
                /*            analysis->getNodeInfo( lastOffloadNode ).c_str(), */
                /*            analysis->getRealTime( node->getTime() ) ); */

                analysis->newEdge( lastOffloadNode->getGraphPair( ).second, node, false );

                /* set the last offload node for the current target region (over streams) */
                /* use only leave nodes */
                if ( lastTargetOffloadNode == NULL ||
                    Node::compareLess( lastTargetOffloadNode, lastOffloadNode->getGraphPair( ).second ) )
                {
                  lastTargetOffloadNode = lastOffloadNode->getGraphPair( ).second;
                }

                /* get the first offload node of each stream and mark it for walkback abort condition */
                GraphNode* firstOffloadNode =
                    ompAnalysis->consumTargetOffloadFirstEvent( deviceStream->getId( ) );
                firstOffloadNode->setCounter( OMP_FIRST_OFFLOAD_EVT, 1 );

                /* get first target region global offload node */
                if ( firstTargetOffloadNode == NULL ||
                    Node::compareLess( firstOffloadNode, firstTargetOffloadNode ) )
                {
                  firstTargetOffloadNode = firstOffloadNode;
                }
              }

              /* reset target enter node for next target region */
              ompAnalysis->consumeOmpTargetBegin( node->getStreamId( ) );

              if ( lastTargetOffloadNode && firstTargetOffloadNode )
              {
                uint64_t waiting_time = node->getTime( );
                if ( lastTargetOffloadNode->getTime( ) < node->getTime( ) )
                {
                  waiting_time = lastTargetOffloadNode->getTime( );
                }

                if ( targetEnter->getTime( ) < firstTargetOffloadNode->getTime( ) )
                {
                  waiting_time -= firstTargetOffloadNode->getTime( );
                }
                else
                {
                  waiting_time -= targetEnter->getTime( );
                }

                /* add waiting time to offloading statistics */
                analysis->getStatistics( ).addStatWithCount(
                  OFLD_STAT_EARLY_BLOCKING_WAIT, waiting_time );

                node->setCounter( WAITING_TIME, waiting_time );

                /* distribute blame on the device */
                distributeBlame( analysis,
                    lastTargetOffloadNode,
                    waiting_time,            /* total blame */
                    ompDeviceStreamWalkCallback,
                    REASON_OFLD_WAIT4DEVICE );
              }
              else
              {
                UTILS_WARNING( "No device streams? (%llu)", deviceStreams.size( ) );
              }
            }
            /* if it is a device enter node
            else if ( nodeStream->getStreamType( ) == EventStream::ES_DEVICE )
            {

            }*/
          }

          return true;
        }
    };
  }
}
