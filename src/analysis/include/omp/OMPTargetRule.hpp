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
    static bool
    targetHostWalkCallback( void* userData, GraphNode* node )
    {
      GraphNode::GraphNodeList* list = (GraphNode::GraphNodeList*)userData;

      list->push_front( node );

      if ( node->isEnter( ) && node->isOMPTargetOffload( ) )
      {
        return false;
      }

      return true;
    }

    class OMPTargetRule :
      public IOMPRule
    {
      public:

        OMPTargetRule( int priority ) :
          IOMPRule( "OMPTargetRule", priority )
        {

        }

      private:
        bool
        apply( AnalysisParadigmOMP* analysis, GraphNode* node )
        {
          AnalysisEngine* commonAnalysis = analysis->getCommon( );
          EventStream*    nodeStream     =
              commonAnalysis->getStream( node->getStreamId( ) );

          /* for enter nodes */
          if ( node->isEnter( ) )
          {
            if ( nodeStream->getStreamType( ) == EventStream::ES_DEVICE )
            {
              analysis->setOmpTargetFirstEvent( node );

              return true;
            }

            /* remember the current target begin node */
            if ( node->isOMPTargetOffload( ) )
            {
              analysis->setOmpTargetBegin( node );

              return true;
            }

            return false;
          }

          /* handle leave nodes (NOT enter nodes) */
          if ( nodeStream->getStreamType( ) == EventStream::ES_DEVICE )
          {
            analysis->setOmpTargetLastEvent( node );

            return true;
          }

          /* for the leave node of an OpenMP target region */
          if ( node->isOMPTargetOffload( ) )
          {
            /* consume the target begin value */
            analysis->consumeOmpTargetBegin( node->getStreamId( ) );

            GraphNode* targetBegin    = node->getPartner( );
            uint64_t   refStreamId    = targetBegin->getReferencedStreamId( );

            if ( !refStreamId )
            {
              return false;
            }

            GraphNode* firstEventNode = analysis->consumeOmpTargetFirstEvent(
              refStreamId );
            GraphNode* lastEventNode  = analysis->consumeOmpTargetLastEvent(
              refStreamId );

            if ( !firstEventNode || !lastEventNode )
            {
              ErrorUtils::getInstance( ).outputMessage(
                "Warning: no device events between (%s, %s)",
                targetBegin->getUniqueName( ).c_str( ),
                node->getUniqueName( ).c_str( ) );
              return false;
            }

            /* set host functions as wait state */
            GraphNode::GraphNodeList waitsStateNodesList;
            nodeStream->walkBackward( node,
                targetHostWalkCallback,
                &waitsStateNodesList );

            for ( GraphNode::GraphNodeList::const_iterator iter =
                waitsStateNodesList.begin( );
                iter != waitsStateNodesList.end( ); )
            {
              GraphNode::GraphNodeList::const_iterator current_iter = iter;
              GraphNode::GraphNodeList::const_iterator next_iter    = ++iter;

              if ( next_iter != waitsStateNodesList.end( ) )
              {
                commonAnalysis->getEdge( *current_iter,
                    *next_iter )->makeBlocking( );

                ( *next_iter )->setCounter( WAITING_TIME,
                    ( *next_iter )->getTime( ) -
                    ( *current_iter )->getTime( ) );
              }
            }

            /* add dependency edges */
            commonAnalysis->newEdge( targetBegin,
                firstEventNode,
                EDGE_NONE )->getName( );

            commonAnalysis->newEdge( lastEventNode,
                node,
                EDGE_CAUSES_WAITSTATE )->getName( );

            distributeBlame( commonAnalysis,
                lastEventNode,
                node->getTime( ) - node->getPartner( )->getTime( ),
                deviceStreamWalkCallback );

            return true;
          }
          else
          {
            /* for OpenMP target flush region leave node */
            if ( node->isOMPTargetFlush( ) )
            {
              GraphNode* targetBegin =
                  analysis->consumeOmpTargetBegin( node->getStreamId ) );
              if ( !targetBegin )
              {
                ErrorUtils::getInstance( ).throwError(
                  "[OMPT] Found OMP target flush %s without target begin",
                  node->getUniqueName( ).c_str( ) );
              }

              uint64_t refStreamId   = node->getReferencedStreamId( );
              if ( !refStreamId )
              {
                return false;
              }

              targetBegin->setReferencedStreamId( refStreamId );

              return true;
            }
          }

          return false;

        }

    };
  }

}
