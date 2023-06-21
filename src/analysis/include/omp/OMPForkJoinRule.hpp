/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016-2018
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

    class OMPForkJoinRule :
      public IOMPRule
    {
      public:

        OMPForkJoinRule( int priority ) :
          IOMPRule( "OMPForkJoinRule", priority )
        {

        }

      private:
        bool
        apply( AnalysisParadigmOMP* analysis, GraphNode* node )
        {
          if ( !node->isOMPForkJoin( ) )
          {
            return false;
          }

          if ( node->isOMPForkJoin( ) && node->isEnter( ) )
          {
            analysis->pushFork( node );

            return true;
          }

          /* ////////////////////////////////////////////////////////////////////// */
          /* this is an OpenMP join node */

          AnalysisEngine* analysisEngine = analysis->getAnalysisEngine( );

          analysisEngine->getStatistics( ).countActivity( STAT_OMP_JOIN );

          GraphNode*      ppr = analysis->getInnerMostFork( );

          UTILS_ASSERT( ppr->getFunctionId( ) == node->getFunctionId( ),
              "[%" PRIu64 "] OpenMP join %s does not match the open fork %s",
              node->getStreamId( ),
              analysisEngine->getNodeInfo( node ).c_str( ),
              analysisEngine->getNodeInfo( ppr ).c_str( ) );

          /* check if closing join matches the open fork (have the same ID)
          if ( ppr->getFunctionId( ) != node->getFunctionId( ) )
          {
            ErrorUtils::getInstance( ).outputMessage(
              "[OMPForkJoinRule] ERROR: "
              "forkjoin %s doesn't match open forkjoin %s \n",
              node->getUniqueName( ).c_str( ), ppr->getUniqueName( ).c_str( ) );
            ErrorUtils::getInstance( ).outputMessage(
              "[OMPForkJoinRule] close "
              "ForkJoin %s and reset to %s \nCorrectness not guaranteed",
              ppr->getUniqueName( ).c_str( ), node->getUniqueName( ).c_str( ) );

            // close parallel region and reset
            analysis->setPendingForkJoin( node );
          }*/

          /* handle collected OpenMP compute nodes to add dependency to previous forkjoin */
          /* \todo: */

          /* iterate over all OpenMP streams and add dependency edge to join */
          const EventStreamGroup::EventStreamList& streams =
              analysisEngine->getHostStreams( );
          for ( EventStreamGroup::EventStreamList::const_iterator pIter =
              streams.begin( ); pIter != streams.end( ); ++pIter )
          {
            EventStream* p      = *pIter;
            GraphNode*   kernel = analysis->getOmpCompute( p->getId( ) );

            /* no edge on the same stream needed */
            if ( ( kernel != NULL ) && ( kernel->getStreamId( ) != node->getStreamId( ) ) )
            {
              analysisEngine->newEdge( kernel, node );
            }

            /* set the last OpenMP compute node on this stream to NULL (joined) */
            analysis->setOmpCompute( NULL, p->getId( ) );
          }

          /* close forkjoin */
          if ( analysis->popFork( ) == NULL )
          {
            UTILS_OUT( "Could not join the fork %s",
                ppr->getUniqueName( ).c_str( ) );
          }

          return true;

        }

    };

  }
}
