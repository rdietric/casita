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
        if ( !node->isOMPForkJoinRegion( ) )
        {
          return false;
        }

        GraphNode* ppr = analysis->getPendingForkJoin( );

        if ( ppr == NULL )
        {
          /* open forkjoin and add as pending for connecting next upcoming kernel */
          analysis->setPendingForkJoin( node );
          return true;
        }

        /* check if closing join matches the pending one */
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

          /* close parallel region and reset */
          analysis->setPendingForkJoin( node );
        }

        /* handle collected omp kernels to add dependency to previous forkjoin */
        /* 1) get all OMP-streams */
        const EventStreamGroup::EventStreamList& streams =
          analysis->getCommon( )->getHostStreams( );

        /* 2) iterate over all omp streams and add dependency edge to join */
        GraphNode* join = node->getGraphPair( ).second;

        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                streams.begin( ); pIter != streams.end( ); ++pIter )
        {
          EventStream* p      = *pIter;
          GraphNode*   kernel = analysis->getOmpCompute( p->getId( ) );
          if ( ( kernel != NULL ) && ( kernel->getStreamId( ) != join->getStreamId( ) ) )
          {
            analysis->getCommon( )->newEdge( kernel, join );
          }

          analysis->setOmpCompute( NULL, p->getId( ) );
        }

        /* close forkjoin and set as null */
        analysis->setPendingForkJoin( NULL );

        return true;

      }

  };

 }
}
