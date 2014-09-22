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

  class OMPParallelRegionRule :
    public IOMPRule
  {
    public:

      OMPParallelRegionRule( int priority ) :
        IOMPRule( "OMPParallelRegionRule", priority )
      {

      }

    private:
      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* node )
      {
        if ( !node->isOMPParallelRegion( ) )
        {
          return false;
        }

        GraphNode* ppr = analysis->getPendingParallelRegion( );

        if ( ppr == NULL )
        {
          /* open parallel region and add as pending for connecting
           * next upcoming kernel */
          analysis->setPendingParallelRegion( node );
          return true;
        }

        /* check if closing parallel region matches the pending one */
        if ( ppr->getFunctionId( ) != node->getFunctionId( ) )
        {
          ErrorUtils::getInstance( ).outputMessage(
            "[OMPParallelRegionRule] ERROR: "
            "parallel region %s doesn't match open parallel region %s \n",
            node->getUniqueName( ).
            c_str( ), ppr->getUniqueName( ).c_str( ) );
          ErrorUtils::getInstance( ).outputMessage(
            "[OMPParallelRegionRule] close "
            "ParallelRegion %s and reset to %s \nCorrectness not guaranteed",
            ppr->getUniqueName( ).c_str(
                                       ), node->getUniqueName( ).c_str( ) );

          /* close parallel region and reset */
          analysis->setPendingParallelRegion( node );
        }

        /* handle collected omp kernels to add dependency to previous
         * parallel region */
        /* 1) get all OMP-streams */
        const EventStreamGroup::EventStreamList& streams =
          analysis->getCommon( )->getHostStreams( );

        /* 2) iterate over all omp streams and add dependency edge
         * to parallel region leave */
        GraphNode* parallelRegionSecond =
          node->getGraphPair( ).second;

        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                streams.begin( );
              pIter != streams.end( ); ++pIter )
        {
          EventStream* p      = *pIter;
          GraphNode*   kernel = analysis->getOmpCompute( p->getId( ) );
          if ( ( kernel != NULL ) &&
               ( kernel->getStreamId( ) != parallelRegionSecond->getStreamId( ) ) )
          {
            analysis->getCommon( )->newEdge( kernel, parallelRegionSecond );
          }

          analysis->setOmpCompute( NULL, p->getId( ) );
        }

        /* close parallel region and set as null */
        analysis->setPendingParallelRegion( NULL );

        return true;

      }

  };

 }
}
