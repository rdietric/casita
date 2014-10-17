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

  class OMPComputeRule :
    public IOMPRule
  {
    public:

      OMPComputeRule( int priority ) :
        IOMPRule( "OMPComputeRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* node )
      {
        if ( !node->isOMPCompute( ) )
        {
          return false;
        }

        /* if no kernel buffered -> save this one */
        if ( analysis->getOmpCompute( node->getStreamId( ) ) == NULL )
        {
          GraphNode* pForkJoin = analysis->getPendingForkJoin( );

          /* if pending forkjoin -> connect kernel to it */
          if ( ( pForkJoin != NULL ) && ( pForkJoin->getStreamId( ) != node->getStreamId( ) ) )
          {
            /* get the complete execution */
            GraphNode::GraphNodePair& kernelPair = node->getGraphPair( );

            /* create Edges */
            analysis->getCommon( )->newEdge( pForkJoin, kernelPair.first );
          }
          analysis->setOmpCompute( node, node->getStreamId( ) );
        }
        else         /* if already kernels buffered -> overwrite */
        {
          analysis->setOmpCompute( node, node->getStreamId( ) );
        }

        return true;

      }

  };
 }

}
