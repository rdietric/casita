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
          GraphNode* ppr = analysis->getPendingParallelRegion( );

          /* if pending parallel region -> connect kernel to it */
          if ( ( ppr != NULL ) && ( ppr->getStreamId( ) != node->getStreamId( ) ) )
          {
            /* get the complete execution */
            GraphNode::GraphNodePair& kernelPair = node->getGraphPair( );

            /* create Edges */
            analysis->getCommon( )->newEdge( ppr, kernelPair.first );

            /* EventStream* p = analysis->getStream( node->getStreamId( ) );
              ErrorUtils::getInstance( ).outputMessage(
              "[OMPCR] add Edge %s to %s (%s)\n",
              ppr->getUniqueName( ).
              c_str( ), kernelPair.first->getUniqueName( ).c_str( ), p->getName( ) );*/
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
