/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014,2016
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
        // ignore non-OpenMP-compute nodes
        if ( !node->isOMPParallel( ) )
        {
          return false;
        }

        // if this is the first compute node (no compute region is set)
        if ( analysis->getOmpCompute( node->getStreamId( ) ) == NULL )
        {
          GraphNode* pForkJoin = analysis->getInnerMostFork();

          // create dependency edge to the innermost fork node
          // if fork and node are on the same stream the dependency is implicit
          if ( pForkJoin && ( pForkJoin->getStreamId() != node->getStreamId() ) )
          {
            GraphNode* kernelEnter = node->getGraphPair().first;

            // create edge from pending fork to OpenMP compute enter node
            analysis->getCommon()->newEdge( pForkJoin, kernelEnter );
            /*UTILS_MSG(analysis->getCommon()->getMPIRank() == 0 &&
                      node->getTime() < 781313516,
                      "Added OpenMP dependency: %s -> %s",
                      analysis->getCommon()->getNodeInfo( pForkJoin ).c_str(),
                      analysis->getCommon()->getNodeInfo( node ).c_str() );*/
          }
        }
        
        // set node as active OpenMP compute region
        analysis->setOmpCompute( node, node->getStreamId( ) );

        return true;
      }
  };
 }

}
