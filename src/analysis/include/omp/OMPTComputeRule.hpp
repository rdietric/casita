/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016
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

  class OMPTComputeRule :
    public IOMPRule
  {
    public:

      OMPTComputeRule( int priority ) :
        IOMPRule( "OMPTComputeRule", priority )
      {

      }

    private:

      /**
       * This rule generates dependency edges a the end of parallel regions. 
       * For a parallel region an edge is created between the last OpenMP leave 
       * node on a stream and the parallel region leave node on the master.
       * 
       * The begin (fork) of a parallel region is handled in 
       * AnalysisParadigmOMP::handleKeyValuesEnter().
       * 
       * @param analysis the OpenMP analysis
       * @param node the graph node under investigation
       * 
       * @return true, if the rule was successfully applied to a node
       */
      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* ompLeaveNode )
      {
        // if leave node has no caller (e.g. last node on a stream)
        if( ompLeaveNode->isOMP() && !ompLeaveNode->getCaller() && ompLeaveNode->isLeave() )
        {
          // get start of the region
          GraphNode *computeEnter = ompLeaveNode->getGraphPair().first;
          if( !computeEnter )
          {
            return false;
          }
          
          // get fork/parallel begin event
          GraphNode *parallelEnter = ( GraphNode * )computeEnter->getLink();
          if( !parallelEnter )
          {
            return false;
          }
          
          // add dependency edge to parallel leave (join)
          GraphNode *parallelLeave = parallelEnter->getGraphPair().second;
          if( parallelLeave )
          {
            analysis->getCommon()->newEdge( ompLeaveNode, parallelLeave );
          }
          else
          {
            return false;
          }
        }

        return true;
      }
  };
 }

}


