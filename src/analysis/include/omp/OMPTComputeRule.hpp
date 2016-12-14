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

      bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* node )
      {
        // if leave node has no caller (e.g. last node on a stream)
        if( node->isOMP() && !node->getCaller() && node->isLeave() )
        {
          // get start of the region
          GraphNode *computeEnter = node->getGraphPair().first;
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
            analysis->getCommon()->newEdge( node, parallelLeave );
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


