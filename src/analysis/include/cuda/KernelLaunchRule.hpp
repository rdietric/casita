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

#include "ICUDARule.hpp"
#include "AnalysisParadigmCUDA.hpp"

namespace casita
{
 namespace cuda
 {
  class KernelLaunchRule :
    public ICUDARule
  {
    public:

      KernelLaunchRule( int priority ) :
        ICUDARule( "KernelLaunchRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {

        /* applied at kernel leave */
        if ( !node->isCUDAKernel( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis  = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair kernel = node->getGraphPair( );

        /* find the stream which launched this kernel and consume the
         * launch event */
        uint64_t   kernelProcessId      = node->getStreamId( );

        GraphNode* launchEnterEvent     = analysis->consumePendingKernelLaunch(
          kernelProcessId );

        if ( !launchEnterEvent )
        {
          ErrorUtils::getInstance( ).throwError( "Found kernel %s without matching kernel launch",
                             node->getUniqueName( ).c_str( ) );
          
          return false;
        }

        launchEnterEvent->setLink( kernel.first );
        kernel.first->setLink( launchEnterEvent );

        // add pending kernel
        commonAnalysis->getStream( kernelProcessId )->addPendingKernel(
          kernel.second );

        /* add dependency */
        commonAnalysis->newEdge( launchEnterEvent, kernel.first );

        return true;
      }
  };
 }
}
