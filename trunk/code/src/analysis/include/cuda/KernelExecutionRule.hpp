/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
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
  class KernelExecutionRule :
    public ICUDARule
  {
    public:

      KernelExecutionRule( int priority ) :
        ICUDARule( "KernelExecutionRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* kernelLeave )
      {

        // applied at kernel leave
        if ( !kernelLeave->isCUDAKernel( ) || !kernelLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis  = analysis->getCommon( );

        GraphNode* kernelEnter     = kernelLeave->getGraphPair( ).first;
        uint64_t   kernelProcessId = kernelLeave->getStreamId( );

        // find the stream which launched this kernel and consume the launch event
        GraphNode* launchEnterEvent = 
                analysis->consumeFirstPendingKernelLaunchEnter( kernelProcessId );

        if ( !launchEnterEvent )
        {
          ErrorUtils::getInstance( ).throwError( "Found kernel %s without matching kernel launch",
                             kernelLeave->getUniqueName( ).c_str( ) );
          
          return false;
        }

        // link the kernel launch enter and kernel enter nodes between each other
        launchEnterEvent->setLink( kernelEnter );
        kernelEnter->setLink( launchEnterEvent );

        // add pending kernel
        commonAnalysis->getStream( kernelProcessId )->addPendingKernel(
          kernelLeave );

        // add dependency
        commonAnalysis->newEdge( launchEnterEvent, kernelEnter );

        return true;
      }
  };
 }
}
