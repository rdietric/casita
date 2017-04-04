/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016, 2017
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "IOpenCLRule.hpp"
#include "AnalysisParadigmOpenCL.hpp"

namespace casita
{
 namespace opencl
 {
  class KernelExecutionRule :
    public IOpenCLRule
  {
    public:

      KernelExecutionRule( int priority ) :
        IOpenCLRule( "KernelExecutionRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOpenCL* analysis, GraphNode* kernelLeave )
      {

        // applied at kernel leave
        if ( !kernelLeave->isOpenCLKernel() || !kernelLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis  = analysis->getCommon();

        GraphNode* kernelEnter  = kernelLeave->getPartner();
        uint64_t   kernelStrmId = kernelLeave->getStreamId();

        // find the stream which launched this kernel and consume the launch event
        // the number of kernel launches and kernel executions has to be the same
        GraphNode* launchEnterEvent = 
                analysis->consumeFirstPendingKernelEnqueueEnter( kernelStrmId );

        if ( !launchEnterEvent )
        {
          //ErrorUtils::getInstance().throwError( 
          UTILS_MSG( true, 
            "[%u] Applying KernelExecutionRule failed. "
            "Found kernel %s without matching kernel launch.",
            commonAnalysis->getMPIRank(),
            kernelLeave->getUniqueName().c_str() );
          
          return false;
        }

        // link the kernel launch enter and kernel enter nodes between each other
        launchEnterEvent->setLink( kernelEnter );
        kernelEnter->setLink( launchEnterEvent );
        
        // add pending kernel
        commonAnalysis->getStream( kernelStrmId )->addPendingKernel( kernelLeave );
        
        // if the launchEnterEvent is marked with an unsatisfied node
        // add dependency edge, assign blame and waiting time
        if( launchEnterEvent->getData() )
        {
          GraphNode* syncEvtLeave = ( GraphNode* )launchEnterEvent->getData();
          
          // if it is a CUDA event synchronize leave (compare EventSyncRule)
          if ( syncEvtLeave->isOpenCLEventSync() && syncEvtLeave->isLeave() )
          {
            GraphNode* syncEvtEnter = syncEvtLeave->getPartner();

            UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_TIME, 
                       "[%u] Process deferred node %s at %s",
                       commonAnalysis->getMPIRank(), 
                       syncEvtLeave->getUniqueName().c_str(),
                       kernelLeave->getUniqueName().c_str() );

            if ( syncEvtEnter->getTime() < kernelLeave->getTime() )
            {
              // make edge of the synchronization blocking
              Edge* sEdge = commonAnalysis->getEdge( syncEvtEnter, syncEvtLeave );
              if( sEdge )
              {
                sEdge->makeBlocking();
              }
              else
              {
                commonAnalysis->newEdge( syncEvtEnter, syncEvtLeave, 
                                         EDGE_IS_BLOCKING );
              }

              // set counters
              uint64_t value = syncEvtLeave->getTime() -
                  std::max( syncEvtEnter->getTime(), kernelEnter->getTime() );
              syncEvtLeave->incCounter( WAITING_TIME, value );
              kernelLeave->incCounter( BLAME, value );
            }

            commonAnalysis->newEdge( kernelLeave, syncEvtLeave );
            
            //commonAnalysis->getStream( kernelStrmId )->consumePendingKernel();
            // clear all pending kernels before that kernel
            commonAnalysis->getStream( kernelLeave->getStreamId() )
                                         ->consumePendingKernels( kernelLeave );
            
          }
        }

        // add dependency
        commonAnalysis->newEdge( launchEnterEvent, kernelEnter );

        return true;
      }
  };
 }
}
