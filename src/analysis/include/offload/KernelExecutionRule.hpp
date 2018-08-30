/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017, 2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "IOffloadRule.hpp"
#include "AnalysisParadigmOffload.hpp"

namespace casita
{
 namespace offload
 {
  class KernelExecutionRule :
    public IOffloadRule
  {
    public:
      /**
       * The kernel execution rule is triggered at kernel leave nodes.
       * It uses the pending kernel launch map and links kernel enter with 
       * respective kernel launch enter.
       * 
       * @param priority
       */
      KernelExecutionRule( int priority ) :
        IOffloadRule( "KernelExecutionRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* kernelLeave )
      {

        // applied at kernel leave
        if ( !kernelLeave->isOffloadKernel() || !kernelLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* analysis = ofldAnalysis->getCommon();
        
        // count occurrence
        analysis->getStatistics().countActivity( STAT_OFLD_KERNEL );

        uint64_t kernelStrmId = kernelLeave->getStreamId();

        // find the stream which launched this kernel and consume the launch event
        // the number of kernel launches and kernel executions has to be the same
        GraphNode* launchEnterEvent = 
                ofldAnalysis->consumeFirstPendingKernelLaunchEnter( kernelStrmId );

        if ( !launchEnterEvent )
        {
          UTILS_OUT( "[%" PRIu32 "] Applying KernelExecutionRule failed. "
                     "Found kernel %s without matching kernel launch.",
                     analysis->getMPIRank(),
                     analysis->getNodeInfo( kernelLeave ).c_str() );
          
          return false;
        }

        GraphNode* kernelEnter = kernelLeave->getGraphPair().first;
        
        // link the kernel launch enter and kernel enter nodes between each other
        launchEnterEvent->setLink( kernelEnter );
        kernelEnter->setLink( launchEnterEvent );
        
        // add pending kernel
        analysis->getStreamGroup().getDeviceStream( kernelStrmId )
          ->addPendingKernel( kernelLeave );
        
        // add dependency
        analysis->newEdge( launchEnterEvent, kernelEnter );
        
        ////////////////////////////////////////////////////////////////////////
        // EXTRA handling for imperfect traces:
        // if the launchEnterEvent is marked with an unsatisfied node
        // add dependency edge, assign blame and waiting time
        if( launchEnterEvent->getData() )
        {
          GraphNode* syncEvtLeave = ( GraphNode* )launchEnterEvent->getData();
          
          // if it is a CUDA event synchronize leave
          if ( syncEvtLeave && syncEvtLeave->isOffloadWaitEvent() && 
               syncEvtLeave->isLeave() )
          {
            GraphNode* syncEvtEnter = syncEvtLeave->getPartner();

            /*UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_TIME, 
                       "[%u] Process deferred node %s at %s",
                       commonAnalysis->getMPIRank(), 
                       syncEvtLeave->getUniqueName().c_str(),
                       kernelLeave->getUniqueName().c_str() );*/

            if ( syncEvtEnter && syncEvtEnter->getTime() < kernelLeave->getTime() )
            {
              // make edge of the synchronization blocking
              Edge* sEdge = analysis->getEdge( syncEvtEnter, syncEvtLeave );
              if( sEdge )
              {
                sEdge->makeBlocking();
              }
              else
              {
                analysis->newEdge( syncEvtEnter, syncEvtLeave, 
                                         EDGE_IS_BLOCKING );
              }

              // compute waiting time
              uint64_t waitingTime = syncEvtLeave->getTime() -
                  std::max( syncEvtEnter->getTime(), kernelEnter->getTime() );
              
              // attribute waiting time to the sync event leave node
              syncEvtLeave->incCounter( WAITING_TIME, waitingTime );
              //kernelLeave->incCounter( BLAME, value );
              
              // blame the kernel
              Edge* kernelEdge = analysis->getEdge( kernelEnter, kernelLeave );
              if( kernelEdge )
              {
                kernelEdge->addBlame( waitingTime );
              }
              else
              {
                UTILS_WARNING( "Could not find kernel edge %s -> %s",
                               analysis->getNodeInfo( kernelEnter ).c_str(),
                               analysis->getNodeInfo( kernelLeave ).c_str() );
              }
            }

            // create edge between kernel end and synchonization end
            analysis->newEdge( kernelLeave, syncEvtLeave );
            
            //commonAnalysis->getStream( kernelStrmId )->consumePendingKernel();
            // clear all pending kernels before that kernel
            analysis->getStreamGroup().getDeviceStream( kernelLeave->getStreamId() )
                                         ->consumePendingKernels( kernelLeave );
          }
        }

        return true;
      }
  };
 }
}
