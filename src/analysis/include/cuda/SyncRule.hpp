/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016,
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
  class SyncRule :
    public ICUDARule
  {
    public:

      /**
       * Handles both, early and late synchronization of device activities.
       * 
       * Blames kernels for causing wait states, which is basically an early
       * blocking synchronization.
       * This rule uses the pending kernel list and needs access to kernel
       * enter and leave nodes.
       * 
       * Blames late synchronization operations.
       * 
       * @param priority
       */
      SyncRule( int priority ) :
        ICUDARule( "SyncRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {
        // applied at sync
        if ( !syncLeave->isCUDASync() || !syncLeave->isLeave() )
        {
          return false;
        }
        
        AnalysisEngine* commonAnalysis = analysis->getCommon();

        GraphNode* syncEnter = syncLeave->getGraphPair().first;
        
        // get referenced device stream
        uint64_t refStreamId = syncEnter->getReferencedStreamId();
        if( refStreamId == 0 )
        {
          return false;
        }
        EventStream* deviceStream = commonAnalysis->getStream( refStreamId );

        // test that there is a pending kernel (leave)
        bool isLastKernel = true;
        while ( true )
        {
          // check if we have a pending kernel, break otherwise
          GraphNode* kernelLeave = deviceStream->getLastPendingKernel();
          if ( !kernelLeave )
          {
            break;
          }

          // Early sync: sync start time < kernel end time
          if ( syncEnter->getTime() < kernelLeave->getTime() )
          {
            // add an edge between the last pending kernel and the sync operation
            if ( isLastKernel )
            {
              commonAnalysis->newEdge( kernelLeave, syncLeave,
                                       EDGE_CAUSES_WAITSTATE );
              isLastKernel = false;
            }

            // make edge between sync enter and leave blocking (early sync)
            Edge* syncEdge = commonAnalysis->getEdge( syncEnter, syncLeave );
            if( syncEdge )
            {
              syncEdge->makeBlocking();
            }

            GraphNode* kernelEnter = kernelLeave->getGraphPair().first;
            
            // set counters
            uint64_t waitingTime = std::min( syncLeave->getTime(),
                                             kernelLeave->getTime() ) -
                                   std::max( syncEnter->getTime(),
                                             kernelEnter->getTime() );
            
            syncLeave->incCounter( WAITING_TIME, waitingTime );
            kernelLeave->incCounter( BLAME, waitingTime );

            // set link to sync leave node (mark kernel as synchronized)
            kernelLeave->setLink( syncLeave );
            
            deviceStream->consumeLastPendingKernel();
          }
          else // late sync: all pending kernels are synchronized
          {
            // set link to sync leave node (mark kernel as synchronized)
            deviceStream->setPendingKernelsSyncLink( syncLeave );
            deviceStream->clearPendingKernels();

            // set counters
            uint64_t waitingTime = syncLeave->getTime() - syncEnter->getTime();
              
            syncLeave->incCounter( BLAME, waitingTime );
            syncLeave->setCounter( WAITING_TIME, waitingTime );

            break;
          }
        }

        return true;
      }
  };
 }
}
