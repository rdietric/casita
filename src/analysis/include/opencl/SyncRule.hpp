/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017
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
  class SyncRule :
    public IOpenCLRule
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
       * \todo: handle default stream
       * 
       * Blames late synchronization operations.
       * 
       * @param priority
       */
      SyncRule( int priority ) :
        IOpenCLRule( "SyncRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOpenCL* oclAnalysis, GraphNode* syncLeave )
      {
        // applied at sync
        if ( !syncLeave->isOpenCLQueueSync() || !syncLeave->isLeave() )
        {
          return false;
        }
        
        AnalysisEngine* analysis = oclAnalysis->getCommon();

        GraphNode* syncEnter = syncLeave->getGraphPair().first;
        
        // get referenced device stream
        uint64_t refStreamId = syncEnter->getReferencedStreamId();
        if( refStreamId == 0 )
        {
          UTILS_MSG( true, "Sync %s does not reference a device stream!", 
                     analysis->getNodeInfo( syncLeave ).c_str() );
          return false;
        }

        EventStream* deviceStream = analysis->getStream( refStreamId );

        // if no stream has a pending kernel for early synchronization, 
        // it is a late synchronization that can be blamed for being useless
        bool isLateSync = true; 
        
        // test that there is a pending kernel (leave)
        bool isLastKernel = true;
        while ( true )
        {
          // check if there is a pending kernel left, start from the latest pending kernel
          GraphNode* kernelLeave = deviceStream->getLastPendingKernel();
          if ( !kernelLeave )
          {
            break;
          }

          // Early sync: sync start time < kernel end time
          if ( syncEnter->getTime() < kernelLeave->getTime() )
          {
            //UTILS_OUT( "Found early sync %s on kernel %s", 
            //           analysis->getNodeInfo(syncLeave).c_str(),
            //           analysis->getNodeInfo(kernelLeave).c_str() );

            // add an edge between the last pending kernel and the sync operation
            if ( isLastKernel )
            {
              analysis->newEdge( kernelLeave, syncLeave );
              isLastKernel = false;
            }

            // make edge between sync enter and leave blocking (early sync)
            Edge* syncEdge = analysis->getEdge( syncEnter, syncLeave );
            
            // check for blocking edge to add statistics only once
            if( syncEdge )
            {
              if( !syncEdge->isBlocking() )
              {
                syncEdge->makeBlocking();
              
                // early blocking wait statistics
                analysis->getStatistics().addStatWithCount( 
                  OFLD_STAT_EARLY_BLOCKING_WAIT, 
                  syncLeave->getTime() - syncEnter->getTime() );
              }
            }
            else
            {
              analysis->newEdge( syncEnter, syncLeave, EDGE_IS_BLOCKING );
              
              // early blocking wait statistics
              analysis->getStatistics().addStatWithCount( 
                OFLD_STAT_EARLY_BLOCKING_WAIT, 
                syncLeave->getTime() - syncEnter->getTime() );
            }
            
            // determine waiting time
            GraphNode* kernelEnter = kernelLeave->getGraphPair().first;
            uint64_t waitingTime = std::min( syncLeave->getTime(),
                                             kernelLeave->getTime() ) -
                                   std::max( syncEnter->getTime(),
                                             kernelEnter->getTime() );
            
            // time statistics
            analysis->getStatistics().addStatValue( 
              OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL, waitingTime );
            
            // set counters
            syncLeave->incCounter( WAITING_TIME, waitingTime );
            kernelLeave->incCounter( BLAME, waitingTime );

            // set link to sync leave node (mark kernel as synchronized)
            kernelLeave->setLink( syncLeave );

            isLateSync = false;

            //\todo: distribute blame over all direct predecessors
          }
          else
          {
            // we can stop here, as all earlier kernels will not pass the 
            // early sync condition
            break;
          }

          // consume the last pending kernel to reverse iterate over the list
          // of pending kernels
          deviceStream->consumeLastPendingKernel();
        }
        
        // if this is a late (useless) synchronization
        if( isLateSync )
        {
          //UTILS_MSG( true, "[%"PRIu32"] Found late synchronized kernel at %s",
          //                 analysis->getMPIRank(),
          //                analysis->getNodeInfo(syncLeave).c_str() );

          // set link to sync leave node (mark kernel as synchronized)
          deviceStream->setPendingKernelsSyncLink( syncLeave );
          deviceStream->clearPendingKernels();

          // set counters
          uint64_t waitingTime = syncLeave->getTime() - syncEnter->getTime();

          syncLeave->incCounter( BLAME, waitingTime );
          syncLeave->setCounter( WAITING_TIME, waitingTime );
        }

        return true;
      }
  };
 }
}
