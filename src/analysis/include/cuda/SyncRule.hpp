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
       * \todo: handle default stream
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
      apply( AnalysisParadigmCUDA* cudaAnalysis, GraphNode* syncLeave )
      {
        // applied at sync
        if ( !syncLeave->isCUDASync() || !syncLeave->isLeave() )
        {
          return false;
        }
        
        AnalysisEngine* analysis = cudaAnalysis->getCommon();

        GraphNode* syncEnter = syncLeave->getGraphPair().first;
        
        // find all referenced (device) streams of this synchronization
        EventStreamGroup::EventStreamList deviceStreams;
        
        // add all device streams for collective CUDA synchronization, e.g. cudaDeviceSynchronize
        if( syncLeave->isCUDACollSync() )
        {
          analysis->getAllDeviceStreams( deviceStreams );
          
          // create dependencies between all pending kernels (no lower bound)
          cudaAnalysis->createKernelDependencies( NULL );
        }
        else
        {
          // get referenced device stream
          uint64_t refStreamId = syncEnter->getReferencedStreamId();
          if( refStreamId == 0 )
          {
            UTILS_MSG( true, "Sync %s does not reference a device stream!", 
                         analysis->getNodeInfo(syncLeave).c_str() );
            return false;
          }
          
          deviceStreams.push_back( analysis->getStream( refStreamId ) );
          
          GraphNode* kernelLeave = 
            analysis->getStream( refStreamId )->getLastPendingKernel();
          cudaAnalysis->createKernelDependencies( kernelLeave );
        }
        
        // if no stream has a pending kernel for early synchronization, 
        // it is a late synchronization that can be blamed for being useless
        bool isLateSync = true; 
        
        // iterate over streams and try to find an early synchronization
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin();
              pIter != deviceStreams.end(); ++pIter )
        {
          EventStream* deviceStream = *pIter;
        
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
              /*UTILS_MSG( syncLeave->getStreamId() == 0 && syncLeave->getId() == 11348, 
                         "Found early sync %s on kernel %s", 
                         analysis->getNodeInfo(syncLeave).c_str(),
                         analysis->getNodeInfo(kernelLeave).c_str() );*/
              
              // add an edge between the last pending kernel and the sync operation
              if ( isLastKernel )
              {
                analysis->newEdge( kernelLeave, syncLeave );
                isLastKernel = false;
              }

              // make edge between sync enter and leave blocking (early sync)
              Edge* syncEdge = analysis->getEdge( syncEnter, syncLeave );
              if( syncEdge && !syncEdge->isBlocking() )
              {
                syncEdge->makeBlocking();
                
                // early blocking wait statistics
                analysis->getStatistics().addStatWithCount( 
                  OFLD_STAT_EARLY_BLOCKING_WAIT, 
                  syncLeave->getTime() - syncEnter->getTime() );
              }

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
        }
        
        // if this is a late (useless) synchronization
        if( isLateSync )
        {
          //UTILS_MSG( true, "[%"PRIu32"] Found late synchronized kernel at %s",
          //                 analysis->getMPIRank(),
          //                analysis->getNodeInfo(syncLeave).c_str() );
              
          for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin();
              pIter != deviceStreams.end(); ++pIter )
          {
            EventStream* deviceStream = *pIter;
            // set link to sync leave node (mark kernel as synchronized)
            deviceStream->setPendingKernelsSyncLink( syncLeave );
            deviceStream->clearPendingKernels();
          }

          // set counters
          uint64_t waitingTime = syncLeave->getTime() - syncEnter->getTime();

          syncLeave->incCounter( BLAME, waitingTime );
          syncLeave->setCounter( WAITING_TIME, waitingTime );
        }
        
        deviceStreams.clear();

        return true;
      }
  };
 }
}
