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
   
  /*
   * This rule handles the rare case when a CUDA synchronization shall be blamed.
   * This may happen, when the synchronization executes more than a "delta" 
   * longer after the synchronizing kernel finished execution. This can also be 
   * caused by a CUDA communication.
   */
  class BlameSyncRule :
    public ICUDARule
  {
    public:

      BlameSyncRule( int priority ) :
        ICUDARule( "BlameSyncRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {
        if ( !node->isCUDASync( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair& sync = node->getGraphPair( );

        uint64_t syncDeltaTicks = commonAnalysis->getDeltaTicks( );

        bool ruleResult = false;
        
        /* find all referenced (device) streams */
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );

        // for all device streams
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( ); ++pIter )
        {
          EventStream* deviceProcess = *pIter;

          if ( !sync.first->referencesStream( deviceProcess->getId( ) ) )
          {
            continue;
          }

          /* test that there is a pending kernel (leave) */
          GraphNode* kernelLeave = deviceProcess->getPendingKernel( );
          if ( !kernelLeave )
          {
            break;
          }

          GraphNode::GraphNodePair& kernel = kernelLeave->getGraphPair( );

          // Blame the sync if it is noticeably longer executed after the kernels end
          // if (sync starts before kernel ends)
          // AND (sync end time - kernel end time) > sync delta
          if ( ( sync.first->getTime( ) < kernel.second->getTime( ) ) &&
               ( sync.second->getTime( ) - kernel.second->getTime( ) >
                 syncDeltaTicks ) )
          {
            // get the last leave node before the synchronization end
            GraphNode* lastLeaveNode = commonAnalysis->getLastLeaveNode(
              sync.second->getTime( ), deviceProcess->getId( ) );
            
            // leave node of the wait state
            // potentially a leave node before the sync end with the same end time as the sync end
            GraphNode* waitLeave = NULL;

            // if the last leave node is a wait state
            if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
            {
              // if the last leave node has the same time stamp as the sync end 
              if ( lastLeaveNode->getTime( ) == sync.second->getTime( ) )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                // insert a new synthetic graph node for the wait state enter
                commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime( ),
                            kernel.second->getTime( ) ),
                  deviceProcess, NAME_WAITSTATE,
                  PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
              }

            }
            else
            {
              // if last leave node is not a wait state
              // insert a new synthetic graph node for the wait state enter
              commonAnalysis->addNewGraphNode(
                kernel.second->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
            }

            // make sure that we have a wait leave 
            if ( !waitLeave )
            {
              waitLeave = commonAnalysis->addNewGraphNode(
                sync.second->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE );
            }

            // create edge between sync end and wait leave
            commonAnalysis->newEdge( sync.second,
                                     waitLeave,
                                     EDGE_CAUSES_WAITSTATE );

            // set counters
            //\todo: write counters to enter nodes
            sync.second->incCounter( BLAME,
                                     sync.second->getTime( ) -
                                     kernel.second->getTime( ) );
            //waitLeave->getPartner()->
            waitLeave->incCounter( WAITING_TIME,
                                   sync.second->getTime( ) -
                                   kernel.second->getTime( ) );

            deviceProcess->clearPendingKernels( );
            ruleResult = true;
          }
        }

        return ruleResult;
      }
  };
 }
}
