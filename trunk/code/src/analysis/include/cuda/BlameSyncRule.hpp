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

        uint64_t syncDeltaTicks        = commonAnalysis->getDeltaTicks( );

        bool     ruleResult = false;
        /* find all referenced (device) streams */
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );

        // for all device streams
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( ); ++pIter )
        {
          EventStream* deviceProcess       = *pIter;

          if ( !sync.first->referencesStream( deviceProcess->getId( ) ) )
          {
            continue;
          }

          /* test that there is a pending kernel (leave) */
          GraphNode* kernelLeave           = deviceProcess->getPendingKernel( );
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
            GraphNode* lastLeaveNode = commonAnalysis->getLastLeave(
              sync.second->getTime( ), deviceProcess->getId( ) );
            GraphNode* waitLeave     = NULL;

            if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
            {
              if ( lastLeaveNode->getTime( ) == sync.second->getTime( ) )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime( ),
                            kernel.second->getTime( ) ),
                  deviceProcess, NAME_WAITSTATE,
                  PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
              }

            }
            else
            {
              commonAnalysis->addNewGraphNode(
                kernel.second->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
            }

            if ( !waitLeave )
            {
              waitLeave = commonAnalysis->addNewGraphNode(
                sync.second->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE );
            }

            commonAnalysis->newEdge( sync.second,
                                     waitLeave,
                                     EDGE_CAUSES_WAITSTATE );

            // set counters
            sync.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                       CTR_BLAME ),
                                     sync.second->getTime( ) -
                                     kernel.second->getTime( ) );
            waitLeave->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                     CTR_WAITSTATE ),
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
