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
  class LateSyncRule :
    public ICUDARule
  {
    public:

      LateSyncRule( int priority ) :
        ICUDARule( "LateSyncRule", priority )
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

        bool ruleResult = false;
        /* find all referenced (device) streams */
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );

        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( );
              ++pIter )
        {
          EventStream* deviceProcess = *pIter;

          if ( !sync.first->referencesStream( deviceProcess->getId( ) ) )
          {
            continue;
          }

          /* test that there is a pending kernel leave */
          GraphNode* kernelLeave     = deviceProcess->getPendingKernel( );

          if ( kernelLeave && kernelLeave->getTime( ) <= sync.first->getTime( ) )
          {
            UTILS_DBG_MSG( true, "latesync %s", kernelLeave->getUniqueName( ).c_str( ) );

            GraphNode* lastLeaveNode = commonAnalysis->getLastLeave(
              sync.second->getTime( ), deviceProcess->getId( ) );
            GraphNode* waitEnter     = NULL, * waitLeave = NULL;

            if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
            {
              if ( lastLeaveNode->getTime( ) == sync.second->getTime( ) )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                waitEnter = commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime( ),
                            sync.first->getTime( ) ),
                  deviceProcess, NAME_WAITSTATE,
                  PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
              }

            }
            else
            {
              waitEnter = commonAnalysis->addNewGraphNode(
                sync.first->getTime( ),
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

            commonAnalysis->newEdge( sync.first,
                                     waitEnter,
                                     EDGE_CAUSES_WAITSTATE );
            commonAnalysis->newEdge( sync.second, waitLeave );

            if ( sync.first->isCUDAKernel( ) )
            {
              commonAnalysis->newEdge( kernelLeave, sync.first );
            }

            /* set counters */
            sync.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                      CTR_BLAME ),
                                    sync.second->getTime( ) -
                                    sync.first->getTime( ) );
            waitLeave->setCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                     CTR_WAITSTATE ),
                                   sync.second->getTime( ) -
                                   sync.first->getTime( ) );

            deviceProcess->clearPendingKernels( );
            ruleResult = true;
          }
        }

        return ruleResult;
      }
  };
 }
}
