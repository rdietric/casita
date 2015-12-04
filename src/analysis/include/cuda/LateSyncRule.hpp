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
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {
        if ( !syncLeave->isCUDASync( ) || !syncLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode* syncEnter = syncLeave->getGraphPair( ).first;

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

          if ( !syncEnter->referencesStream( deviceProcess->getId( ) ) )
          {
            continue;
          }

          // test that there is a pending kernel leave
          GraphNode* kernelLeave = deviceProcess->getPendingKernel( );

          if ( kernelLeave && kernelLeave->getTime( ) <= syncEnter->getTime( ) )
          {
            GraphNode* lastLeaveNode = commonAnalysis->getLastLeave(
              syncLeave->getTime( ), deviceProcess->getId( ) );
            GraphNode* waitEnter = NULL, * waitLeave = NULL;

            if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
            {
              if ( lastLeaveNode->getTime( ) == syncLeave->getTime( ) )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                waitEnter = commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime( ),
                            syncEnter->getTime( ) ),
                  deviceProcess, NAME_WAITSTATE,
                  PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
              }

            }
            else
            {
              waitEnter = commonAnalysis->addNewGraphNode(
                syncEnter->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
            }

            if ( !waitLeave )
            {
              waitLeave = commonAnalysis->addNewGraphNode(
                syncLeave->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE );
            }

            if( waitEnter )
            {
              commonAnalysis->newEdge( syncEnter, waitEnter,
                                       EDGE_CAUSES_WAITSTATE );
            }
              
            commonAnalysis->newEdge( syncLeave, waitLeave );

            //\todo: is that possible?
            if ( syncEnter->isCUDAKernel( ) )
            {
              commonAnalysis->newEdge( kernelLeave, syncEnter );
              UTILS_MSG( true, "syncEnter is CUDA kernel!" );
            }

            // set counters
            syncLeave->incCounter( BLAME,
                                     syncLeave->getTime( ) -
                                     syncEnter->getTime( ) );
            //waitLeave->getPartner
            waitLeave->setCounter( WAITING_TIME,
                                   syncLeave->getTime( ) -
                                   syncEnter->getTime( ) );

            deviceProcess->clearPendingKernels( );            
            ruleResult = true;
          }
        }

        return ruleResult;
      }
  };
 }
}
