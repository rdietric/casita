/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016
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

      /**
       * Handles the case where device kernels already finished before the 
       * synchronization starts. 
       * 
       * Uses the pendingKernels list. 
       * 
       * @param priority
       */
      LateSyncRule( int priority ) :
        ICUDARule( "LateSyncRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {
        if ( !syncLeave->isCUDASync() || !syncLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();

        bool ruleResult = false;
        
        // find all referenced (device) streams
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );

        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin();
              pIter != deviceStreams.end();
              ++pIter )
        {
          EventStream* deviceProcess = *pIter;

          // check for the stream we are looking for
          GraphNode* syncEnter = syncLeave->getGraphPair().first;
          if ( !syncEnter->referencesStream( deviceProcess->getId() ) )
          {
            continue;
          }
          
          // found the stream we are looking for

          // test that there is a pending kernel leave, otherwise stop here
          GraphNode* kernelLeave = deviceProcess->getFirstPendingKernel();
          if( !kernelLeave )
          {
            break;
          }
          
          // if kernel ends before synchronization starts
          if ( kernelLeave->getTime() <= syncEnter->getTime() )
          {
            GraphNode* lastLeaveNode = commonAnalysis->getLastLeaveNode(
              syncLeave->getTime(), deviceProcess->getId() );
            GraphNode* waitEnter = NULL, * waitLeave = NULL;

            if ( lastLeaveNode && lastLeaveNode->isWaitstate() )
            {
              if ( lastLeaveNode->getTime() == syncLeave->getTime() )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                FunctionDescriptor functionDesc;
                functionDesc.paradigm = PARADIGM_CUDA;
                functionDesc.functionType = CUDA_WAITSTATE;
                functionDesc.recordType = RECORD_ENTER; 
              
                waitEnter = commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime(), syncEnter->getTime() ),
                  deviceProcess, NAME_WAITSTATE,
                  &functionDesc );
              }

            }
            else
            {
              FunctionDescriptor functionDesc;
              functionDesc.paradigm = PARADIGM_CUDA;
              functionDesc.functionType = CUDA_WAITSTATE;
              functionDesc.recordType = RECORD_ENTER; 
              
              waitEnter = commonAnalysis->addNewGraphNode(
                syncEnter->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                &functionDesc );
            }

            if ( !waitLeave )
            {
              FunctionDescriptor functionDesc;
              functionDesc.paradigm = PARADIGM_CUDA;
              functionDesc.functionType = CUDA_WAITSTATE;
              functionDesc.recordType = RECORD_LEAVE; 
              
              waitLeave = commonAnalysis->addNewGraphNode(
                syncLeave->getTime(),
                deviceProcess, NAME_WAITSTATE,
                &functionDesc );
            }

            if( waitEnter )
            {
              commonAnalysis->newEdge( syncEnter, waitEnter,
                                       EDGE_CAUSES_WAITSTATE );
            }
              
            commonAnalysis->newEdge( syncLeave, waitLeave );

            // set counters
            syncLeave->incCounter( BLAME,
                                     syncLeave->getTime() -
                                     syncEnter->getTime() );
            //waitLeave->getPartner
            waitLeave->setCounter( WAITING_TIME,
                                   syncLeave->getTime() -
                                   syncEnter->getTime() );

            deviceProcess->clearPendingKernels();            
            ruleResult = true;
          }
        }

        return ruleResult;
      }
  };
 }
}
