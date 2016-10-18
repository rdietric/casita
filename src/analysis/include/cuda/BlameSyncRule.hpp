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
   * caused by a CUDA communication. Hence, the rule might be switched off.
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
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {
        if ( !syncLeave->isCUDASync( ) || !syncLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode* syncEnter = syncLeave->getGraphPair( ).first;

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

          if ( !syncEnter->referencesStream( deviceProcess->getId( ) ) )
          {
            continue;
          }

          /* test that there is a pending kernel (leave) */
          GraphNode* kernelLeave = deviceProcess->getFirstPendingKernel( );
          if ( !kernelLeave )
          {
            break;
          }

          // Blame the sync if it is noticeably longer executed after the kernels end
          // if (sync starts before kernel ends)
          // AND (sync end time - kernel end time) > sync delta
          if ( ( syncEnter->getTime( ) < kernelLeave->getTime( ) ) &&
               ( syncLeave->getTime( ) - kernelLeave->getTime( ) >
                 syncDeltaTicks ) )
          {
            // get the last leave node before the synchronization end
            GraphNode* lastLeaveNode = commonAnalysis->getLastLeaveNode(
              syncLeave->getTime( ), deviceProcess->getId( ) );
            
            // leave node of the wait state
            // potentially a leave node before the sync end with the same end time as the sync end
            GraphNode* waitLeave = NULL;

            // if the last leave node is a wait state
            if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
            {
              // if the last leave node has the same time stamp as the sync end 
              if ( lastLeaveNode->getTime( ) == syncLeave->getTime( ) )
              {
                waitLeave = lastLeaveNode;
              }
              else
              {
                // insert a new synthetic graph node for the wait state enter
                FunctionDescriptor functionDesc;
                functionDesc.paradigm = PARADIGM_CUDA;
                functionDesc.functionType = CUDA_WAITSTATE;
                functionDesc.recordType = RECORD_ENTER;
                
                commonAnalysis->addNewGraphNode(
                  std::max( lastLeaveNode->getTime( ),
                            kernelLeave->getTime( ) ),
                  deviceProcess, NAME_WAITSTATE,
                  &functionDesc );
              }

            }
            else
            {
              // if last leave node is not a wait state
              // insert a new synthetic graph node for the wait state enter
              FunctionDescriptor functionDesc;
              functionDesc.paradigm = PARADIGM_CUDA;
              functionDesc.functionType = CUDA_WAITSTATE;
              functionDesc.recordType = RECORD_ENTER;
                
              commonAnalysis->addNewGraphNode(
                kernelLeave->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                &functionDesc );
            }

            // make sure that we have a wait leave 
            if ( !waitLeave )
            {
              FunctionDescriptor functionDesc;
              functionDesc.paradigm = PARADIGM_CUDA;
              functionDesc.functionType = CUDA_WAITSTATE;
              functionDesc.recordType = RECORD_LEAVE;
              
              waitLeave = commonAnalysis->addNewGraphNode(
                syncLeave->getTime( ),
                deviceProcess, NAME_WAITSTATE,
                &functionDesc );
            }

            // create edge between sync end and wait leave
            commonAnalysis->newEdge( syncLeave,
                                     waitLeave,
                                     EDGE_CAUSES_WAITSTATE );

            // set counters
            syncLeave->incCounter( BLAME,
                                   syncLeave->getTime( ) -
                                   kernelLeave->getTime( ) );
            //waitLeave->getPartner()->
            waitLeave->incCounter( WAITING_TIME,
                                   syncLeave->getTime( ) -
                                   kernelLeave->getTime( ) );

            deviceProcess->clearPendingKernels( );
            ruleResult = true;
          }
        }

        return ruleResult;
      }
  };
 }
}
