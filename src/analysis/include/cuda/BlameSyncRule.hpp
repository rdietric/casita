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

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"

namespace casita
{

 class BlameSyncRule :
   public AbstractRule
 {
   public:

     BlameSyncRule( int priority ) :
       AbstractRule( "BlameSyncRule", priority )
     {

     }

     bool
     apply( AnalysisEngine* analysis, GraphNode* node )
     {
       if ( !node->isCUDASync( ) || !node->isLeave( ) )
       {
         return false;
       }

       /* get the complete execution */
       GraphNode::GraphNodePair& sync = node->getGraphPair( );

       uint64_t syncDeltaTicks = analysis->getDeltaTicks( );

       bool ruleResult = false;
       /* find all referenced (device) streams */
       EventStreamGroup::EventStreamList deviceStreams;
       analysis->getAllDeviceStreams( deviceStreams );

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

         if ( ( sync.first->getTime( ) < kernel.second->getTime( ) ) &&
              ( sync.second->getTime( ) - kernel.second->getTime( ) >
                syncDeltaTicks ) )
         {
           GraphNode* lastLeaveNode = analysis->getLastLeave(
             sync.second->getTime( ), deviceProcess->getId( ) );
           GraphNode* waitEnter = NULL, * waitLeave = NULL;

           if ( lastLeaveNode && lastLeaveNode->isWaitstate( ) )
           {
             if ( lastLeaveNode->getTime( ) == sync.second->getTime( ) )
             {
               waitLeave = lastLeaveNode;
             }
             else
             {
               waitEnter = analysis->addNewGraphNode(
                 std::max( lastLeaveNode->getTime( ),
                           kernel.second->getTime( ) ),
                 deviceProcess, NAME_WAITSTATE,
                 PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
             }

           }
           else
           {
             waitEnter = analysis->addNewGraphNode(
               kernel.second->getTime( ),
               deviceProcess, NAME_WAITSTATE,
               PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
           }

           if ( !waitLeave )
           {
             waitLeave = analysis->addNewGraphNode(
               sync.second->getTime( ),
               deviceProcess, NAME_WAITSTATE,
               PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE );
           }

           analysis->newEdge( sync.second, waitLeave, EDGE_CAUSES_WAITSTATE );

           /* set counters */
           sync.first->incCounter( analysis->getCtrTable( ).getCtrId( CTR_BLAME ),
                                   sync.second->getTime( ) -
                                   kernel.second->getTime( ) );
           waitEnter->incCounter( analysis->getCtrTable( ).getCtrId(
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
