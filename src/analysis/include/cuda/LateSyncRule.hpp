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

 class LateSyncRule :
   public AbstractRule
 {
   public:

     LateSyncRule( int priority ) :
       AbstractRule( "LateSyncRule", priority )
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

       bool ruleResult = false;
       /* find all referenced (device) streams */
       EventStreamGroup::EventStreamList deviceStreams;
       analysis->getAllDeviceStreams( deviceStreams );

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
         GraphNode* kernelLeave = deviceProcess->getPendingKernel( );

         if ( kernelLeave && kernelLeave->getTime( ) <= sync.first->getTime( ) )
         {
           printf( "latesync %s\n", kernelLeave->getUniqueName( ).c_str( ) );
           GraphNode* lastLeaveNode = analysis->getLastLeave(
             sync.second->getTime( ),
             deviceProcess->
             getId( ) );
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
                           sync.first->getTime( ) ),
                 deviceProcess, NAME_WAITSTATE,
                 PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE );
             }

           }
           else
           {
             waitEnter = analysis->addNewGraphNode(
               sync.first->getTime( ),
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

           analysis->newEdge( sync.first, waitEnter, EDGE_CAUSES_WAITSTATE );
           analysis->newEdge( sync.second, waitLeave );

           if ( sync.first->isCUDAKernel( ) )
           {
             analysis->newEdge( kernelLeave, sync.first );
           }

           /* set counters */
           sync.first->incCounter( analysis->getCtrTable( ).getCtrId( CTR_BLAME ),
                                   sync.second->getTime( ) -
                                   sync.first->getTime( ) );
           waitEnter->setCounter( analysis->getCtrTable( ).getCtrId(
                                    CTR_WAITSTATE ),
                                  sync.second->getTime( ) - sync.first->getTime( ) );

           deviceProcess->clearPendingKernels( );
           ruleResult = true;
         }
       }

       return ruleResult;
     }
 };

}
