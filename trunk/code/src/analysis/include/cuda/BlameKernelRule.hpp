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

 class BlameKernelRule :
   public AbstractRule
 {
   public:

     BlameKernelRule( int priority ) :
       AbstractRule( "BlameKernelRule", priority )
     {

     }

     bool
     apply( AnalysisEngine* analysis, GraphNode* node )
     {
       /* applied at sync */
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
         EventStream* deviceStream = *pIter;

         if ( !sync.first->referencesStream( deviceStream->getId( ) ) )
         {
           continue;
         }

         /* test that there is a pending kernel (leave) */
         bool isFirstKernel = true;
         while ( true )
         {
           GraphNode* kernelLeave = deviceStream->getPendingKernel( );
           if ( !kernelLeave )
           {
             break;
           }

           GraphNode::GraphNodePair& kernel = kernelLeave->getGraphPair( );

           if ( ( isFirstKernel &&
                  ( sync.first->getTime( ) < kernel.second->getTime( ) ) &&
                  ( sync.second->getTime( ) - kernel.second->getTime( ) <=
                    syncDeltaTicks ) ) ||
                ( !isFirstKernel &&
                  ( sync.first->getTime( ) < kernel.second->getTime( ) ) ) )
           {
             if ( isFirstKernel )
             {
               analysis->newEdge( kernel.second,
                                  sync.second,
                                  EDGE_CAUSES_WAITSTATE );
             }

             analysis->getEdge( sync.first, sync.second )->makeBlocking( );

             /* set counters */
             sync.first->incCounter( analysis->getCtrTable( ).getCtrId(
                                       CTR_WAITSTATE ),
                                     std::min( sync.second->getTime( ),
                                               kernel.second->getTime( ) ) -
                                     std::max( sync.first->getTime( ),
                                               kernel.first->getTime( ) ) );
             kernel.first->incCounter( analysis->getCtrTable( ).getCtrId(
                                         CTR_BLAME ),
                                       std::min( sync.second->getTime( ),
                                                 kernel.second->getTime( ) ) -
                                       std::max( sync.first->getTime( ),
                                                 kernel.first->getTime( ) ) );

             ruleResult = true;
             isFirstKernel = false;
             deviceStream->consumePendingKernel( );
           }
           else
           {
             deviceStream->clearPendingKernels( );
             break;
           }
         }
       }

       return ruleResult;
     }
 };

}
