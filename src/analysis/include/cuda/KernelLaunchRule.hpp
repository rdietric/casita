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

namespace casita
{

 class KernelLaunchRule :
   public AbstractRule
 {
   public:

     KernelLaunchRule( int priority ) :
       AbstractRule( "KernelLaunchRule", priority )
     {

     }

     bool
     apply( AnalysisEngine* analysis, GraphNode* node )
     {

       /* applied at kernel leave */
       if ( !node->isCUDAKernel( ) || !node->isLeave( ) )
       {
         return false;
       }

       /* get the complete execution */
       GraphNode::GraphNodePair kernel = node->getGraphPair( );

       /* find the stream which launched this kernel and consume the
        * launch event */
       uint64_t kernelProcessId = node->getStreamId( );

       GraphNode* launchEnterEvent = analysis->consumePendingKernelLaunch(
         kernelProcessId );

       if ( !launchEnterEvent )
       {
         throw RTException( "Found kernel %s without matching kernel launch",
                            node->getUniqueName( ).c_str( ) );
       }

       launchEnterEvent->setLink( kernel.first );
       kernel.first->setLink( launchEnterEvent );

       /* add pending kernel */
       analysis->getStream( kernelProcessId )->addPendingKernel( kernel.second );

       /* add dependency */
       analysis->newEdge( launchEnterEvent, kernel.first );

       return true;
     }
 };

}
