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

 class EventLaunchRule :
   public AbstractRule
 {
   public:

     EventLaunchRule( int priority ) :
       AbstractRule( "EventLaunchRule", priority )
     {

     }

     bool
     apply( AnalysisEngine* analysis, GraphNode* node )
     {
       if ( !node->isCUDAEventLaunch( ) || !node->isLeave( ) )
       {
         return false;
       }

       /* get the complete execution */
       GraphNode::GraphNodePair& evLaunch = node->getGraphPair( );
       EventStream* refProcess = analysis->getStream(
         node->getReferencedStreamId( ) );
       if ( !refProcess )
       {
         RTException(
           "Event launch %s (%f) does not reference any stream (id = %u)",
           node->getUniqueName( ).c_str( ),
           analysis->getRealTime( node->getTime( ) ),
           node->getReferencedStreamId( ) );
       }

       if ( refProcess->isHostStream( ) )
       {
         RTException(
           "Process %s referenced by event launch %s is a host stream",
           refProcess->getName( ), node->getUniqueName( ).c_str( ) );
       }

       analysis->setEventProcessId( ( (EventNode*)evLaunch.second )->getEventId( ),
                                    refProcess->getId( ) );

       GraphNode* kernelLaunchLeave = NULL;

       /* if event is on NULL stream, test if any kernel launch can be
        * found */
       if ( refProcess->isDeviceNullStream( ) )
       {
         /*Allocation::ProcessList deviceProcs;
         analysis->getAllDeviceStreams(deviceProcs);

         for (Allocation::ProcessList::const_iterator iter = deviceProcs.begin();
                 iter != deviceProcs.end(); ++iter)
         {
             kernelLaunchLeave = analysis->getLastLaunchLeave(
                     evLaunch.first->getTime(), (*iter)->getId());

             if (kernelLaunchLeave)
                 break;
         }

         if (kernelLaunchLeave)
         {*/
         analysis->setLastEventLaunch( (EventNode*)( evLaunch.second ) );
         return true;
         /* } */
       }
       else
       {
         /* otherwise, test on its stream only */
         kernelLaunchLeave = analysis->getLastLaunchLeave(
           evLaunch.first->getTime( ), refProcess->getId( ) );

         if ( kernelLaunchLeave )
         {
           evLaunch.second->setLink( (GraphNode*)kernelLaunchLeave );
         }

         analysis->setLastEventLaunch( (EventNode*)( evLaunch.second ) );
         return true;
         /* } */
       }

       return false;
     }
 };

}
