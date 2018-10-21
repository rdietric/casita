/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "graph/GraphNode.hpp"
#include "BlameDistribution.hpp"

namespace casita
{
  namespace offload
  {

    /**
     * Add the given node to the stream-walk list and add its waiting time to 
     * the stream walk waiting time. Return false when a device synchronization
     * leave, a launch leave or a process start node has been found. 
     * 
     * @param userData pointer to StreamWalkInfo
     * @param node the node under investigation
     * 
     * @return false when a synchronization leave, a launch leave or a process 
     *         start node has been found, otherwise true.
     */
    static bool
    streamWalkCallback( void* userData, GraphNode* node )
    {
      StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*)userData;
      listAndWaitTime->list.push_back( node );

      // return false, if we found a process start node or a sync device leave node
      if ( node->isProcess() || ( node->isLeave() && ( node->isOffloadWait() || 
                                                       node->isOffloadEnqueueKernel() ) ) )
      {
        return false;
      }
      
      //UTILS_OUT( "Walk node %s", node->getUniqueName().c_str() );

      //listAndWaitTime->waitStateTime += node->getCounter( WAITING_TIME, NULL );

      return true;
    }
  }
}
