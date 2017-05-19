/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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

#define CASITA_MPI_REPLAY_TAG 17
#define CASITA_MPI_REVERS_REPLAY_TAG 10000

namespace casita
{
  namespace mpi
  {

    /**
     * Add the given node to the stream-walk list and add its waiting time to 
     * the stream walk waiting time. Return false when a blocking MPI node or 
     * a process start node has been found. However, the blocking MPI node is 
     * included in the list.
     * 
     * @param userData pointer to StreamWalkInfo
     * @param node the node under investigation
     * 
     * @return false when a blocking MPI node or a process start node has been 
     *         found, otherwise true.
     */
    static bool
    streamWalkCallback( void* userData, GraphNode* node )
    {
      StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*)userData;
      listAndWaitTime->list.push_back( node );

      // return false, if we found a process start node or a blocking MPI leave node
      if ( node->isProcess() || ( node->isMPIBlocking() && node->isLeave() ) )
      {
        return false;
      }

      // ignore the waiting time of the last node (blocking MPI leave)
      listAndWaitTime->waitStateTime += node->getCounter( WAITING_TIME, NULL );

      return true;
    }
  }
}
