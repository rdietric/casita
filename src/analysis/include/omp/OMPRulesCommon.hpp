/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
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
  namespace omp
  {

    /**
     * Add the given node to the (host-)stream-walk list and add its waiting 
     * time to the stream walk waiting time. Return false when a blocking OpenMP
     * leave node has been found or ...
     * 
     * @param userData pointer to StreamWalkInfo
     * @param node the node under investigation
     * 
     * @return false when a blocking OpenMP leave node has been found or ...
     */
    static bool
    streamWalkCallback( void* userData, GraphNode* node )
    {
      StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*) userData;

      // add node to walk list
      listAndWaitTime->list.push_back(node);

      // if there are already nodes in the list AND this node has no caller 
      // (e.g. is not nested) AND node time is smaller then last node in the 
      // list (back walk)
      if( listAndWaitTime->list.size() > 0 && node->getCaller() == NULL &&
          node->getTime() < listAndWaitTime->list.back()->getTime() )
      {
        return false;
      }

      // if blocking OpenMP leave node
      if ( node->isLeave() && node->isOMPSync() )
      {
        // if the barrier is not considered blocking, continue walking
        if ( node->getCounter(OMP_IGNORE_BARRIER, NULL) )
        {
          // add waiting time of current node which is not the list end
          listAndWaitTime->waitStateTime += node->getCounter(WAITING_TIME, NULL);
          
          return true;
        }

        return false;
      }

      // if atomic process start or intermediate node
      if ( node->isProcess() )
      {
        return false;
      }

      // add waiting time of current node which is not the list end
      listAndWaitTime->waitStateTime += node->getCounter(WAITING_TIME, NULL);
      
      return true;
    }

    static bool
    deviceStreamWalkCallback( void* userData, GraphNode* node )
    {
      StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*) userData;

      listAndWaitTime->list.push_back(node);
      listAndWaitTime->waitStateTime += node->getCounter(WAITING_TIME, NULL);

      if ( listAndWaitTime->list.size() > 0 )
      {
        if ( node->getTime() < listAndWaitTime->list.back()->getTime() &&
             node->getCaller() == NULL )
        {
          return false;
        }
      }

      if ( node->isProcess() || (node->isEnter() && node->isOMPTargetOffload()) )
      {
        return false;
      }

      return true;
    }
  }
}
