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

      // if this node has no caller (parallel begin should have no caller)
      // (e.g. is not nested) AND node time is smaller then last node in the 
      // list (back walk)
      if( node->getCaller() == NULL && listAndWaitTime->list.size() &&
          node->getTime() < listAndWaitTime->list.back()->getTime() )
      {
        // add interval end node to walk list
        listAndWaitTime->list.push_back(node);
        return false;
      }
      
      // add node to walk list
      listAndWaitTime->list.push_back(node);

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

    /**
     * Walk the device stream backwards and create a list of wait nodes and 
     * wait state time
     * 
     * @param userData stream walk info (walk list and waiting time)
     * @param node current walk node
     * @return 
     */
    static bool
    deviceStreamWalkCallback( void* userData, GraphNode* node )
    {
      StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*) userData;

      listAndWaitTime->list.push_back(node);
      listAndWaitTime->waitStateTime += node->getCounter(WAITING_TIME, NULL);
      
      if( node->getCaller() == NULL && 
          node->getCounter( OMP_FIRST_OFFLOAD_EVT, NULL ) == 1 )
      {
        return false;
      }

      /*if ( listAndWaitTime->list.size() > 0 ) //always true as we pushed the node before
      {
        // if node time is less than
        if ( node->getTime() < listAndWaitTime->list.back()->getTime() &&
             node->getCaller() == NULL )
        {
          return false;
        }
      }

      // if node is start/intermediate node OR node is target enter
      if ( node->isProcess() || ( node->isEnter() && node->isOMPTarget() ) )
      {
        return false;
      }*/

      return true;
    }
  }
}
