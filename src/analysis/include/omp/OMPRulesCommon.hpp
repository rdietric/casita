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

#include "graph/GraphNode.hpp"
#include "BlameDistribution.hpp"

namespace casita
{
 namespace omp
 {

  static bool
  streamWalkCallback( void* userData, GraphNode* node )
  {
    StreamWalkInfo* listAndWaitTime = (StreamWalkInfo*)userData;

    listAndWaitTime->list.push_back( node );
    listAndWaitTime->waitStateTime += node->getCounter( CTR_WAITSTATE, NULL );

    if ( listAndWaitTime->list.size( ) > 0 )
    {
      if ( node->getTime( ) < listAndWaitTime->list.back( )->getTime( ) &&
           node->getCaller( ) == NULL )
      {
        return false;
      }
    }

    if ( node->isProcess( ) || ( node->isLeave( ) && node->isOMPSync( ) ) )
    {
      return false;
    }

    return true;
  }
 }
}
