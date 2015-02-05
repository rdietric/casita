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
 * What this file does:
 * Provide the type "activity".
 * An activity is a region instance with start and end node.
 *
 */

#pragma once

#include "graph/GraphNode.hpp"

namespace casita
{

 class Activity
 {
   public:

     Activity( GraphNode* start, GraphNode* end ) :
       start( start ),
       end( end )
     {

     }

     Activity( GraphNode::GraphNodePair pair ) :
       start( pair.first ),
       end( pair.second )
     {

     }

     uint64_t
     getDuration( ) const
     {
       return end->getTime( ) - start->getTime( );
     }

     const char*
     getName( ) const
     {
       return start->getName( );
     }

     uint64_t
     getProcessId( ) const
     {
       return start->getStreamId( );
     }

     uint64_t
     getFunctionId( ) const
     {
       return start->getFunctionId( );
     }

     GraphNode*
     getStart( ) const
     {
       return start;
     }

     GraphNode*
     getEnd( ) const
     {
       return end;
     }

   private:
     GraphNode* start, * end;
 };
}
