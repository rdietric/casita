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

#include <stack>

#include "GraphNode.hpp"

namespace casita
{

 class EventNode :
   public GraphNode
 {
   public:

     typedef std::stack< EventNode* > EventNodeStack;
     typedef std::list< EventNode* > EventNodeList;

     enum FunctionResultType
     {
       FR_UNKNOWN = 0,
       FR_SUCCESS = 1
     };

     EventNode( uint64_t time, uint64_t streamId, uint64_t eventId,
                FunctionResultType fResult, const std::string name,
                Paradigm paradigm, NodeRecordType recordType, int nodeType ) :
       GraphNode( time, streamId, name, paradigm, recordType, nodeType ),
       eventId( eventId ),
       fResult( fResult )
     {
     }

     uint64_t
     getEventId( ) const
     {
       return eventId;
     }

     FunctionResultType
     getFunctionResult( ) const
     {
       return fResult;
     }

     bool
     isEventNode( ) const
     {
       return true;
     }

   protected:
     uint64_t eventId;
     FunctionResultType fResult;
 };

}
