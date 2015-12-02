/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <stack>
#include <map>
#include <vector>

#include "IAnalysisParadigm.hpp"

using namespace casita::io;

namespace casita
{
 namespace cuda
 {
  class AnalysisParadigmCUDA :
    public IAnalysisParadigm
  {
    public:

      typedef struct
      {
        EventNode*           node;
        std::set< uint32_t > tags;
      } StreamWaitTagged;

      typedef std::list< StreamWaitTagged* > NullStreamWaitList;

      AnalysisParadigmCUDA( AnalysisEngine* analysisEngine );

      virtual
      ~AnalysisParadigmCUDA( );

      Paradigm
      getParadigm( );

      void
      handlePostEnter( GraphNode* node );

      void
      handlePostLeave( GraphNode* node );

      void
      handleKeyValuesEnter( ITraceReader*     reader,
                            GraphNode*        node,
                            OTF2KeyValueList* list );

      void
      handleKeyValuesLeave( ITraceReader*     reader,
                            GraphNode*        node,
                            GraphNode*        oldNode,
                            OTF2KeyValueList* list );

      void
      setLastEventLaunch( EventNode* eventLaunchLeave );

      EventNode*
      consumeLastEventLaunchLeave( uint64_t eventId );

      EventNode*
      getEventRecordLeave( uint64_t eventId ) const;

      void
      setEventProcessId( uint64_t eventId, uint64_t streamId );

      uint64_t
      getEventProcessId( uint64_t eventId ) const;

      void
      addPendingKernelLaunch( GraphNode* launch );

      GraphNode*
      consumePendingKernelLaunch( uint64_t kernelStreamId );

      void
      addStreamWaitEvent( uint64_t deviceProcId, EventNode* streamWaitLeave );

      EventNode*
      getFirstStreamWaitEvent( uint64_t deviceStreamId );

      EventNode*
      consumeFirstStreamWaitEvent( uint64_t deviceStreamId );

      void
      linkEventQuery( EventNode* eventQueryLeave );

      void
      removeEventQuery( uint64_t eventId );

      GraphNode*
      getLastLaunchLeave( uint64_t timestamp, uint64_t deviceStreamId ) const;

    private:
      /* maps event ID to (cuEventRecord) leave node */
      IdEventNodeMap     eventLaunchMap;

      /* maps event ID to (cuEventQuery) leave node */
      IdEventNodeMap     eventQueryMap;

      /* maps (device) stream ID to list of (cuStreamWaitEvent)
       * leave nodes */
      IdEventsListMap    streamWaitMap;

      /* maps event ID to (device) stream ID */
      IdIdMap eventProcessMap;
      NullStreamWaitList nullStreamWaits;
      /* Contains a list of nodes for every stream */
      IdNodeListMap      pendingKernelLaunchMap;
  };

 }
}
