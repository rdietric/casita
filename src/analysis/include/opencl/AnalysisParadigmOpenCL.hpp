/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016, 2017
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
 namespace opencl
 {
  class AnalysisParadigmOpenCL :
    public IAnalysisParadigm
  {
    public:

      AnalysisParadigmOpenCL( AnalysisEngine* analysisEngine );

      virtual
      ~AnalysisParadigmOpenCL( );

      Paradigm
      getParadigm( );

      void
      handlePostEnter( GraphNode* node );

      void
      handlePostLeave( GraphNode* node );

      void
      handleKeyValuesEnter( OTF2TraceReader*  reader,
                            GraphNode*        node,
                            OTF2KeyValueList* list );

      void
      handleKeyValuesLeave( OTF2TraceReader*  reader,
                            GraphNode*        node,
                            GraphNode*        oldNode,
                            OTF2KeyValueList* list );

      void
      setEventProcessId( uint64_t eventId, uint64_t streamId );

      uint64_t
      getEventProcessId( uint64_t eventId ) const;
      
      bool
      isKernelPending( GraphNode* kernelNode ) const;

      void
      addPendingKernelEnqueue( GraphNode* launch );

      GraphNode*
      consumeFirstPendingKernelEnqueueEnter( uint64_t kernelStreamId );

      GraphNode*
      getLastEnqueueLeave( uint64_t timestamp, uint64_t deviceStreamId ) const;
      
      void
      removeKernelLaunch( GraphNode* kernel );
      
      void
      clearKernelEnqueues( uint64_t streamId );

    private:
      //!< maps event ID to (device) stream ID
      IdIdMap            eventProcessMap;
      
      //!< list of nodes for every (device) stream; 
      // <stream, list of nodes>
      IdNodeListMap      pendingKernelEnqueueMap;
  };

 }
}
