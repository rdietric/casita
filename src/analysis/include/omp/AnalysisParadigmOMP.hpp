/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014, 2016,
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

#include "AnalysisEngine.hpp"
#include "IAnalysisParadigm.hpp"

using namespace casita::io;

namespace casita
{
 namespace omp
 {
  class AnalysisParadigmOMP :
    public IAnalysisParadigm
  {
    public:
      typedef std::stack< GraphNode* > OmpNodeStack;
      typedef std::map< uint64_t, OmpNodeStack > pendingOMPKernelStackMap;
      typedef std::map< uint64_t, GraphNode* > OmpEventMap;
      typedef std::map< uint64_t, std::pair<
                          std::map< uint64_t, GraphNode* >,
                          std::vector< uint64_t > > >
      OmpStreamRegionsMap;

      AnalysisParadigmOMP( AnalysisEngine* analysisEngine );

      virtual
      ~AnalysisParadigmOMP( );

      Paradigm
      getParadigm( );

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
      
      /**
       * Get the innermost fork-join node (top node on the stack).
       * 
       * @return the innermost fork-join node or NULL if stack is empty.
       */
      GraphNode*
      getInnerMostFork( );

      /**
       * Push fork operation (parallel begin) to the fork-join stack.
       * 
       * @param forkJoinNode fork-join node
       */
      void
      pushFork( GraphNode* forkJoinNode );

      /**
       * Take the innermost fork-join node from stack.
       * 
       * @param forkJoinNode innermost fork-join node
       * 
       * @return the innermost fork-join node or NULL if stack is empty.
       */
      GraphNode*
      popFork( );

      GraphNode*
      getOmpCompute( uint64_t streamId );

      void
      setOmpCompute( GraphNode* node, uint64_t streamId );

      const GraphNode::GraphNodeList&
      getBarrierEventList( bool device, GraphNode* caller = NULL, int matchingId = 0 );

      void
      addBarrierEventToList( GraphNode* node, bool device, int matchingId = 0 );

      void
      clearBarrierEventList( bool device, GraphNode* caller = NULL, int matchingId = 0 );

      void
      setOmpTargetBegin( GraphNode* node );

      GraphNode*
      consumeOmpTargetBegin( uint64_t streamId );

      void
      setOmpTargetFirstEvent( GraphNode* node );

      GraphNode*
      consumeOmpTargetFirstEvent( uint64_t streamId );

      void
      setOmpTargetLastEvent( GraphNode* node );

      GraphNode*
      consumeOmpTargetLastEvent( uint64_t streamId );

      void
      pushOmpTargetRegion( GraphNode* node, uint64_t regionId );

      void
      popOmpTargetRegion( GraphNode* node );

      GraphNode*
      findOmpTargetParentRegion( GraphNode* node, uint64_t parentRegionId );

    private:
      /* log the OMP enter events, needed to resolve
      * nested function calls */
      pendingOMPKernelStackMap ompBackTraceStackMap;

      /* remember last omp event per stream -> needed to resolve
       * nested function calls */
      OmpEventMap lastOmpEventMap;
      
      //<! Stack of open parallel regions (fork-join regions)
      OmpNodeStack forkJoinStack;

      /* keep track of omp kernels between forkjoins */
      OmpEventMap ompComputeTrackMap;

      /* collect barriers from different streams */
      GraphNode::GraphNodeList ompBarrierListHost;
      IdPairNodeListMap ompBarrierListDevice;

      /* keep track of last OMP Target Begin on each event stream */
      OmpEventMap ompTargetRegionBeginMap;

      /* keep track of the first event on each device stream after an
       *OMP target begin */
      OmpEventMap ompTargetDeviceFirstEventMap;

      /* keep track of the last event on each device stream before an
       *OMP target end */
      OmpEventMap ompTargetDeviceLastEventMap;

      OmpStreamRegionsMap ompTargetStreamRegionsMap;
  };
 }
}
