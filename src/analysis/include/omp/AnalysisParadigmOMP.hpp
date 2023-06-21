/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014, 2016, 2017
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

using namespace std;
using namespace casita::io;

namespace casita
{
  namespace omp
  {
    class AnalysisParadigmOMP :
      public IAnalysisParadigm
    {
      public:
        typedef stack< GraphNode* > OmpNodeStack;
        typedef map< uint64_t, OmpNodeStack > pendingOMPKernelStackMap;
        typedef map< uint64_t, GraphNode* > IdNodeMap;

        typedef stack< pair< GraphNode*, uint64_t > > BarrierCountStack;
        typedef map< uint64_t, BarrierCountStack > StreamParallelBarrierMap;

        typedef vector< GraphNode* > GraphNodeVec;
        typedef map< uint64_t, GraphNodeVec > IdNodeVecMap;
        typedef vector< GraphNodeVec > VecNodeVec;
        typedef map< GraphNode*, VecNodeVec > BarrierNodesMap;

        typedef map< uint64_t, pair< IdNodeMap, vector< uint64_t > > >
            OmpStreamRegionsMap;

        AnalysisParadigmOMP( AnalysisEngine* analysisEngine );

        virtual
        ~AnalysisParadigmOMP( );

        Paradigm
        getParadigm( );

        void
        handlePostEnter( GraphNode* node );

        void
        handlePostLeave( GraphNode* node );

        void
        handleKeyValuesEnter( OTF2TraceReader* reader,
            GraphNode*                         node,
            OTF2KeyValueList*                  list );

        void
        handleKeyValuesLeave( OTF2TraceReader* reader,
            GraphNode*                         node,
            GraphNode*                         oldNode,
            OTF2KeyValueList*                  list );

        size_t
        getNestingLevel( );

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
        setTargetEnter( GraphNode* node );

        GraphNode*
        getTargetEnter( int targetDeviceId );

        bool
        isFirstTargetOffloadEvent( uint64_t streamId );

        GraphNode*
        consumeOmpTargetBegin( uint64_t streamId );

        void
        setTargetOffloadFirstEvent( GraphNode* node );

        GraphNode*
        consumTargetOffloadFirstEvent( uint64_t streamId );

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

        /* <! nesting level while reading the trace */
        size_t    nestingLevel;

        /* // OMPT related //// */

        /* <! key: parallel region ID, value: parallel enter node */
        IdNodeMap ompParallelIdNodeMap;

        /* <! key: stream ID, value: stack of barrier, number pair */
        StreamParallelBarrierMap ompParallelBarriersMap;

        /* <! key: parallel enter node, value: vector of barriers with vector of corresponding barrier nodes */
        BarrierNodesMap ompBarrierNodesMap;

        /* this is due to measurement artefacts */
        /* <! vector of open nodes where parallel region was not opened */
        IdNodeVecMap    ompOpenWorkerNodesMap;

        /* ///////////////////// */

        /* // OMPT target related //// */

        /* <! key: parallel region ID, value: parallel enter node */
        IdNodeMap ompDeviceParallelIdNodeMap;

        /* /////////////////////////// */

        /* <! log the OpenMP enter events, needed to resolve nested function calls */
        pendingOMPKernelStackMap ompBackTraceStackMap;

        /* <! remember last OpenMP event per stream to resolve nested function calls */
        IdNodeMap                lastOmpEventMap;

        /* <! Stack of open parallel regions (fork-join regions) */
        OmpNodeStack             forkJoinStack;

        /* <! keep track of omp kernels between forkjoins */
        IdNodeMap                ompComputeTrackMap;

        /* <! collect barriers from different streams */
        GraphNode::GraphNodeList ompBarrierListHost;
        IdPairNodeListMap        ompBarrierListDevice;

        /* <! keep track of last OMP Target Begin on each event stream */
        IdNodeMap                ompTargetRegionBeginMap;

        /* <! keep track of the first event on each device stream after an OpenMP target begin */
        IdNodeMap                ompTargetDeviceFirstEventMap;

        /* <! keep track of the last event on each device stream before an OpenMP target end */
        IdNodeMap                ompTargetDeviceLastEventMap;

        OmpStreamRegionsMap      ompTargetStreamRegionsMap;

        void
        omptParallelRule( GraphNode* ompLeave );

        void
        omptBarrierRule( GraphNode* syncLeave );

    };
  }
}
