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

#include <map>
#include <vector>
#include <string>
#include "utils/ErrorUtils.hpp"
#include "graph/Graph.hpp"
#include "graph/Node.hpp"
#include "graph/EventNode.hpp"
#include "graph/Edge.hpp"
#include "otf/ITraceWriter.hpp"
#include "Activity.hpp"
#include "EventStream.hpp"
#include "EventStreamGroup.hpp"
#include "CounterTable.hpp"
#include "FunctionTable.hpp"

#define SCOREP_CUPTI_CUDA_STREAMREF_KEY "CUDA_STREAM_REF"
#define SCOREP_CUPTI_CUDA_EVENTREF_KEY "CUDA_EVENT_REF"
#define SCOREP_CUPTI_CUDA_CURESULT_KEY "CUDA_DRV_API_RESULT"

#define SYNC_DELTA 500 /* in us */

namespace casita
{

 class GraphEngine
 {
   private:

     Graph::EdgeList emptyEdgeList;
     typedef struct
     {
       uint64_t startTime;
       uint64_t endTime;
       uint32_t numberOfEvents;
     } EdgeCPUData;

     std::map< uint64_t, EdgeCPUData > cpuDataPerProcess;
     
     GraphNode::GraphNodePair criticalLocalStartEnd;

   public:
     typedef std::map< uint64_t, EventStream* > EventStreamMap;
     typedef std::stack< GraphNode* > GraphNodeStack;
     typedef std::map< uint64_t, GraphNodeStack > GraphNodeStackMap;

     GraphEngine( );
     virtual
     ~GraphEngine( );

     Graph&
     getGraph( );

     Graph*
     getGraph( Paradigm paradigm );

     CounterTable&
     getCtrTable( );

     virtual void
     reset( );

     void
     resetCounters( );

     /* streams */
     EventStream*
     getStream( uint64_t id ) const;

     void
     getStreams( EventStreamGroup::EventStreamList& streams ) const;

     void
     getLocalStreams( EventStreamGroup::EventStreamList& streams ) const;

     void
     getStreams( EventStreamGroup::EventStreamList& streams,
                 Paradigm                           paradigm ) const;

     const EventStreamGroup::EventStreamList&
     getHostStreams( ) const;

     const EventStreamGroup::EventStreamList&
     getDeviceStreams( ) const;

     void
     getAllDeviceStreams( EventStreamGroup::EventStreamList& deviceStreams )
     const;

     /* allocators */
     EventStream*
     newEventStream( uint64_t                     id,
                     uint64_t                     parentId,
                     const std::string            name,
                     EventStream::EventStreamType streamType,
                     /*Paradigm                     paradigm,*/
                     bool                         remoteStream = false );

     Edge*
     newEdge( GraphNode* n1, GraphNode* n2, int properties = EDGE_NONE,
              Paradigm* edgeType = NULL );

     GraphNode*
     newGraphNode( uint64_t          time,
                   uint64_t          streamId,
                   const std::string name,
                   Paradigm          paradigm,
                   NodeRecordType    recordType,
                   int               nodeType );

     EventNode*
     newEventNode( uint64_t                      time,
                   uint64_t                      streamId,
                   uint64_t                      eventId,
                   EventNode::FunctionResultType fResult,
                   const std::string             name,
                   Paradigm                      paradigm,
                   NodeRecordType                recordType,
                   int                           nodeType );

     GraphNode*
     addNewGraphNode( uint64_t       time,
                      EventStream*   stream,
                      const char*    name,
                      Paradigm       paradigm,
                      NodeRecordType recordType,
                      int            nodeType );

     EventNode*
     addNewEventNode( uint64_t                      time,
                      uint64_t                      eventId,
                      EventNode::FunctionResultType fResult,
                      EventStream*                  stream,
                      const char*                   name,
                      Paradigm                      paradigm,
                      NodeRecordType                recordType,
                      int                           nodeType );

     void
     addCPUEvent( uint64_t time, uint64_t stream );

     Edge*
     getEdge( GraphNode* source, GraphNode* target );

     void
     removeEdge( Edge* e );

     /* query timeline objects */
     GraphNode*
     getLastNode( ) const;

     GraphNode*
     getSourceNode( ) const;

     GraphNode*
     getLastGraphNode( ) const;

     GraphNode*
     getFirstTimedGraphNode( Paradigm paradigm ) const;

     GraphNode*
     getLastGraphNode( Paradigm paradigm ) const;
     
     GraphNode::GraphNodePair*
     getLocalCriticalStartEnd( );

     void
     getAllNodes( EventStream::SortedGraphNodeList& allNodes ) const;
     
     void
     createIntermediateBegin( );

     /* timings */
     void
     setTimerResolution( uint64_t ticksPerSecond );

     uint64_t
     getTimerResolution( );

     uint64_t
     getDeltaTicks( );

     void
     runSanityCheck( uint32_t mpiRank );

   protected:
     EventStreamGroup  streamGroup;
     uint64_t ticksPerSecond;

     Graph graph;
     GraphNode* globalSourceNode;
     GraphNodeStackMap pendingGraphNodeStackMap;

     EventStreamMap    streamsMap;

     CounterTable      ctrTable;

     /* query graph objects */
     bool
     hasInEdges( GraphNode* n );

     bool
     hasOutEdges( GraphNode* n );

     const Graph::EdgeList&
     getInEdges( GraphNode* n ) const;

     const Graph::EdgeList&
     getOutEdges( GraphNode* n ) const;

     GraphNode*
     topGraphNodeStack( uint64_t streamId );

     void
     popGraphNodeStack( uint64_t streamId );

     void
     pushGraphNodeStack( GraphNode* node, uint64_t streamId );

     void
     sanityCheckEdge( Edge* edge, uint32_t mpiRank );

     void
     addNewGraphNodeInternal( GraphNode* node, EventStream* stream );

     static io::ITraceWriter::ProcessGroup
     streamTypeToGroup( EventStream::EventStreamType pt );

 };

}
