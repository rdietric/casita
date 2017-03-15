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

#include <map>
#include <vector>
#include <string>
#include "utils/ErrorUtils.hpp"
#include "graph/Graph.hpp"
#include "graph/Node.hpp"
#include "graph/EventNode.hpp"
#include "graph/Edge.hpp"
#include "otf/OTF2ParallelTraceWriter.hpp"
#include "EventStream.hpp"
#include "EventStreamGroup.hpp"
#include "AnalysisMetric.hpp"
#include "FunctionTable.hpp"

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
       uint64_t exclEvtRegTime; //<! time of regions from CPU events between paradigm nodes
       uint32_t numberOfEvents;
     } EdgeCPUData;

     std::map< uint64_t, EdgeCPUData > cpuDataPerProcess;

   public:
     typedef std::map< uint64_t, EventStream* > EventStreamMap;
     typedef std::stack< GraphNode* > GraphNodeStack;
     typedef std::map< uint64_t, GraphNodeStack > GraphNodeStackMap;

     GraphEngine();
     virtual
     ~GraphEngine();

     Graph&
     getGraph();

     Graph*
     getGraph( Paradigm paradigm );

     AnalysisMetric&
     getCtrTable();

     virtual void
     reset();

     void
     resetCounters();

     EventStream*
     getStream( uint64_t id ) const;
     
     /**
      * Get a constant reference to a vector of all execution streams.
      * 
      * @return constant reference to a vector of all streams.
      */
     const EventStreamGroup::EventStreamList&
     getStreams() const;

     void
     getStreams( EventStreamGroup::EventStreamList& streams,
                 Paradigm                           paradigm ) const;

     const EventStreamGroup::EventStreamList&
     getHostStreams() const;

     const EventStreamGroup::EventStreamList&
     getDeviceStreams() const;
     
     void
     getDeviceStreams( int deviceId, 
                       EventStreamGroup::EventStreamList& deviceStreams ) const;

     void
     getAllDeviceStreams( EventStreamGroup::EventStreamList& deviceStreams ) const;

     /* allocators */
     EventStream*
     newEventStream( uint64_t                     id,
                     uint64_t                     parentId,
                     const std::string            name,
                     EventStream::EventStreamType streamType );

     Edge*
     newEdge( GraphNode* n1, GraphNode* n2, 
              int properties = EDGE_NONE, Paradigm* paradigm = NULL );

     GraphNode*
     newGraphNode( uint64_t          time,
                   uint64_t          streamId,
                   const std::string name,
                   Paradigm          paradigm,
                   RecordType        recordType,
                   int               nodeType );

     EventNode*
     newEventNode( uint64_t                      time,
                   uint64_t                      streamId,
                   uint64_t                      eventId,
                   EventNode::FunctionResultType fResult,
                   const std::string             name,
                   Paradigm                      paradigm,
                   RecordType                    recordType,
                   int                           nodeType );

     GraphNode*
     addNewGraphNode( uint64_t       time,
                      EventStream*   stream,
                      const char*    name,
                      Paradigm       paradigm,
                      RecordType     recordType,
                      int            nodeType );

     EventNode*
     addNewEventNode( uint64_t                      time,
                      uint64_t                      eventId,
                      EventNode::FunctionResultType fResult,
                      EventStream*                  stream,
                      const char*                   name,
                      FunctionDescriptor*           desc );

     void
     addCPUEvent( uint64_t time, uint64_t stream, bool isLeave );

     Edge*
     getEdge( GraphNode* source, GraphNode* target );

     void
     removeEdge( Edge* e );

     GraphNode*
     getSourceNode() const;

     GraphNode*
     getFirstTimedGraphNode( Paradigm paradigm ) const;

     GraphNode*
     getLastGraphNode( Paradigm paradigm ) const;

     void
     getAllNodes( EventStream::SortedGraphNodeList& allNodes ) const;

     void
     setTimerResolution( uint64_t ticksPerSecond );

     uint64_t
     getTimerResolution();

     uint64_t
     getDeltaTicks();

     void
     runSanityCheck( uint32_t mpiRank );

   protected:
     EventStreamGroup  streamGroup;
     uint64_t ticksPerSecond;

     Graph graph;
     GraphNode* globalSourceNode;
     GraphNodeStackMap pendingGraphNodeStackMap;

     EventStreamMap streamsMap;

     // create the only instance of AnalysisMetric
     AnalysisMetric ctrTable;

     // >>> query graph objects <<< //
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

     //static io::OTF2ParallelTraceWriter::ProcessGroup
     //streamTypeToGroup( EventStream::EventStreamType pt );

 };

}
