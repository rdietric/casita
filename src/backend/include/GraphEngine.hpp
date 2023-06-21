/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2019,
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

#include "utils/Utils.hpp"
#include "graph/Graph.hpp"
#include "graph/EventNode.hpp"
#include "EventStreamGroup.hpp"
#include "FunctionTable.hpp"

#define SYNC_DELTA 500 /* in us */

namespace casita
{

  class GraphEngine
  {
    private:

      Graph::EdgeList emptyEdgeList;
      /*     typedef struct */
      /*     { */
      /*       uint64_t startTime; */
      /*       uint64_t endTime; */
      /*       uint64_t exclEvtRegTime; //<! time of regions from CPU events between paradigm nodes */
      /*       uint32_t numberOfEvents; */
      /*     } EdgeCPUData; */
      /*  */
      /*     std::map< uint64_t, EdgeCPUData > cpuDataPerProcess; */

    public:
      typedef std::stack< GraphNode* > GraphNodeStack;
      typedef std::map< uint64_t, GraphNodeStack > GraphNodeStackMap;

      GraphEngine( );
      virtual
      ~GraphEngine( );

      Graph&
      getGraph( );

      Graph*
      getGraph( Paradigm paradigm );

      AnalysisMetric&
      getCtrTable( );

      virtual void
      reset( );

      void
      resetCounters( );

      EventStream*
      getStream( uint64_t id ) const;

      EventStreamGroup&
      getStreamGroup( );

      /**
       * Get a constant reference to a vector of all execution streams.
       *
       * @return constant reference to a vector of all streams.
       */
      const EventStreamGroup::EventStreamList&
      getStreams( ) const;

      void
      getStreams( EventStreamGroup::EventStreamList& streams,
          Paradigm                                   paradigm ) const;

      const EventStreamGroup::EventStreamList&
      getHostStreams( ) const;

      const EventStreamGroup::DeviceStreamList&
      getDeviceStreams( ) const;

      size_t
      getNumDeviceStreams( ) const;

      const EventStreamGroup::DeviceStreamList&
      getDeviceStreams( int deviceId );

      void
      getDeviceStreams( EventStreamGroup::DeviceStreamList& deviceStreams ) const;

      /* allocators */
      void
      newEventStream( uint64_t         id,
          uint64_t                     parentId,
          const std::string            name,
          EventStream::EventStreamType streamType );

      Edge*
      newEdge( GraphNode* n1, GraphNode* n2,
          bool blocking = false, Paradigm* paradigm = NULL );

      GraphNode*
      newGraphNode( uint64_t time,
          uint64_t           streamId,
          const char*        name,
          Paradigm           paradigm,
          RecordType         recordType,
          int                nodeType );

      EventNode*
      newEventNode( uint64_t            time,
          uint64_t                      streamId,
          uint64_t                      eventId,
          EventNode::FunctionResultType fResult,
          const char*                   name,
          Paradigm                      paradigm,
          RecordType                    recordType,
          int                           nodeType );

      GraphNode*
      addNewGraphNode( uint64_t time,
          EventStream*          stream,
          const char*           name,
          Paradigm              paradigm,
          RecordType            recordType,
          int                   nodeType );

      EventNode*
      addNewEventNode( uint64_t         time,
          uint64_t                      eventId,
          EventNode::FunctionResultType fResult,
          EventStream*                  stream,
          const char*                   name,
          FunctionDescriptor*           desc );

      /*     void */
      /*     addCPUEvent( uint64_t time, uint64_t stream, bool isLeave ); */

      Edge*
      getEdge( GraphNode* source, GraphNode* target );

      void
      removeEdge( Edge* e );

      Edge*
      getShortestInEdge( GraphNode* node ) const;

      GraphNode*
      getSourceNode( ) const;

      GraphNode*
      getFirstTimedGraphNode( Paradigm paradigm ) const;

      GraphNode*
      getLastGraphNode( Paradigm paradigm ) const;

      void
      getAllNodes( EventStream::SortedGraphNodeList& allNodes ) const;

      void
      setTimerResolution( uint64_t ticksPerSecond );

      uint64_t
      getTimerResolution( );

      double
      getRealTime( uint64_t t );

      const std::string
      getNodeInfo( Node* node );

      void
      streamWalkBackward( GraphNode* node, EventStream::StreamWalkCallback callback,
          void* userData ) const;

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

      /* create the only instance of AnalysisMetric */
      AnalysisMetric    ctrTable;

      /* >>> query graph objects <<< // */

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

  };

}
