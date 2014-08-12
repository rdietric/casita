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

#include <vector>
#include <algorithm>
#include <string>
#include <list>
#include <iostream>
#include <map>
#include <math.h>

#include "graph/GraphNode.hpp"
#include "graph/Edge.hpp"
#include "common.hpp"

#include <sys/time.h>

namespace casita
{

 class EventStream
 {
   public:

     enum EventStreamType
     {
       ES_HOST = 1, ES_DEVICE = 2, ES_DEVICE_NULL = 3
     };

     enum MPIType
     {
       MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_SENDRECV, MPI_ONEANDALL
     };

     typedef struct
     {
       MPIType mpiType;
       uint64_t rootId;         /* root process ID (or 0) */
       uint64_t partnerId;      /* process or process group */
     } MPICommRecord;

     typedef std::vector< MPICommRecord > MPICommRecordList;

     typedef std::vector< GraphNode* > SortedGraphNodeList;

     typedef bool ( *StreamWalkCallback )( void* userData, GraphNode* node );

   private:

     typedef struct
     {
       GraphNode* firstNode;
       GraphNode* lastNode;
     } GraphData;

   public:

     EventStream( uint64_t id, uint64_t parentId, const std::string name,
                  EventStreamType eventStreamType, bool remoteStream =
                    false );

     virtual
     ~EventStream( );

     uint64_t
     getId( ) const;

     uint64_t
     getParentId( ) const;

     const char*
     getName( ) const;

     EventStream::EventStreamType
     getStreamType( ) const;

     bool
     isHostStream( ) const;

     bool
     isDeviceStream( ) const;

     bool
     isDeviceNullStream( ) const;

     bool
     isRemoteStream( ) const;

     GraphNode*
     getLastNode( ) const;

     GraphNode*
     getLastNode( Paradigm paradigm ) const;

     GraphNode*
     getFirstNode( Paradigm paradigm ) const;

     uint64_t
     getLastEventTime( ) const;

     void
     addGraphNode( GraphNode* node, GraphNode::ParadigmNodeMap* predNodes );

     void
     insertGraphNode( GraphNode* node,
                      GraphNode::ParadigmNodeMap& predNodes,
                      GraphNode::ParadigmNodeMap& nextNodes );

     EventStream::SortedGraphNodeList&
     getNodes( );

     void
     addPendingKernel( GraphNode* kernelLeave );

     GraphNode*
     getPendingKernel( );

     GraphNode*
     consumePendingKernel( );

     void
     clearPendingKernels( );

     void
     setPendingMPIRecord( MPIType mpiType, uint64_t partnerId, uint64_t rootId );

     EventStream::MPICommRecordList
     getPendingMPIRecords( );

     Edge::TimeProfileMap*
     newTimeProfile( );

     Edge::TimeProfileMap*
     getTimeProfile( );

     bool
     walkBackward( GraphNode* node, StreamWalkCallback callback, void* userData );

     bool
     walkForward( GraphNode* node, StreamWalkCallback callback, void* userData );

   private:
     uint64_t id;
     uint64_t parentId;
     const std::string name;
     EventStreamType streamType;
     bool remoteStream;

     SortedGraphNodeList pendingKernels;    /* list of unsynchronized
                                             * kernels (leave records) */

     GraphNode* lastNode;
     GraphData graphData[NODE_PARADIGM_COUNT];
     SortedGraphNodeList nodes;
     SortedGraphNodeList unlinkedMPINodes;

     MPICommRecordList mpiCommRecords;

     Edge::TimeProfileMap* currentTimeProfile;

     EventStream::SortedGraphNodeList::const_reverse_iterator
     findNode( GraphNode* node ) const;

     void
     addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node );

 };

}
