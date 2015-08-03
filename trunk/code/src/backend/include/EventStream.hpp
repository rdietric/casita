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

     // Types for blocking MPI communication
     enum MPIType
     {
       MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_SENDRECV, MPI_ONEANDALL
     };

     typedef struct
     {
       MPIType  mpiType;
       uint64_t rootId;         /**< root process ID (or 0) */
       uint64_t partnerId;      /**< process or process group */
     } MPICommRecord;
     
     typedef std::vector< MPICommRecord > MPICommRecordList;
     
     typedef struct
     {
       uint64_t   requestId;      /**< OTF2 request ID */
       GraphNode* leaveNode;      /**< leave node of MPI Irecv record */
     } MPIIrecvRecord;

     typedef std::vector< MPIIrecvRecord > MPIIrecvRecordList;

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
     insertGraphNode( GraphNode*                  node,
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
     
     /**
      * Save the request ID for the following MPI_Irecv leave node.
      * 
      * @param request MPI_Irecv request ID
      */
     void
     saveMPIIrecvRequest  ( uint64_t request );
     
     /**
      * Add the MPI_Irecv node to a list to add the partner stream ID later.
      * Consume the stored request ID.
      * 
      * @param node the MPI_Irecv request ID
      */
     void
     addPendingMPIIrecvNode( GraphNode* node );
     
    /**
     * Set the stream ID of the communication partner for a pending MPI_Irecv 
     * leave node.
     * 
     * @param partnerId stream ID of the communication partner
     * @param requestId MPI_Irecv request ID
     */
     void
     handleMPIIrecvEventData ( uint64_t requestId, uint64_t partnerId );
     
     void
     handleMPIIsendEventData( uint64_t requestId, uint64_t partnerId );
             
     bool
     setMPIIsendNodeData( GraphNode* node );
     
     /**
      * Temporarily store the MPI_Isend request that is consumed by MPI_Wait leave.
      * See {@link #setMPIWaitNodeData(GraphNode* node)}.
      * 
      * @param request OTF2 MPI_Isend request ID
      */
     void
     saveMPIIsendRequest( uint64_t request );
     
     
     bool
     setMPIWaitNodeData( GraphNode* node );

     Edge::TimeProfileMap*
     newTimeProfile( );

     Edge::TimeProfileMap*
     getTimeProfile( );

     bool
     walkBackward( GraphNode* node, StreamWalkCallback callback, void* userData );

     bool
     walkForward( GraphNode* node, StreamWalkCallback callback, void* userData );

   private:
     uint64_t            id;
     uint64_t            parentId;
     const std::string   name;
     EventStreamType     streamType;
     bool                remoteStream;

     SortedGraphNodeList pendingKernels;    /* list of unsynchronized
                                             * kernels (leave records) */

     GraphNode*          lastNode;
     GraphData           graphData[NODE_PARADIGM_COUNT];
     SortedGraphNodeList nodes;
     SortedGraphNodeList unlinkedMPINodes;

     /**< pending blocking MPI communcation records */
     MPICommRecordList   mpiCommRecords;  
     
     /**< pending OTF2 request ID to be consumned by MPI_Isend, MPI_Irecv or 
          MPI_Wait leave node */
     uint64_t            pendingMPIRequestId;
     uint64_t            mpiIsendPartner; /**< partner ID of the MPI_Isend */
     
     MPIIrecvRecordList  mpiIrecvRecords; /**< list of MPI_Irecv record data */


     EventStream::SortedGraphNodeList::const_reverse_iterator
     findNode( GraphNode* node ) const;

     void
     addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node );

 };

}
