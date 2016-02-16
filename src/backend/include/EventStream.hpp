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
#include <mpi.h>

#include "graph/GraphNode.hpp"
#include "graph/Edge.hpp"
#include "common.hpp"

#include <sys/time.h>

/** Number of elements for replayed MPI communication */
#define CASITA_MPI_P2P_BUF_SIZE 5

/** MPI type of buffer elements */
#define CASITA_MPI_P2P_ELEMENT_TYPE MPI_UNSIGNED_LONG_LONG

namespace casita
{

 class EventStream
 {
   public:

     
     // keep numbers to the power of two, as required by stream type identification
     enum EventStreamType
     {
       ES_HOST = 1, ES_HOST_MASTER = 2, ES_DEVICE = 4, ES_DEVICE_NULL = 8
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

     /**< list of request IDs */
     typedef std::vector< uint64_t > MPIIcommRequestList;
     
     typedef struct
     {
       uint64_t    requestId;   /**< OTF2 request ID */
       MPI_Request requests[2]; /**< internel MPI_Isend and MPI_Irecv request */
       uint64_t    sendBuffer[CASITA_MPI_P2P_BUF_SIZE]; /**< MPI_Isend buffer */
       uint64_t    recvBuffer[CASITA_MPI_P2P_BUF_SIZE]; /**< MPI_Irecv buffer */
       GraphNode*  leaveNode;   /**< pointer to associated node of MPI Icomm */
     } MPIIcommRecord;
     
     /**< Map of OTF2 request IDs (key) and the corresponding record data */
     typedef std::map< uint64_t, MPIIcommRecord > MPIIcommRecordMap;
     //typedef std::map< uint64_t, MPIIcommRecord * > MPIIcommRecordMap;

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
                  EventStreamType eventStreamType, bool remoteStream = false );

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
     isHostMasterStream( ) const;

     bool
     isDeviceStream( ) const;

     bool
     isDeviceNullStream( ) const;

     bool
     isRemoteStream( ) const;
     
     /**
      * Compare function:
      * Sort the streams by stream id, but with host streams first.
      */
     static bool
     streamSort( const EventStream* p1, const EventStream* p2 )
     {
       if ( p1->isDeviceStream( ) && p2->isHostStream( ) )
       {
         return false;
       }
        if ( p2->isDeviceStream( ) && p1->isHostStream( ) )
       {
         return true;
       }
        return p1->getId( ) <= p2->getId( );
     }
     
     /**
      * Get the stream's first enter and last leave time stamps
      * 
      * @return a pair the first enter and last leave time stamp
      */
     std::pair< uint64_t, uint64_t >&
     getPeriod( );
     
     /**
      * Does this stream contains the global first critical node?
      * 
      * @return true, if the critical path starts on this stream
      */
     bool&
     isFirstCritical( );
     
     /**
      * Does this stream contains the global last event (of the trace)?
      * 
      * @return true, if the critical path ends on this stream
      */
     bool&
     hasLastGlobalEvent( );

     GraphNode*
     getLastNode( ) const;

     GraphNode*
     getLastNode( Paradigm paradigm ) const;

     GraphNode*
     getFirstNode( Paradigm paradigm ) const;

     /**
      * Get the time stamp of the last event read from an event stream. 
      * (This is often a RMA window destroy event.)
      * 
      * @return time stamp of the last event
      */
     uint64_t
     getLastEventTime( ) const;
     
     /**
      * Set the time of the last read event. 
      * (This is often a RMA window destroy event.)
      * 
      * @param time time to be set.
      */
     void
     setLastEventTime( uint64_t time );

     void
     addGraphNode( GraphNode* node, GraphNode::ParadigmNodeMap* predNodes );

     void
     insertGraphNode( GraphNode*                  node,
                      GraphNode::ParadigmNodeMap& predNodes,
                      GraphNode::ParadigmNodeMap& nextNodes );

     EventStream::SortedGraphNodeList&
     getNodes( );
     
     void
     clearNodes( );

     void
     addPendingKernel( GraphNode* kernelLeave );

     GraphNode*
     getPendingKernel( );

     GraphNode*
     consumePendingKernel( );
     
     /**
      * Consume all pending kernels before the given node.
      */
     void
     consumePendingKernels( GraphNode* kernelEnter );

     void
     clearPendingKernels( );

     void
     setPendingMPIRecord( MPIType mpiType, uint64_t partnerId, uint64_t rootId );

     /**
      * Consume the pending (blocking) MPI records an retrieve a copy of the list.
      * The list should be cleared, when it is not needed any more.
      * 
      * @return a copy of all pending (blocking) MPI records
      */
     EventStream::MPICommRecordList
     getPendingMPIRecords( );
     
     /**
      * Temporarily save the MPI_Irecv request ID. The following MPI_Irecv function 
      * leave record will consume and invalidate it. 
      * See {@link #addPendingMPIIrecvNode(GraphNode* node)}.
      * 
      * @param requestId OTF2 MPI_Irecv request ID
      */
     void
     saveMPIIrecvRequest( uint64_t request );
     
     /**
      * Temporarily save the MPI_Isend request that is consumed by MPI_Wait leave.
      * See {@link #setMPIWaitNodeData(GraphNode* node)}.
      * 
      * @param request OTF2 MPI_Isend request ID
      */
     void
     saveMPIIsendRequest( uint64_t request );
     
     /**
      * Store the MPI_Irecv leave node together with the MPI_Request handle. The 
      * MPI_Irecv record provides the communication partner ID and the MPI_request to 
      * put it all together. 
      * See {@link #setMPIIrecvPartnerStreamId(uint64_t requestId, uint64_t partnerId)}.
      * 
      * @param node the graph node of the MPI_Irecv leave record
      */
     void
     addPendingMPIIrecvNode( GraphNode* node );
     
     /**
      * Set partner stream ID for the given MPI_Irecv request ID.
      * The node is identified by the given request ID.
      * It saves the request ID to be consumed by the following MPI_Wait leave node. 
      * Triggered by the MPI_Irecv record (between MPI_Wait enter and leave).
      * 
      * @param requestId OTF2 MPI_Irecv request ID 
      * @param partnerId stream ID of the communication partner
      */
     void
     handleMPIIrecvEventData ( uint64_t requestId, uint64_t partnerId );
     
     /**
      * Temporarily store the request that is consumed by MPI_Isend leave event.
      * Triggered by MPI_Isend communication record, between MPI_Isend enter/leave.
      * 
      * @param partnerId stream ID of the communication partner
      * @param requestId OTF2 MPI_Isend request ID 
      */
     void
     handleMPIIsendEventData( uint64_t requestId, uint64_t partnerId );

     /**
      * Adds MPI_Isend request to a map and sets node-specific data. 
      * Consumes the pending OTF2 request ID and the MPI_Isend communication partner ID.
      * 
      * @param node the graph node of the MPI_Isend leave record
      */
     void
     setMPIIsendNodeData( GraphNode* node );

     /**
      * Sets node-specific data for the given MPI_Wait leave node.
      * Consumes the pending OTF2 request ID.
      * 
      * @param node the graph node of the MPI_Wait leave record
      */
     void
     setMPIWaitNodeData( GraphNode* node );
     
     /**
      * Consumes the pending OTF2 request IDs and sets the given node as 
      * associated operation.
      * 
      * @param node the graph node of the MPI_Waitall leave record
      */
     void
     setMPIWaitallNodeData( GraphNode* node );
     
     /**
      * Return whether we have pending MPI requests or not.
      * 
      * @return true, if we have pending MPI requests in the list.
      */
     bool
     havePendingMPIRequests( );

     /**
      * Safely complete MPI request that are associated with the request ID.
      * (Only if the request ID is the pending map.)
      * 
      * @param requestId OTF2 request for replayed non-blocking communication to be completed.
      * 
      * @return true, if the handle was found, otherwise false
      */
     bool
     waitForPendingMPIRequest( uint64_t requestId );
     

     /**
      * Wait for all pending MPI requests that are associated with the given node.
      * 
      * @param node the MPI_Waitall leave node
      * 
      * @return true, if the handle was found, otherwise false
      */
     void
     waitForPendingMPIRequests( GraphNode* node );
     
     /**
      * Analysis rules for non-blocking MPI communication:
      * 
      * Wait for open MPI_Request handles. Should be called before MPI_Finalize().
      */
     void
     waitForAllPendingMPIRequests( );
     
     /**
      * Analysis rules for non-blocking MPI communication:
      * 
      * Test for completed MPI_Request handles. Can be used to decrease the number of 
      * open MPI request handles, e.g. at blocking collective operations.
      * This might improve the performance of the MPI implementation. 
      */
     void
     testAllPendingMPIRequests( );

     Edge::TimeProfileMap*
     newTimeProfile( );

     Edge::TimeProfileMap*
     getTimeProfile( );

     bool
     walkBackward( GraphNode* node, StreamWalkCallback callback, void* userData );

     bool
     walkForward( GraphNode* node, StreamWalkCallback callback, void* userData );
     
     /**
      * Did the stream change (new nodes added) since the interval start
      * 
      * @return true, if nodes have been added, otherwise false
      */
     bool
     hasNewNodes( );
     
     void
     reset( );

   private:
     uint64_t            id;
     uint64_t            parentId;
     const std::string   name;
     EventStreamType     streamType;
     bool                remoteStream;
     bool                nodesAdded; //!< has the stream new nodes?
     
     //!< first enter and last leave time
     std::pair< uint64_t, uint64_t > streamPeriod; 
     
     //!< Does this stream contain the first (global) critical path node?
     bool                hasFirstCriticalNode;
     
     //! Does this stream contain the last global event of the trace?
     bool                hasLastEvent;

     //!< list of unsynchronized kernels (leave nodes only)
     SortedGraphNodeList pendingKernels;

     //!< pointer to the last node (paradigm independent) of the analysis interval
     GraphNode*          lastNode;
     
     //! time stamp of the last read event for this stream (e.g. RMA win destroy)
     uint64_t            lastEventTime;
     
     //<! first and last node of the analysis interval (for each paradigm)
     GraphData           graphData[NODE_PARADIGM_COUNT];
     SortedGraphNodeList nodes;
     SortedGraphNodeList unlinkedMPINodes;

     //!< pending blocking MPI communcation records
     MPICommRecordList   mpiCommRecords;
     
     //!< pending OTF2 request ID to be consumned by MPI_Isend, MPI_Irecv or MPI_Wait leave node
     uint64_t            pendingMPIRequestId;
     uint64_t            mpiIsendPartner; /**< partner ID of the MPI_Isend */
     
     //!< Pending OTF2 request IDs (not yet associated to a MPI_Wait[all] leave node
     MPIIcommRequestList pendingRequests;
     
     //!< pending non-blocking MPI communication records
     MPIIcommRecordMap   mpiIcommRecords;

     EventStream::SortedGraphNodeList::const_reverse_iterator
     findNode( GraphNode* node ) const;

     void
     addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node );
 };

}
