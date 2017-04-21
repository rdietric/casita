/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

#pragma once

#include "EventStream.hpp"

namespace casita
{

  class MpiStream : public EventStream
  {
    public:
      
      //!< Types for blocking MPI communication
      enum MPIType
      {
        MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_SENDRECV/*, MPI_ONEANDALL*/
      };

      typedef struct
      {
        MPIType  mpiType;
        uint32_t rootId;    //!< root process ID (collectives) or communicator (P2P)
        uint64_t partnerId; //!< process or process group (communicator)
        uint32_t tag;       //!< MPI communication tag
      } MPICommRecord;
     
      typedef struct
      {
        uint32_t comRef;        //!< MPI communicator OTF2 reference
        uint32_t sendPartnerId; //!< communication partner process ID
        uint32_t recvPartnerId; //!< communication partner process ID
        uint32_t sendTag;       //!< MPI communication tag
        uint32_t recvTag;       //!< MPI communication tag
      } MpiBlockingCommData;
     
      typedef std::vector< MPICommRecord > MPICommRecordList;

      //!< list of request IDs
      typedef std::vector< uint64_t > MPIIcommRequestList;
     
      typedef struct
      {
        uint32_t    comRef;      //!< MPI communicator  OTF2 reference
        uint32_t    msgTag;      //!< MPI communication tag
        uint64_t    requestId;   //!< OTF2 request ID
        MPI_Request requests[ 2 ]; //!< internel MPI_Isend and MPI_Irecv request
        uint64_t    sendBuffer[ CASITA_MPI_P2P_BUF_SIZE ]; //!< MPI_Isend buffer
        uint64_t    recvBuffer[ CASITA_MPI_P2P_BUF_SIZE ]; //!< MPI_Irecv buffer
        GraphNode*  leaveNode;   //!< pointer to associated MPI_I[send|recv] node
      } MPIIcommRecord;
     
      //!< Map of OTF2 request IDs (key) and the corresponding record data
      typedef std::map< uint64_t, MPIIcommRecord > MPIIcommRecordMap;
     
      MpiStream( uint64_t id, uint64_t parentId, const std::string name );
      //virtual ~MpiStream( );
      
      void
      reset();
      
      void
      setPendingMPIRecord( MPIType mpiType, uint32_t partnerId, 
                           uint32_t root_comm_id, uint32_t tag );

      /**
       * Consume the pending (blocking) MPI records an retrieve a copy of the list.
       * The list should be cleared, when it is not needed any more.
       * 
       * @return a copy of all pending (blocking) MPI records
       */
      MpiStream::MPICommRecordList
      getPendingMPIRecords();
     
      MpiBlockingCommData&
      getPendingMpiCommRecord();
     
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
      handleMPIIrecvEventData ( uint64_t requestId, uint64_t partnerId, 
                               OTF2_CommRef comm, uint32_t tag );
     
      /**
       * Temporarily store the request that is consumed by MPI_Isend leave event.
       * Triggered by MPI_Isend communication record, between MPI_Isend enter/leave.
       * 
       * @param partnerId stream ID of the communication partner
       * @param requestId OTF2 MPI_Isend request ID 
       */
      void
      handleMPIIsendEventData( uint64_t requestId, uint64_t partnerId,
                               OTF2_CommRef comm, uint32_t tag );

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
       * Consumes the pending OTF2 request ID and remove the corresponding record, as 
       * MPI_Test does not influence the critical path. If it completes a non-blocking
       * communication it is not even waiting time. In an OTF2 trace, completed 
       * communication operations are between ENTER and LEAVE of MPI_Wait[all],
       * MPI_Test[all].
       * 
       * If there are no pending request, no communication operation has completed 
       * here.
       * 
       * @param node the graph node of the MPI_Test leave record
       */
      void
      handleMPITest( GraphNode* node );

      /**
       * Consumes the pending OTF2 request IDs and removes the associated 
       * communication records.
       * 
       * @param node the graph node of the MPI_Testall leave record
       */
      void
      handleMPITestall( GraphNode* node );

      /**
       * Return whether we have pending MPI requests or not.
       * 
       * @return true, if we have pending MPI requests in the list.
       */
      bool
      havePendingMPIRequests();

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

      MPIIcommRecord*
      getPendingMPIIcommRecord( uint64_t requestId );

      /**
       * Remove an MPI request form the map, when it has been processed (e.g. 
       * successful MPI_Test or MPI_Wait).
       * 
       * @param requestId OTF2 request ID for replayed non-blocking communication to be completed.
       * 
       * @return true, if the handle was found, otherwise false
       */
      void
      removePendingMPIRequest( uint64_t requestId );

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
      waitForAllPendingMPIRequests();

      /**
       * Analysis rules for non-blocking MPI communication:
       * 
       * Test for completed MPI_Request handles. Can be used to decrease the number of 
       * open MPI request handles, e.g. at blocking collective operations.
       * This might improve the performance of the MPI implementation. 
       */
      void
      testAllPendingMPIRequests();
      
    private:
      //!< pending blocking MPI communcation records
      MPICommRecordList   mpiCommRecords;

      MpiBlockingCommData pendingMpiComm;

      //!< pending OTF2 request ID to be consumned by MPI_Isend, MPI_Irecv or MPI_Wait leave node
      uint64_t            pendingMPIRequestId;
      uint64_t            mpiIsendPartner; /**< partner ID of the MPI_Isend */

      //!< Pending OTF2 request IDs (not yet associated to a MPI_Wait[all] leave node
      MPIIcommRequestList pendingRequests;

      //!< pending non-blocking MPI communication records
      MPIIcommRecordMap   mpiIcommRecords;
  };

}

