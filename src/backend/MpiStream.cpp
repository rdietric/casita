/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017-2019,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

#include "MpiStream.hpp"

using namespace casita;

MpiStream::MpiStream( uint64_t id,
    uint64_t                   parentId,
    const std::string          name ) :
  EventStream( id, parentId, name, ES_MPI ),
  pendingMPIRequestId( UINT64_MAX ),
  mpiIsendPartner( UINT64_MAX )
{
  pendingMpiComm.comRef = UINT32_MAX;
}

/* MpiStream::~MpiStream() { } */

void
MpiStream::reset( )
{
  EventStream::reset( );

  /* clear list of pending MPI blocking communication records */
  mpiCommRecords.clear( );

  /* reset temporary values for non-blocking MPI communication */
  pendingMPIRequestId = std::numeric_limits< uint64_t >::max( );
  mpiIsendPartner     = std::numeric_limits< uint64_t >::max( );

  /* reset list of pending request IDs (non-blocking MPI) */
  if ( !( pendingRequests.empty( ) ) )
  {
    UTILS_MSG_ONCE_OR( Parser::getVerboseLevel( ) >= VERBOSE_BASIC,
        "[%" PRIu64 "] Clear list of pending OTF2 requests (%lu)!",
        this->id, this->pendingRequests.size( ) );
    pendingRequests.clear( );
  }

  /* clear list of pending non-blocking MPI communication records */
  if ( !( mpiIcommRecords.empty( ) ) )
  {
    UTILS_MSG_ONCE_OR( Parser::getVerboseLevel( ) >= VERBOSE_BASIC,
        "[%" PRIu64 "] Clear list of pending non-blocking MPI communication "
                    "records (%lu)!", this->id, this->mpiIcommRecords.size( ) );

    mpiIcommRecords.clear( );
  }
}

/**
 *
 * @param mpiType
 * @param partnerId MPI rank of communication partner in communicator "root_comm_id"
 * @param root_comm_id root MPI rank in collectives or communicator for point to point
 */
void
MpiStream::setPendingMPIRecord( MPIType mpiType, uint32_t partnerId,
    uint32_t root_comm_id, uint32_t tag )
{
  /*MPICommRecord record;
  record.mpiType   = mpiType;
  record.partnerId = partnerId;    // the communicator for collectives
  record.rootId    = root_comm_id; // root rank for collectives, communicator for point to point
  record.tag       = tag;

  mpiCommRecords.push_back( record );*/

  switch ( mpiType )
  {
    case MPI_COLLECTIVE:
      pendingMpiComm.comRef        = partnerId;
      pendingMpiComm.sendPartnerId = root_comm_id;
      break;

    case MPI_RECV:
      pendingMpiComm.comRef        = root_comm_id;
      pendingMpiComm.recvPartnerId = partnerId;
      pendingMpiComm.recvTag       = tag;
      break;

    case MPI_SEND:
      pendingMpiComm.comRef        = root_comm_id;
      pendingMpiComm.sendPartnerId = partnerId;
      pendingMpiComm.sendTag       = tag;
      break;

    default: throw RTException( "Unknown EventStream::MPIType %u", mpiType );
  }
}

/**
 * Consume the pending (blocking) MPI records an retrieve a copy of the list.
 * The list should be cleared, when it is not needed any more.
 *
 * @return a copy of all pending (blocking) MPI records
 */
MpiStream::MPICommRecordList
MpiStream::getPendingMPIRecords( )
{
  /* create a copy of the current pending records */
  MPICommRecordList copyList;
  copyList.assign( mpiCommRecords.begin( ), mpiCommRecords.end( ) );
  /* the list is cleared in AnalysisParadigmMPI::handlePostLeave()) */

  /* clear the pending list */
  mpiCommRecords.clear( );
  return copyList;
}

MpiStream::MpiBlockingCommData&
MpiStream::getPendingMpiCommRecord( )
{
  return pendingMpiComm;
}

/**
 * Save the MPI_Irecv request ID. The directly following MPI_Irecv leave will
 * consume and invalidate it.
 * (Triggered by MPI_IrecvRequest event which is in between MPI_Irecv enter/leave.)
 * See {@link #addPendingMPIIrecvNode(GraphNode* node)}.
 *
 * @param requestId OTF2 MPI_Irecv request ID
 */
void
MpiStream::handleMPIIrecvRequest( uint64_t requestId )
{
  /* UTILS_OUT( "[%" PRIu64 "] Save MPI_Irecv request %" PRIu64, this->id, requestId); */

  pendingMPIRequestId = requestId;
}

/**
 * Add the request ID from the preceding MPI_Irecv request to the MPI_Irecv
 * leave node.
 * See {@link #handleMPIIrecvEventData(uint64_t requestId, uint64_t partnerId)}.
 *
 * @param node the graph node of the MPI_Irecv leave record
 */
void
MpiStream::handleMPIIrecvLeave( GraphNode* node )
{
  if ( pendingMPIRequestId == std::numeric_limits< uint64_t >::max( ) )
  {
    UTILS_MSG( Parser::getVerboseLevel( ) > VERBOSE_NONE,
        "%s: no request ID. Trace file might be corrupted!",
        node->getUniqueName( ).c_str( ) );

    /* make MPI_Irecv rule skip this node */
    node->setData( NULL );

    return;
  }

  MPIIcommRecord record;
  record.requests[0] = MPI_REQUEST_NULL;
  record.requests[1] = MPI_REQUEST_NULL;
  record.requestId   = pendingMPIRequestId;
  record.msgNode     = node;
  record.syncNode    = NULL;

  /* UTILS_OUT( "[%"PRIu64"] New MPI_Irecv record: %s Request ID: %"PRIu64, */
  /*           this->id, node->getUniqueName().c_str(), pendingMPIRequestId ); */

  /* add new record to map */
  mpiIcommRecords[pendingMPIRequestId] = record;

  /* set node-specific data to a pointer to the record in the map */
  node->setData( &mpiIcommRecords[pendingMPIRequestId] );

  /* invalidate request ID variable */
  pendingMPIRequestId = std::numeric_limits< uint64_t >::max( );
}

/**
 * Store information on the MPI_Irecv to be consumed by the directly following
 * MPI_Wait/Test[all].
 * Triggered by the MPI_Irecv record (between MPI_Wait/Test[all] enter and leave).
 *
 * @param requestId OTF2 MPI_Irecv request ID
 * @param partnerId stream ID of the communication partner
 */
void
MpiStream::handleMPIIrecv( uint64_t requestId, uint64_t partnerId,
    OTF2_CommRef comm, uint32_t tag )
{
  if ( mpiIcommRecords.count( requestId ) > 0 )
  {
    /* temporarily store the request that is consumed by MPI_Wait[all] leave event */
    pendingRequests.push_back( requestId );

    mpiIcommRecords[requestId].msgNode->setReferencedStreamId( partnerId );
    mpiIcommRecords[requestId].comRef = comm;
    mpiIcommRecords[requestId].msgTag = tag;

    /*UTILS_OUT( "[%" PRIu64 "] MPI_IRECV at %s:%lf from %" PRIu64 " with request ID %" PRIu64,
               this->id, mpiIcommRecords[ requestId ].msgNode->getUniqueName().c_str(),
               UTILS_GET_REALTIME(mpiIcommRecords[ requestId ].msgNode->getTime()),
               partnerId, requestId );*/
  }
  else
  {
    /* if non-blocking communication over interval boundaries occurs it would */
    /* probably not influence the critical path or generate waiting time or blame */
    UTILS_OUT( "[%" PRIu64 "<-%" PRIu64 "] Ignore MPI_Irecv communication "
                                        "over interval boundaries. (OTF2 request: %" PRIu64 ")",
        this->id, partnerId, requestId );
  }
}

/**
 * Store the MPI_Isend request that is consumed by MPI_Isend leave node.
 * Triggered by MPI_Isend communication record, between MPI_Isend enter/leave.
 *
 * This function is called while handling OTF2 MPI_ISEND record.
 *
 * @param partnerId stream ID of the communication partner
 * @param requestId OTF2 MPI_Isend request ID
 */
void
MpiStream::handleMPIIsend( uint64_t requestId, uint64_t partnerId,
    OTF2_CommRef comm, uint32_t tag )
{
  pendingMPIRequestId        = requestId;
  mpiIsendPartner            = partnerId;

  /* add new record to map */
  MPIIcommRecord record;
  record.comRef              = comm;
  record.msgTag              = tag;
  record.requests[0]         = MPI_REQUEST_NULL;
  record.requests[1]         = MPI_REQUEST_NULL;
  record.msgNode             = NULL;
  record.syncNode            = NULL;
  record.requestId           = requestId;
  mpiIcommRecords[requestId] = record;

  /*UTILS_OUT( "[%" PRIu64 "] MPI_ISEND to %" PRIu64 " with request ID %" PRIu64,
             this->id, partnerId, requestId );*/
}

/**
 * Handle the MPI_Isend leave node and add information from the previous
 * MPI_ISEND record. Consumes the
 * pending OTF2 request ID and the MPI_Isend communication partner ID, which
 * have been stored in handleMPIIsendEventData().
 *
 * This function is called when handling the MPI_ISEND (region) leave record.
 *
 * @param node the graph node of the MPI_Isend leave record
 */
void
MpiStream::handleMPIIsendLeave( GraphNode* node )
{
  /*  UTILS_ASSERT( pendingMPIRequestId != std::numeric_limits< uint64_t >::max() */
  /*                 && mpiIsendPartner != std::numeric_limits< uint64_t >::max(), */
  /*                "[%"PRIu64"] %s MPI request %"PRIu64" or MPI partner ID %"PRIu64 */
  /*                " is invalid", this->id, node->getUniqueName().c_str(), */
  /*                pendingMPIRequestId, mpiIsendPartner ); */

  if ( pendingMPIRequestId == std::numeric_limits< uint64_t >::max( ) ||
      mpiIsendPartner == std::numeric_limits< uint64_t >::max( ) )
  {
    if ( pendingMPIRequestId == mpiIsendPartner )
    {
      UTILS_MSG( Parser::getVerboseLevel( ) > VERBOSE_NONE,
          "%s: Neither MPI request ID nor partner available. "
          "Corrupted trace file?", node->getUniqueName( ).c_str( ) );
    }
    else
    {
      UTILS_MSG( Parser::getVerboseLevel( ) > VERBOSE_NONE,
          "%s: no MPI request ID (%" PRIu64 ") or partner (%" PRIu64 "). "
                                                                     "Corrupted trace file?",
          node->getUniqueName( ).c_str( ),
          pendingMPIRequestId, mpiIsendPartner );
    }

    /* "inform" MPI_Isend rule that this node is invalid */
    node->setData( NULL );

    return;
  }

  /* add new record to map */
  /*  MPIIcommRecord record; */
  /*  record.requests[0] = MPI_REQUEST_NULL; */
  /*  record.requests[1] = MPI_REQUEST_NULL; */
  /*  record.leaveNode = node; */
  /*  record.requestId = pendingMPIRequestId; */
  /*  mpiIcommRecords[pendingMPIRequestId] = record; */
  if ( mpiIcommRecords.count( pendingMPIRequestId ) > 0 )
  {
    mpiIcommRecords[pendingMPIRequestId].msgNode = node;
  }
  else
  {
    UTILS_MSG( Parser::getVerboseLevel( ) > VERBOSE_NONE,
        "%s:%lf no record found for MPI request ID (%" PRIu64 ").",
        node->getUniqueName( ).c_str( ), UTILS_GET_NODE_REALTIME( node ),
        pendingMPIRequestId );
    node->setData( NULL );
    return;
  }

  /* UTILS_OUT( "[%"PRIu64"] New MPI_Isend record: %s Request ID: %"PRIu64, */
  /*           this->id, node->getUniqueName().c_str(), pendingMPIRequestId ); */

  /* set node-specific data to a pointer to the record in the map */
  node->setData( &mpiIcommRecords[pendingMPIRequestId] );

  node->setReferencedStreamId( mpiIsendPartner );

  /* invalidate temporary stored request and partner ID */
  pendingMPIRequestId = std::numeric_limits< uint64_t >::max( );
  mpiIsendPartner     = std::numeric_limits< uint64_t >::max( );
}

/**
 * Push the request ID of the MPI_Isend complete on the stack. The request ID
 * is consumed by MPI_Test/Wait[all]. (Triggered by MPI_IsendComplete event.)
 * See {@link #setMPIWaitNodeData(GraphNode* node)}.
 *
 * @param requestId OTF2 MPI_Isend request ID
 */
void
MpiStream::handleMPIIsendComplete( uint64_t requestId )
{
  /* UTILS_OUT( "[%" PRIu64 "] Save MPI_Isend request %" PRIu64, this->id, requestId); */

  pendingRequests.push_back( requestId );
}

/**
 * Consume the directly preceding MPI_ISEND_COMPLETE and its request ID or an
 * MPI_IRECV communication record and its information to add it to the wait
 * leave node. In an OTF2 trace, completed communication operations are between
 * ENTER and LEAVE of MPI_Test/Wait[all].
 *
 * If there are no pending request, no communication operation has completed
 * here.
 *
 * @param node the graph node of the MPI_Wait leave record
 */
void
MpiStream::handleMPIWaitLeave( GraphNode* node )
{
  if ( pendingRequests.size( ) == 1 )
  {
    uint64_t pendingReqId = pendingRequests.back( );

    /* the request ID has to be already in the map from Irecv or Isend record */
    if ( mpiIcommRecords.count( pendingReqId ) > 0 )
    {
      /* UTILS_OUT( "[%" PRIu64 "] MPI_Wait: handle request ID %" PRIu64 " at %s:%lf", */
      /*           this->id, pendingReqId, UTILS_GET_NODE_INFO( node ) ); */

      /* add the wait leave node to the MPI_Icomm record */
      mpiIcommRecords[pendingReqId].syncNode = node;

      /* set MPIIcommRecord data as node-specific data */
      node->setData( &mpiIcommRecords[pendingReqId] );

      /* request ID is consumed, therefore pop it from the vector */
      pendingRequests.pop_back( );
    }
    else
    {
      UTILS_OUT( "[%" PRIu64 "] MPI_Wait: Could not find communication record "
                             "with request ID %" PRIu64 " and communication partner %"
          PRIu64, this->id, pendingReqId );
    }
  }
  else
  if ( pendingRequests.size( ) == 0 ) {
    UTILS_OUT( "[%" PRIu64 "] MPI_Wait: No pending request at %s:%lf",
        this->id, node->getUniqueName( ).c_str( ),
        UTILS_GET_NODE_REALTIME( node ) );

    /* assign waiting time to this node, as it is unnecessary */
    node->setCounter( WAITING_TIME,
        node->getTime( ) - node->getGraphPair( ).first->getTime( ) );
  }
  else
  {
    UTILS_OUT( "MPI_Wait: Pending OTF2 request IDs: %llu > 1 at %s:%lf",
        pendingRequests.size( ), node->getUniqueName( ).c_str( ),
        UTILS_GET_NODE_REALTIME( node ) );

    pendingRequests.erase( pendingRequests.begin( ) );
  }
}

/**
 * Consumes the pending OTF2 request IDs and sets the given node as
 * associated operation.
 *
 * @param node the graph node of the MPI_Waitall leave record
 */
void
MpiStream::setMPIWaitallNodeData( GraphNode* node )
{
  /* UTILS_ASSERT( pendingRequests.size() > 0, */
  /*              "List of pending OTF2 request IDs !> 0.\n"); */

  if ( pendingRequests.size( ) > 0 )
  {
    /* create a copy of the pending requests */
    MPIIcommRequestList* copyList = new MPIIcommRequestList( pendingRequests );
    node->setData( copyList );

    pendingRequests.clear( );
  }
  else
  {
    /* assign waiting time to this node, as it is unnecessary */
    node->setCounter( WAITING_TIME,
        node->getTime( ) - node->getGraphPair( ).first->getTime( ) );

    /* make sure the WaitAllRule is not triggered */
    node->setData( NULL );
  }
}

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
MpiStream::handleMPITest( GraphNode* mpiTestLeave )
{
  if ( pendingRequests.size( ) == 1 )
  {
    uint64_t pendingReqId = pendingRequests.back( );

    /* the request ID has to be already in the map from Irecv or Isend record */
    if ( mpiIcommRecords.count( pendingReqId ) > 0 )
    {
      /* UTILS_OUT( "[%" PRIu64 "] MPI_Test: handle request ID %" PRIu64 " at %s:%lf", */
      /*           this->id, pendingReqId, UTILS_GET_NODE_INFO( mpiTestLeave ) ); */

      /* add the wait/test leave node to the MPI_Icomm record */
      mpiIcommRecords[pendingReqId].syncNode = mpiTestLeave;

      /* set MPIIcommRecord data as node-specific data */
      /* mpiTestLeave->setData( &mpiIcommRecords[ pendingReqId ] ); // was needed in MPI_Test rule */

      /* request ID is consumed, therefore pop it from the vector */
      pendingRequests.pop_back( );
    }
    else
    {
      UTILS_OUT( "[%" PRIu64 "] MPI_Test leave: Could not find communication"
                             " record with request ID %" PRIu64 " and communication partner %"
          PRIu64, this->id, pendingReqId );
    }
  }
  else
  if ( pendingRequests.size( ) == 0 ) {
    /* MPI_Test does not complete an MPI_Isend/Irecv necessarily */

    /* assign waiting time to this node, as it is unnecessary */
    mpiTestLeave->setCounter( WAITING_TIME,
        mpiTestLeave->getTime( ) - mpiTestLeave->getGraphPair( ).first->getTime( ) );
  }
  else
  {
    UTILS_OUT( "MPI_Test: Pending OTF2 request IDs: %llu > 1 at %s",
        pendingRequests.size( ), mpiTestLeave->getUniqueName( ).c_str( ) );

    mpiIcommRecords.erase( pendingRequests.front( ) );
    pendingRequests.erase( pendingRequests.begin( ) );
  }
}

/**
 * Consumes the pending OTF2 request IDs and removes the associated
 * communication records.
 *
 * @param node the graph node of the MPI_Testall leave record
 */
void
MpiStream::handleMPITestall( GraphNode* node )
{
  if ( pendingRequests.size( ) > 0 )
  {
    /* iterate over all associated requests */
    MPIIcommRequestList::const_iterator it = pendingRequests.begin( );
    for (; it != pendingRequests.end( ); ++it )
    {
      /* remove associated record, as data is not used */
      removePendingMPIRequest( *it );
    }

    pendingRequests.clear( );
  }
  else
  {
    /* assign waiting time to this node, as it is unnecessary */
    node->setCounter( WAITING_TIME,
        node->getTime( ) - node->getGraphPair( ).first->getTime( ) );
  }
}

/**
 * Return the number of pending MPI requests.
 *
 * @return the number of pending MPI requests
 */
size_t
MpiStream::havePendingMPIRequests( )
{
  return mpiIcommRecords.size( );
}

/**
 * If the MPI request is not complete (is still in the list) we wait for it and
 * remove the handle from the list. Otherwise it might have been completed
 * before.
 *
 * @param requestId OTF2 request ID for replayed non-blocking communication to be completed.
 *
 * @return true, if the handle was found, otherwise false
 */
bool
MpiStream::waitForPendingMPIRequest( uint64_t requestId )
{
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin( );

  while ( it != mpiIcommRecords.end( ) )
  {
    if ( it->first == requestId )
    {
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
          "[%" PRIu64 "] Finish requests (%p) associated with OTF2 "
                      "request ID %llu \n", this->id, it->second, requestId );

      if ( it->second.requests[0] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &( it->second.requests[0] ), MPI_STATUS_IGNORE ) );
      }

      if ( it->second.requests[1] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &( it->second.requests[1] ), MPI_STATUS_IGNORE ) );
      }

      /* invalidate node-specific data */
      it->second.msgNode->setData( NULL );

      mpiIcommRecords.erase( it );

      return true;
    }
    else
    {
      it++;
    }
  }

  UTILS_OUT( "[%" PRIu64 "] OTF2 MPI request ID %" PRIu64 " could not be found."
                                                          " Has already completed?", this->id, requestId );

  return false;
}

/**
 * Get the MPI non-blocking communication record by OTF2 request ID.
 *
 * @param requestId OTF2 request ID
 *
 * @return the corresponding MPI non-blocking communication record
 */
MpiStream::MPIIcommRecord*
MpiStream::getPendingMPIIcommRecord( uint64_t requestId )
{
  /* invalidate node-specific data for the MPI_Isend or MPI_Irecv */
  try
  {
    return &( mpiIcommRecords.at( requestId ) );
  }
  catch( const std::out_of_range& oor )
  {
    UTILS_OUT( "[%" PRIu64 "] OTF2 MPI request ID %" PRIu64 " could not be found. "
                                                            "Has already completed? (%s)", this->id, requestId,
        oor.what( ) );
    return NULL;
  }
}

/**
 * Remove an MPI request form the map, when it has been processed (e.g.
 * successful MPI_Test or MPI_Wait).
 *
 * @param requestId OTF2 request ID for replayed non-blocking communication to be completed.
 *
 * @return true, if the handle was found, otherwise false
 */
void
MpiStream::removePendingMPIRequest( uint64_t requestId )
{
  /* "Because all elements in a map container are unique, count can only */
  /*  return 1 (if the element is found) or zero (otherwise)." */
  if ( mpiIcommRecords.count( requestId ) > 0 )
  {
    /* invalidate node-specific data for the MPI_Isend or MPI_Irecv */
    mpiIcommRecords[requestId].msgNode->setData( NULL );
    mpiIcommRecords.erase( requestId );
  }
  else
  {
    UTILS_OUT( "[%" PRIu64 "] OTF2 MPI request ID %" PRIu64 " could not be "
                                                            "found. Has already completed?", this->id, requestId );
  }
}

/**
 * Wait for all pending MPI requests that are associated with the given node.
 *
 * @param node the MPI_Waitall leave node
 *
 * @return true, if the handle was found, otherwise false
 */
void
MpiStream::waitForPendingMPIRequests( GraphNode* node )
{
  /* std::vector < MPI_Request > tmpRequests; */

  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin( );

  while ( it != mpiIcommRecords.end( ) )
  {
    if ( it->second.msgNode == node )
    {
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
          "[%" PRIu64 "] Finish requests (%p) associated with OTF2 "
                      "request ID %" PRIu64 " in waitForPendingMPIRequests()",
          this->id, it->second, it->second.requestId );

      if ( it->second.requests[0] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &( it->second.requests[0] ), MPI_STATUS_IGNORE ) );
      }

      if ( it->second.requests[1] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &( it->second.requests[1] ), MPI_STATUS_IGNORE ) );
      }

      /* invalidate node-specific data */
      it->second.msgNode->setData( NULL );

      /* delete allocated memory */
      /* delete it->second; */
      /* it->second = 0; */

      mpiIcommRecords.erase( it++ );
    }
    else
    {
      ++it;
    }
  }

  /* TODO: use an MPI_Waitall for performance reasons? */
  /*if( tmpRequests.size () )
  {
    MPI_CHECK( MPI_Waitall( tmpRequests.size(), &tmpRequests[0], MPI_STATUSES_IGNORE ) );
  }*/
}

/**
 * Wait for open MPI_Request handles.
 */
void
MpiStream::waitForAllPendingMPIRequests( )
{
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin( );

  UTILS_MSG( mpiIcommRecords.size( ) > 0,
      "[%" PRIu64 "] Number of pending MPI request handles at "
                  "MPI_Finalize: %lu", this->id, mpiIcommRecords.size( ) );

  for (; it != mpiIcommRecords.end( ); ++it )
  {
    MPI_Request request = it->second.requests[0];

    if ( MPI_REQUEST_NULL != request )
    {
      MPI_CHECK( MPI_Wait( &request, MPI_STATUS_IGNORE ) );
    }

    request = it->second.requests[1];

    if ( MPI_REQUEST_NULL != request )
    {
      MPI_CHECK( MPI_Wait( &request, MPI_STATUS_IGNORE ) );
    }

    /* invalidate node-specific data */
    it->second.msgNode->setData( NULL );
  }

  /* clear the map of pending non-blocking MPI operations */
  mpiIcommRecords.clear( ); /* invalidates all references and pointers for this container */
}

/**
 * Test for completed MPI_Request handles. Can be used to decrease the number of
 * open MPI request handles, e.g. at blocking collective operations.
 * This might improve the performance of the MPI implementation.
 */
void
MpiStream::testAllPendingMPIRequests( )
{
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin( );

  while ( it != mpiIcommRecords.end( ) )
  {
    MPI_Status status;
    int finished[2] = { 0, 0 };

    if ( MPI_REQUEST_NULL != it->second.requests[0] )
    {
      MPI_CHECK( MPI_Test( &( it->second.requests[0] ), &( finished[0] ), &status ) );
    }

    if ( MPI_REQUEST_NULL != it->second.requests[1] )
    {
      MPI_CHECK( MPI_Test( &( it->second.requests[1] ), &( finished[1] ), &status ) );
    }

    /* if both MPI_Irecv and MPI_Isend are finished, we can delete the record */
    if ( finished[0] && finished[1] )
    {

      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
          "[%" PRIu64 "] Finished requests (%p) with OTF2 request ID"
                      " %" PRIu64 " in testAllPendingMPIRequests()\n",
          this->id, it->second, it->second.requestId );

      /* invalidate node-specific data */
      it->second.msgNode->setData( NULL );

      /* delete it->second; */
      /* it->second = NULL; */

      mpiIcommRecords.erase( it++ );
    }
    else
    {
      if ( finished[0] )
      {
        it->second.requests[0] = MPI_REQUEST_NULL;
      }

      if ( finished[1] )
      {
        it->second.requests[1] = MPI_REQUEST_NULL;
      }

      ++it;
    }
  }
}
