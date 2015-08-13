/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * - Basic interaction with an event stream: add/remove/insert nodes, getter, get Attributes about eventstream
 * - walk forward/backward through stream (and call callback for each node on that walk)
 * - manage pending/consuming kernels (CUDA)
 * - manage pending MPIRecords
 *
 */

#include "EventStream.hpp"
#include "utils/ErrorUtils.hpp"



//#define UINT64_MAX 0xFFFFFFFFFFFFFFFF

using namespace casita;

EventStream::EventStream( uint64_t          id,
                          uint64_t          parentId,
                          const std::string name,
                          EventStreamType   eventStreamType,
                          bool              remoteStream ) :
  id( id ),
  parentId( parentId ),
  name( name ),
  streamType( eventStreamType ),
  remoteStream( remoteStream ),
  lastNode( NULL ),
  pendingMPIRequestId( UINT64_MAX ),
  mpiIsendPartner( UINT64_MAX )
{
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    graphData[i].firstNode = NULL;
    graphData[i].lastNode  = NULL;
  }
}

EventStream::~EventStream( )
{
  for ( SortedGraphNodeList::iterator iter = nodes.begin( );
        iter != nodes.end( ); ++iter )
  {
    delete( *iter );
  }
}

uint64_t
EventStream::getId( ) const
{
  return id;
}

uint64_t
EventStream::getParentId( ) const
{
  return parentId;
}

const char*
EventStream::getName( ) const
{
  return name.c_str( );
}

EventStream::EventStreamType
EventStream::getStreamType( ) const
{
  return streamType;
}

bool
EventStream::isHostStream( ) const
{
  return streamType == ES_HOST;
}

bool
EventStream::isDeviceStream( ) const
{
  return streamType != ES_HOST;
}

bool
EventStream::isDeviceNullStream( ) const
{
  return streamType == ES_DEVICE_NULL;
}

bool
EventStream::isRemoteStream( ) const
{
  return remoteStream;
}

GraphNode*
EventStream::getLastNode( ) const
{
  return getLastNode( PARADIGM_ALL );
}

GraphNode*
EventStream::getLastNode( Paradigm paradigm ) const
{
  size_t     i           = 0;
  GraphNode* tmpLastNode = NULL;

  for ( i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    Paradigm tmpP = (Paradigm)( 1 << i );
    if ( tmpP & paradigm )
    {
      if ( graphData[i].lastNode )
      {
        tmpLastNode = graphData[i].lastNode;
        break;
      }
    }

  }

  i++;

  for (; i < NODE_PARADIGM_COUNT; ++i )
  {
    Paradigm tmpP = (Paradigm)( 1 << i );
    if ( tmpP & paradigm )
    {
      if ( graphData[i].lastNode &&
           Node::compareLess( tmpLastNode, graphData[i].lastNode ) )
      {
        tmpLastNode = graphData[i].lastNode;
      }
    }

  }

  return tmpLastNode;
}

GraphNode*
EventStream::getFirstNode( Paradigm paradigm ) const
{
  return graphData[(int)log2( paradigm )].firstNode;
}

uint64_t
EventStream::getLastEventTime( ) const
{
  if ( lastNode )
  {
    return lastNode->getTime( );
  }
  else
  {
    return 0;
  }
}

void
EventStream::addGraphNode( GraphNode*                  node,
                           GraphNode::ParadigmNodeMap* predNodes )
{
  GraphNode* oldNode[NODE_PARADIGM_COUNT];
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    oldNode[i] = NULL;
  }
  Paradigm   nodeParadigm = node->getParadigm( );

  for ( size_t o = 1; o < NODE_PARADIGM_INVALID; o *= 2 )
  {
    Paradigm oparadigm      = (Paradigm)o;
    size_t   paradigm_index = (size_t)log2( oparadigm );

    oldNode[paradigm_index] = getLastNode( oparadigm );
    if ( predNodes && ( oldNode[paradigm_index] ) )
    {
      predNodes->insert( std::make_pair( oparadigm,
                                         oldNode[paradigm_index] ) );
    }

    if ( node->hasParadigm( oparadigm ) )
    {
      if ( oldNode[paradigm_index] &&
           Node::compareLess( node, oldNode[paradigm_index] ) )
      {
        throw RTException(
                "Can't add graph node (%s) before last graph node (%s)",
                node->getUniqueName( ).c_str( ),
                oldNode[paradigm_index]->getUniqueName( ).c_str( ) );
      }

      if ( graphData[paradigm_index].firstNode == NULL )
      {
        graphData[paradigm_index].firstNode = node;
      }

      graphData[paradigm_index].lastNode = node;
    }
  }

  addNodeInternal( nodes, node );

  if ( nodeParadigm == PARADIGM_MPI )
  {
    GraphNode* lastLocalCompute = getLastNode( );
    node->setLinkLeft( lastLocalCompute );
    unlinkedMPINodes.push_back( node );
  }

  if ( node->isEnter( ) )
  {
    for ( SortedGraphNodeList::const_iterator iter =
            unlinkedMPINodes.begin( );
          iter != unlinkedMPINodes.end( ); ++iter )
    {
      ( *iter )->setLinkRight( node );
    }
    unlinkedMPINodes.clear( );
  }
}

void
EventStream::insertGraphNode( GraphNode*                  node,
                              GraphNode::ParadigmNodeMap& predNodes,
                              GraphNode::ParadigmNodeMap& nextNodes )
{
  if ( !lastNode || Node::compareLess( lastNode, node ) )
  {
    lastNode = node;
  }

  SortedGraphNodeList::iterator result = nodes.end( );
  for ( SortedGraphNodeList::iterator iter = nodes.begin( );
        iter != nodes.end( ); ++iter )
  {
    SortedGraphNodeList::iterator next = iter;
    ++next;

    if ( next == nodes.end( ) )
    {
      nodes.push_back( node );
      break;
    }

    if ( Node::compareLess( node, *next ) )
    {
      result = nodes.insert( next, node );
      break;
    }
  }

  SortedGraphNodeList::iterator current;

  for ( size_t paradigm = 1;
        paradigm < NODE_PARADIGM_INVALID;
        paradigm *= 2 )
  {
    /* find previous node */
    GraphNode* predNode = NULL;
    current = result;
    while ( current != nodes.begin( ) )
    {
      --current;
      if ( ( *current )->hasParadigm( (Paradigm)paradigm ) )
      {
        predNode = *current;
        break;
      }
    }

    if ( predNode )
    {
      predNodes.insert( std::make_pair( (Paradigm)paradigm, predNode ) );
    }
  }

  /* find next node */
  bool hasNextNode[NODE_PARADIGM_COUNT];

  for ( size_t paradigm = 1;
        paradigm < NODE_PARADIGM_INVALID;
        paradigm *= 2 )
  {
    current = result;
    SortedGraphNodeList::iterator next = ++current;
    size_t     paradigm_index          = (size_t)log2( paradigm );
    hasNextNode[paradigm_index] = false;

    GraphNode* nextNode = NULL;

    while ( next != nodes.end( ) )
    {
      if ( ( *next )->hasParadigm( (Paradigm)paradigm ) )
      {
        nextNode = *next;
        hasNextNode[paradigm_index] = true;
        break;
      }
      ++next;
    }

    if ( nextNode )
    {
      nextNodes.insert( std::make_pair( (Paradigm)paradigm, nextNode ) );
    }

    if ( node->hasParadigm( (Paradigm)paradigm ) )
    {
      if ( !graphData[paradigm_index].firstNode )
      {
        graphData[paradigm_index].firstNode = node;
      }

      if ( !hasNextNode[paradigm_index] )
      {
        graphData[paradigm_index].lastNode = node;
      }
    }
  }
}

EventStream::SortedGraphNodeList&
EventStream::getNodes( )
{
  return nodes;
}

void
EventStream::addPendingKernel( GraphNode* kernelLeave )
{
  pendingKernels.push_back( kernelLeave );
}

GraphNode*
EventStream::getPendingKernel( )
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin( );
  if ( iter != pendingKernels.rend( ) )
  {
    return *iter;
  }
  else
  {
    return NULL;
  }
}

GraphNode*
EventStream::consumePendingKernel( )
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin( );
  if ( iter != pendingKernels.rend( ) )
  {
    GraphNode* result = *iter;
    pendingKernels.pop_back( );
    return result;
  }

  return NULL;
}

void
EventStream::clearPendingKernels( )
{
  pendingKernels.clear( );
}

void
EventStream::setPendingMPIRecord( MPIType  mpiType,
                                  uint64_t partnerId,
                                  uint64_t rootId )
{
  MPICommRecord record;
  record.mpiType   = mpiType;
  record.partnerId = partnerId;
  record.rootId    = rootId;

  mpiCommRecords.push_back( record );
}

/**
 * Consume the pending (blocking) MPI records an retrieve a copy of the list.
 * The list should be cleared, when it is not needed any more.
 * 
 * @return a copy of all pending (blocking) MPI records
 */
EventStream::MPICommRecordList
EventStream::getPendingMPIRecords( )
{
  // create a copy of the current pending records
  MPICommRecordList copyList;
  copyList.assign( mpiCommRecords.begin( ), mpiCommRecords.end( ) );
  // the list is cleared in AnalysisParadigmMPI::handlePostLeave())
  
  // clear the pending list
  mpiCommRecords.clear( );
  return copyList;
}

/**
 * Temporarily save the MPI_Isend request that is consumed by MPI_Wait leave.
 * (Triggered by MPI_IsendComplete event.)
 * See {@link #setMPIWaitNodeData(GraphNode* node)}.
 * 
 * @param requestId OTF2 MPI_Isend request ID
 */
void
EventStream::saveMPIIsendRequest( uint64_t requestId )
{
 //std::cerr << "MPIIsend: mpiWaitRequest = " << requestId << std::endl;
  
  pendingRequests.push_back(requestId);
}

/**
 * Temporarily save the MPI_Irecv request ID. The following MPI_Irecv function 
 * leave record will consume and invalidate it. 
 * (Triggered by MPI_IrecvRequest event which is in between MPI_Irecv enter/leave.)
 * See {@link #addPendingMPIIrecvNode(GraphNode* node)}.
 * 
 * @param requestId OTF2 MPI_Irecv request ID
 */
void
EventStream::saveMPIIrecvRequest( uint64_t requestId )
{
  //std::cerr << "MPIIrecvRequest: mpiIrecvRequest = " << requestId << std::endl;
  pendingMPIRequestId = requestId;
}

/**
 * Store the MPI_Irecv leave node together with the MPI_Request handle. The 
 * MPI_Irecv record provides the communication partner ID and the MPI_request to 
 * put it all together. 
 * See {@link #setMPIIrecvPartnerStreamId(uint64_t requestId, uint64_t partnerId)}.
 * 
 * @param node the graph node of the MPI_Irecv leave record
 */
void
EventStream::addPendingMPIIrecvNode( GraphNode* node )
{
    UTILS_ASSERT( pendingMPIRequestId != UINT64_MAX,
                  "MPI_Irecv request ID invalid! Trace file might be corrupted!");
    
    MPIIcommRecord record;
    record.requests[0] = MPI_REQUEST_NULL;
    record.requests[1] = MPI_REQUEST_NULL;
    record.requestId = pendingMPIRequestId;
    record.leaveNode = node;
    /*
    MPIIcommRecord *record = new MPIIcommRecord;
    record->requests[0] = MPI_REQUEST_NULL;
    record->requests[1] = MPI_REQUEST_NULL;
    record->requestId = pendingMPIRequestId;
    record->leaveNode = node;
    *//*
    std::cerr << "[" << this->id << "] New MPI_Irecv record: " << record 
              << " Request ID: " << pendingMPIRequestId << std::endl;
    */
    // add new record to map
    mpiIcommRecords[pendingMPIRequestId] = record;
    
    // set node-specific data to a pointer to the record in the map
    node->setData( &mpiIcommRecords[pendingMPIRequestId] );
    //node->setData( record );
    
    //invalidate request ID variable
    pendingMPIRequestId = UINT64_MAX;
}

/**
 * Set partner stream ID for the given MPI_Irecv request ID.
 * The node is identified by the given request ID.
 * It saves the request ID to be consumed by the following MPI_Wait[all] leave node. 
 * Triggered by the MPI_Irecv record (between MPI_Wait[all] enter and leave).
 * 
 * @param requestId OTF2 MPI_Irecv request ID 
 * @param partnerId stream ID of the communication partner
 */
void
EventStream::handleMPIIrecvEventData( uint64_t requestId,
                                      uint64_t partnerId )
{
  //std::cerr << "MPIIrecv: mpiWaitRequest = " << requestId << std::endl;
 
  mpiIcommRecords[requestId].leaveNode->setReferencedStreamId(partnerId);
  //mpiIcommRecords[requestId]->leaveNode->setReferencedStreamId(partnerId);
  
  // temporarily store the request that is consumed by MPI_Wait[all] leave event
  pendingRequests.push_back(requestId);
}

/**
 * Temporarily store the request that is consumed by MPI_Isend leave event.
 * Triggered by MPI_Isend communication record, between MPI_Isend enter/leave.
 * 
 * @param partnerId stream ID of the communication partner
 * @param requestId OTF2 MPI_Isend request ID 
 */
void
EventStream::handleMPIIsendEventData( uint64_t requestId,
                                      uint64_t partnerId )
{
  pendingMPIRequestId = requestId;
  mpiIsendPartner = partnerId;
}

/**
 * Adds MPI_Isend request to a map and sets node-specific data. 
 * Consumes the pending OTF2 request ID and the MPI_Isend communication partner ID.
 * 
 * @param node the graph node of the MPI_Isend leave record
 */
void
EventStream::setMPIIsendNodeData( GraphNode* node )
{
  UTILS_ASSERT( pendingMPIRequestId != UINT64_MAX && mpiIsendPartner != UINT64_MAX, 
                "MPI request or MPI partner ID is invalid!");
 
  // add new record to map
  MPIIcommRecord record;
  record.requests[0] = MPI_REQUEST_NULL;
  record.requests[1] = MPI_REQUEST_NULL;
  record.leaveNode = node;
  record.requestId = pendingMPIRequestId;
  mpiIcommRecords[pendingMPIRequestId] = record;
  
  // set node-specific data to a pointer to the record in the map
  node->setData( &mpiIcommRecords[pendingMPIRequestId] );
  
  /*
  MPIIcommRecord *record = new MPIIcommRecord;
  record->requests[0] = MPI_REQUEST_NULL;
  record->requests[1] = MPI_REQUEST_NULL;
  record->leaveNode = node;
  record->requestId = pendingMPIRequestId;
  mpiIcommRecords[pendingMPIRequestId] = record;
  node->setData( record );
  */
  
  node->setReferencedStreamId( mpiIsendPartner ); 
  
  //invalidate temporary stored request and partner ID
  pendingMPIRequestId = UINT64_MAX;
  mpiIsendPartner = UINT64_MAX;
}

/**
 * Sets node-specific data for the given MPI_Wait leave node.
 * Consumes the pending OTF2 request ID.
 * 
 * @param node the graph node of the MPI_Isend leave record
 */
void
EventStream::setMPIWaitNodeData( GraphNode* node )
{
  UTILS_ASSERT( pendingRequests.size() == 1, 
                "List of pending OTF2 request IDs != 1.\n");

  // set OTF2 request ID as node-specific data
  // the request ID has to be already in the map from Irecv or Isend record
  node->setData( &mpiIcommRecords[ pendingRequests.back( ) ] );
  
  
  // !!! invalidate node-specific data when deleting this entry
  mpiIcommRecords[ pendingRequests.back( ) ].leaveNode = node;
  
  // request ID is consumed, therefore pop it from the vector
  pendingRequests.pop_back( );
}

/**
 * Consumes the pending OTF2 request IDs and sets the given node as 
 * associated operation.
 * 
 * @param node the graph node of the MPI_Waitall leave record
 */
void
EventStream::setMPIWaitallNodeData( GraphNode* node )
{
  UTILS_ASSERT( pendingRequests.size() > 0, 
                "List of pending OTF2 request IDs !> 0.\n");
  
  MPIIcommRequestList::const_iterator it = pendingRequests.begin();
  
  for( ; it != pendingRequests.end( ); ++it )
  {
    mpiIcommRecords[*it].leaveNode = node;
  }
  
  pendingRequests.clear( );
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
EventStream::waitForPendingMPIRequest( uint64_t requestId )
{ 
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();

  while ( it != mpiIcommRecords.end( ) )
  {
    if ( it->first == requestId )
    {
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
                     "[%u] Finish requests (%p) associated with OTF2 request ID %llu \n",
                     this->id, it->second, requestId);
      
      if( it->second.requests[0] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &(it->second.requests[0]), MPI_STATUS_IGNORE ) );
      }

      if( it->second.requests[1] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &(it->second.requests[1]), MPI_STATUS_IGNORE ) );
      }

      // invalidate node-specific data
      it->second.leaveNode->setData(NULL);
      
      // delete allocated memory
      //delete it->second;
      //it->second = 0;
      
      mpiIcommRecords.erase( it );

      return true;
    }
    else
      it++;
  }
  
  UTILS_MSG( true, 
             "[%u] OTF2 MPI request ID %llu could not be found. Has already completed?\n",
             this->id, requestId );

  return false;
}

/**
 * Wait for all pending MPI requests that are associated with the given node.
 * 
 * @param node the MPI_Waitall leave node
 * 
 * @return true, if the handle was found, otherwise false
 */
void
EventStream::waitForPendingMPIRequests( GraphNode* node )
{ 
  //std::vector < MPI_Request > tmpRequests;
 
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();

  while ( it != mpiIcommRecords.end( ) )
  {
    if ( it->second.leaveNode == node )
    {
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
                     "[%u] Finish requests (%p) associated with OTF2 request ID "
                     "%llu (in waitForPendingMPIRequests())\n",
                     this->id, it->second, it->second->requestId);
      
      if( it->second.requests[0] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &(it->second.requests[0]), MPI_STATUS_IGNORE ) );
      }

      if( it->second.requests[1] != MPI_REQUEST_NULL )
      {
        MPI_CHECK( MPI_Wait( &(it->second.requests[1]), MPI_STATUS_IGNORE ) );
      }

      // invalidate node-specific data
      it->second.leaveNode->setData( NULL );
      
      // delete allocated memory
      //delete it->second;
      //it->second = 0;
      
      mpiIcommRecords.erase( it++ );
    }
    else
      ++it;
  }
  
  // TODO: use an MPI_Waitall for performance reasons?
  /*if( tmpRequests.size () )
  {
    MPI_CHECK( MPI_Waitall( tmpRequests.size(), &tmpRequests[0], MPI_STATUSES_IGNORE ) );
  }*/
}

/**
 * Analysis rules for non-blocking MPI communication:
 * 
 * Wait for open MPI_Request handles. Should be called before MPI_Finalize().
 */
void
EventStream::waitForAllPendingMPIRequests( )
{  
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();
  
  UTILS_MSG( mpiIcommRecords.size() > 0,
             "[%llu] Number of pending MPI request handles at MPI_Finalize: %lu \n", 
             this->id, mpiIcommRecords.size() );

  for (; it != mpiIcommRecords.end( ); ++it )
  {
    MPI_Request request = it->second.requests[0];
    
    if( MPI_REQUEST_NULL != request )
      MPI_CHECK( MPI_Wait( &request, MPI_STATUS_IGNORE ) );
    
    request = it->second.requests[1];
    
    if( MPI_REQUEST_NULL != request )
      MPI_CHECK( MPI_Wait( &request, MPI_STATUS_IGNORE ) );
    
    // invalidate node-specific data
    it->second.leaveNode->setData( NULL );
    //delete it->second; 
    //it->second = 0;
  }

  // clear the map of pending non-blocking MPI operations
  mpiIcommRecords.clear(); // invalidates all references and pointers for this container
}

/**
 * Analysis rules for non-blocking MPI communication:
 * 
 * Test for completed MPI_Request handles. Can be used to decrease the number of 
 * open MPI request handles, e.g. at blocking collective operations.
 * This might improve the performance of the MPI implementation. 
 */
void
EventStream::testAllPendingMPIRequests( )
{
  
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();
  
  while ( it != mpiIcommRecords.end( ) )
  {
    MPI_Status status;
    int finished[2] = {0,0};

    if( MPI_REQUEST_NULL != it->second.requests[0] )
      MPI_CHECK( MPI_Test( &(it->second.requests[0]), &(finished[0]), &status ) );
    
    if( MPI_REQUEST_NULL != it->second.requests[1] )
      MPI_CHECK( MPI_Test( &(it->second.requests[1]), &(finished[1]), &status ) );
    
    //if both MPI_Irecv and MPI_Isend are finished, we can delete the record
    if( finished[0] && finished[1] )
    {
      
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM, "[%u] Finished requests (%p) with OTF2 request ID"
                                      " %llu in testAllPendingMPIRequests()\n", 
                                      this->id,
                                      (void *) it->second,
                                      it->second.requestId);
      
      // invalidate node-specific data
      it->second.leaveNode->setData( NULL );
      
      //delete it->second;
      //it->second = NULL;
      
      mpiIcommRecords.erase(it++);
    }
    else
    {
      if( finished[0] )
        it->second.requests[0] = MPI_REQUEST_NULL;
    
      if( finished[1] )
        it->second.requests[1] = MPI_REQUEST_NULL;
      
      ++it;
    }
  }
}

bool
EventStream::walkBackward( GraphNode*         node,
                           StreamWalkCallback callback,
                           void*              userData )
{
  bool result = false;

  if ( !node || !callback )
  {
    return result;
  }

  SortedGraphNodeList::const_reverse_iterator iter = findNode( node );
  UTILS_ASSERT( *iter == node, "no %s in stream %lu",
                node->getUniqueName( ).c_str( ), node->getStreamId( ) );

  for (; iter != nodes.rend( ); ++iter )
  {
    result = callback( userData, *iter );
    if ( result == false )
    {
      return result;
    }
  }

  return result;
}

bool
EventStream::walkForward( GraphNode*         node,
                          StreamWalkCallback callback,
                          void*              userData )
{
  bool result = false;

  if ( !node || !callback )
  {
    return result;
  }

  SortedGraphNodeList::const_reverse_iterator iter_tmp = findNode( node );
  SortedGraphNodeList::const_iterator iter = iter_tmp.base( );

  for (; iter != nodes.end( ); ++iter )
  {
    result = callback( userData, *iter );
    if ( result == false )
    {
      return result;
    }
  }

  return result;
}

EventStream::SortedGraphNodeList::const_reverse_iterator
EventStream::findNode( GraphNode* node ) const
{
  if ( nodes.size( ) == 0 )
  {
    return nodes.rend( );
  }

  if ( nodes.size( ) == 1 )
  {
    return nodes.rbegin( );
  }

  size_t indexMin = 0;
  size_t indexMax = nodes.size( ) - 1;

  do
  {
    size_t index = indexMax - ( indexMax - indexMin ) / 2;

    UTILS_ASSERT( index < nodes.size( ), "index %lu indexMax %lu indexMin %lu", index, indexMax, indexMin );

    if ( nodes[index] == node )
    {
      return nodes.rbegin( ) + ( nodes.size( ) - index - 1 );
    }

    if ( indexMin == indexMax )
    {
      return nodes.rend( );
    }

    if ( Node::compareLess( node, nodes[index] ) )
    {
      /* left side */
      indexMax = index - 1;
    }
    else
    {
      /* right side */
      indexMin = index + 1;
    }

    if ( indexMin > indexMax )
    {
      break;
    }

  }
  while ( true );

  return nodes.rend( );
}

void
EventStream::addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node )
{
  nodes.push_back( node );

  lastNode = node;
}
