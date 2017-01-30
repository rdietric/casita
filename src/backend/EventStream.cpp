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
 * What this file does:
 * - Basic interaction with an event stream: add/remove/insert nodes, getter, 
 *   get Attributes about event stream
 * - walk forward/backward through stream (and call callback for each node on that walk)
 * - manage pending/consuming kernels (CUDA)
 * - manage pending MPIRecords
 *
 */

#include <limits>

#include "EventStream.hpp"
#include "utils/ErrorUtils.hpp"


using namespace casita;

EventStream::EventStream( uint64_t          id,
                          uint64_t          parentId,
                          const std::string name,
                          EventStreamType   eventStreamType ) :
  id( id ),
  parentId( parentId ),
  name( name ),
  streamType( eventStreamType ),
  nodesAdded( false ),
  deviceId ( -1 ),
  hasFirstCriticalNode( false ),
  hasLastEvent( false ),
  lastNode( NULL ),
  lastEventTime( 0 ),
  isFilterOn( false ),
  predictionOffset ( 0 ),
  pendingMPIRequestId( std::numeric_limits< uint64_t >::max() ),
  mpiIsendPartner( std::numeric_limits< uint64_t >::max() )
{
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    graphData[i].firstNode = NULL;
    graphData[i].lastNode  = NULL;
  }
  
  // set the initial values for first enter and last leave
  streamPeriod.first = std::numeric_limits< uint64_t >::max();
  streamPeriod.second = 0;
}

EventStream::~EventStream()
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
  return streamType & ( ES_HOST | ES_HOST_MASTER );
}

bool
EventStream::isHostMasterStream( ) const
{
  return streamType & ES_HOST_MASTER;
}

bool
EventStream::isDeviceStream( ) const
{
  return streamType & ( ES_DEVICE | ES_DEVICE_NULL );
}

bool
EventStream::isDeviceNullStream() const
{
  return streamType & ES_DEVICE_NULL;
}

void
EventStream::setDeviceId( int deviceId )
{
  this->deviceId = deviceId;
}

/**
 * Obtain the device ID that has been parsed from the name of the stream. It is
 * -1 if unknown.
 * 
 * @return the device ID or -1 if unknown
 */
int
EventStream::getDeviceId() const
{
  return deviceId;
}

/**
 * Get the stream's first enter and last leave time stamps
 * 
 * @return a pair the first enter and last leave time stamp
 */
std::pair< uint64_t, uint64_t >&
EventStream::getPeriod()
{
  return streamPeriod;
}

/**
 * Does this stream contains the global first critical node?
 * 
 * @return true, if the critical path starts on this stream
 */
bool&
EventStream::isFirstCritical( )
{
  return hasFirstCriticalNode;
}

/**
 * Does this stream contains the global last event (of the trace)?
 * 
 * @return true, if the critical path ends on this stream
 */
bool&
EventStream::hasLastGlobalEvent( )
{
  return hasLastEvent;
}

GraphNode*
EventStream::getLastNode( ) const
{
  return lastNode;
  //return getLastNode( PARADIGM_ALL );
}

GraphNode*
EventStream::getLastNode( Paradigm paradigm ) const
{
  size_t     i           = 0;
  GraphNode* tmpLastNode = NULL;

  // for all paradigms
  for ( i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    Paradigm tmpP = (Paradigm)( 1 << i );
    
    // if the last node for a given paradigm is set
    if ( ( tmpP & paradigm ) && graphData[i].lastNode )
    {
      tmpLastNode = graphData[i].lastNode;
      break;
    }
  }
  
  // if we found a node, iterate over the other paradigms to find a later node
  i++;
  for (; i < NODE_PARADIGM_COUNT; ++i )
  {
    Paradigm tmpP = (Paradigm)( 1 << i );
    
    // if the last node for a given paradigm is set AND it is later than the 
    // already found node
    if ( ( tmpP & paradigm ) && graphData[i].lastNode &&
         Node::compareLess( tmpLastNode, graphData[i].lastNode ) )
    {
      tmpLastNode = graphData[i].lastNode;
    }
  }

  return tmpLastNode;
}

/**
 * Get the first node of a given paradigm.
 * \todo: this works only for single paradigms and NOT for e.g. PARADIGM_ALL
 * 
 * @param paradigm
 * @return 
 */
GraphNode*
EventStream::getFirstNode( Paradigm paradigm ) const
{
  return graphData[(int)log2( paradigm )].firstNode;
}

void
EventStream::setLastEventTime( uint64_t time )
{
  lastEventTime = time;
}

uint64_t
EventStream::getLastEventTime( ) const
{
  if ( lastEventTime > streamPeriod.second )
  {
    return lastEventTime;
  }
  else
  {
    return streamPeriod.second;
  }
}

void
EventStream::addGraphNode( GraphNode*                  node,
                           GraphNode::ParadigmNodeMap* predNodes )
{
  // set changed flag
  nodesAdded = true;
  
  GraphNode* lastLocalNode = getLastNode();
  
  GraphNode* oldNode[NODE_PARADIGM_COUNT];
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    oldNode[i] = NULL;
  }
  Paradigm   nodeParadigm = node->getParadigm();

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
                node->getUniqueName().c_str(),
                oldNode[paradigm_index]->getUniqueName().c_str() );
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
    // \todo: This was probably wrong. Therefore, took the last node before changing the graph data.
    //GraphNode* lastLocalCompute = getLastNode( );

    //std::cerr << "[" << this->id << "] " << node->getUniqueName() 
    //          << "setLinkLeft: " << lastLocalNode->getUniqueName() << std::endl;
    node->setLinkLeft( lastLocalNode );
    
    // save MPI nodes as they do not have a right link yet
    unlinkedMPINodes.push_back( node );
  }

  
  if ( node->isEnter( ) )
  {
    for ( SortedGraphNodeList::const_iterator iter =
            unlinkedMPINodes.begin( );
          iter != unlinkedMPINodes.end( ); ++iter )
    {
      //std::cerr << "XXXXXRight  " << ( *iter )->getUniqueName() << " -> " << node->getUniqueName() << std::endl;
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
  // set changed flag
  nodesAdded = true;
  
  // set the last-node field
  if ( !lastNode || Node::compareLess( lastNode, node ) )
  {
    lastNode = node;
  }

  // add the node to the sorted nodes list
  SortedGraphNodeList::iterator result = nodes.end( );
  for ( SortedGraphNodeList::iterator iter = nodes.begin( );
        iter != nodes.end( ); ++iter )
  {
    SortedGraphNodeList::iterator next = iter;
    ++next;

    // if next is end of list, then push the node at the end of the vector
    if ( next == nodes.end( ) )
    {      
      nodes.push_back( node );
      break;
    }

    // current node is "before" the next element
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

/**
 * Did the stream change (new nodes added) since the interval start?
 * 
 * @return true, if nodes have been added, otherwise false
 */
bool
EventStream::hasNewNodes( )
{
  return nodesAdded;
}

EventStream::SortedGraphNodeList&
EventStream::getNodes( )
{
  return nodes;
}

void
EventStream::clearNodes( )
{
  // clear the nodes list (do not delete the nodes themselves)
  nodes.clear( );
  
  // set the first and last Node to NULL
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    graphData[i].firstNode = NULL;
    graphData[i].lastNode  = NULL;
  }
  
  lastNode = NULL;
}

void 
EventStream::setFilter( bool enable )
{
  isFilterOn = enable;
}

void
EventStream::addPendingKernel( GraphNode* kernelLeave )
{
  pendingKernels.push_back( kernelLeave );
  //std::cerr << "["<< this->id << "] Add pending kernel: " << kernelLeave->getUniqueName() << std::endl;
}

/**
 * Retrieve the last pending kernel leave in the vector.
 * 
 * @return first pending kernel (leave) in the vector
 */
GraphNode*
EventStream::getLastPendingKernel()
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
  if ( iter != pendingKernels.rend() )
  {
    return *iter;
  }
  
  return NULL;
}

/**
 * Remove the last pending kernel from the list.
 * 
 * @return the kernel leave node that has been removed
 */
GraphNode*
EventStream::consumeLastPendingKernel()
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
  if ( iter != pendingKernels.rend() )
  {
    GraphNode* result = *iter;
    pendingKernels.pop_back();
    return result;
  }

  return NULL;
}

/**
 * Consume all pending kernels before the given node.
 * 
 * @kernelLeave the kernel leave node
 */
void
EventStream::consumePendingKernels( GraphNode* kernelLeave )
{
  if( !kernelLeave )
  {
    UTILS_WARNING( "Cannot consume pending kernels as input node is invalid!" );
    return;
  }
  
  // do nothing, if there are no pending kernels
  if( pendingKernels.empty() )
    return;
  
  // frequent case: kernel is the last one in the list
  GraphNode*  lastKernel = pendingKernels.back();
  if( lastKernel == kernelLeave )
  {
    clearPendingKernels();
  }

  // erase a range of kernels
  SortedGraphNodeList::iterator iterBegin = pendingKernels.begin();
  SortedGraphNodeList::iterator iter = iterBegin;
  while( iter != pendingKernels.end() )
  {
    if( ( *iter ) == kernelLeave )
    {
      break;
    }
      
    ++iter;
  }
  
  pendingKernels.erase( iterBegin, iter );
}

void
EventStream::clearPendingKernels()
{
  pendingKernels.clear();
}

void
EventStream::setPendingKernelsSyncLink( GraphNode* syncLeave )
{
  for( SortedGraphNodeList::iterator it = pendingKernels.begin( );
       it != pendingKernels.end(); ++it )
  {
    (*it)->setLink( syncLeave );
  }
}

/**
 * 
 * @param mpiType
 * @param partnerId
 * @param rootId root MPI rank of the ONEANDALL collective
 */
void
EventStream::setPendingMPIRecord( MPIType  mpiType,
                                  uint64_t partnerId,
                                  uint64_t rootId )
{
  MPICommRecord record;
  record.mpiType   = mpiType;
  record.partnerId = partnerId; // the communicator for collectives
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
  //UTILS_MSG( true, "[%"PRIu64"] Save MPIIrecvRequest %"PRIu64, this->id, requestId);
  
  pendingMPIRequestId = requestId;
}

/**
 * Store the MPI_Irecv leave node together with the MPI_Request handle. The 
 * MPI_Irecv record provides the communication partner ID and the MPI_request to 
 * put it all together. 
 * See {@link #handleMPIIrecvEventData(uint64_t requestId, uint64_t partnerId)}.
 * 
 * @param node the graph node of the MPI_Irecv leave record
 */
void
EventStream::addPendingMPIIrecvNode( GraphNode* node )
{
//    UTILS_ASSERT( pendingMPIRequestId != std::numeric_limits< uint64_t >::max(),
//                  "MPI_Irecv request ID invalid! Trace file might be corrupted!");
    
    if( pendingMPIRequestId == std::numeric_limits< uint64_t >::max() )
    {
      UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE,
                 "%s: no request ID. Trace file might be corrupted!", 
                 node->getUniqueName().c_str() );
      
      // "inform" MPI_Irecv rule that this node is invalid
      node->setData( NULL );
      
      return;
    }
    
    MPIIcommRecord record;
    record.requests[0] = MPI_REQUEST_NULL;
    record.requests[1] = MPI_REQUEST_NULL;
    record.requestId = pendingMPIRequestId;
    record.leaveNode = node;

    //UTILS_MSG( true, "[%"PRIu64"] New MPI_Irecv record: %s Request ID: %"PRIu64,
    //           this->id, node->getUniqueName().c_str(), pendingMPIRequestId );
    
    // add new record to map
    mpiIcommRecords[pendingMPIRequestId] = record;
    
    // set node-specific data to a pointer to the record in the map
    node->setData( &mpiIcommRecords[pendingMPIRequestId] );

    //invalidate request ID variable
    pendingMPIRequestId = std::numeric_limits< uint64_t >::max( );
}

/**
 * Set partner stream ID for the given MPI_Irecv request ID.
 * The node is identified by the given request ID.
 * It saves the request ID to be consumed by the following MPI_Wait[all] or 
 * MPI_Test[all] leave node. 
 * Triggered by the MPI_Irecv record (between MPI_Wait[all] enter and leave).
 * 
 * @param requestId OTF2 MPI_Irecv request ID 
 * @param partnerId stream ID of the communication partner
 */
void
EventStream::handleMPIIrecvEventData( uint64_t requestId,
                                      uint64_t partnerId )
{
  //UTILS_MSG( true, "[%"PRIu64"] MPIIrecv: mpiWaitRequest = %"PRIu64, 
  //                 this->id, requestId );
  
  if( mpiIcommRecords.count( requestId ) > 0 )
  {
    mpiIcommRecords[requestId].leaveNode->setReferencedStreamId( partnerId );

    // temporarily store the request that is consumed by MPI_Wait[all] leave event
    pendingRequests.push_back( requestId );
  }
  else
  {
    // if non-blocking communication over interval boundaries occurs it would
    // probably not influence the critical path or generate waiting time or blame
    UTILS_MSG( true, "[%"PRIu64"<-%"PRIu64"] Ignore MPI_Irecv communication "
                     "over interval boundaries. (OTF2 request: %"PRIu64")", 
                     this->id, partnerId, requestId );
  }
}

/**
 * Temporarily store the request that is consumed by MPI_Isend leave event.
 * Triggered by MPI_Isend communication record, between MPI_Isend enter/leave.
 * 
 * This function is called while handling OTF2 MPI_ISEND record. 
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
 * Adds MPI_Isend request to a map and sets node-specific data. Consumes the 
 * pending OTF2 request ID and the MPI_Isend communication partner ID, which 
 * have been stored in handleMPIIsendEventData().
 * 
 * This function is called when handling the MPI_ISEND (region) leave record.
 * 
 * @param node the graph node of the MPI_Isend leave record
 */
void
EventStream::setMPIIsendNodeData( GraphNode* node )
{
//  UTILS_ASSERT( pendingMPIRequestId != std::numeric_limits< uint64_t >::max() 
//                 && mpiIsendPartner != std::numeric_limits< uint64_t >::max(), 
//                "[%"PRIu64"] %s MPI request %"PRIu64" or MPI partner ID %"PRIu64
//                " is invalid", this->id, node->getUniqueName().c_str(),
//                pendingMPIRequestId, mpiIsendPartner );
  
  if( pendingMPIRequestId == std::numeric_limits< uint64_t >::max() ||
      mpiIsendPartner == std::numeric_limits< uint64_t >::max() )
  {
    if( pendingMPIRequestId == mpiIsendPartner )
    {
      UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE,
                 "%s: Neither MPI request ID nor partner available. "
                 "Corrupted trace file?", node->getUniqueName().c_str() );
    }
    else
    {
      UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE,
                 "%s: no MPI request ID (%"PRIu64") or partner (%"PRIu64"). "
                 "Corrupted trace file?", node->getUniqueName().c_str(), 
                 pendingMPIRequestId, mpiIsendPartner );
    }

    // "inform" MPI_Isend rule that this node is invalid
    node->setData( NULL );
    
    return;
  }
 
  // add new record to map
  MPIIcommRecord record;
  record.requests[0] = MPI_REQUEST_NULL;
  record.requests[1] = MPI_REQUEST_NULL;
  record.leaveNode = node;
  record.requestId = pendingMPIRequestId;
  mpiIcommRecords[pendingMPIRequestId] = record;
  
  //UTILS_MSG( true, "[%"PRIu64"] New MPI_Isend record: %s Request ID: %"PRIu64,
  //           this->id, node->getUniqueName().c_str(), pendingMPIRequestId );
  
  // set node-specific data to a pointer to the record in the map
  node->setData( &mpiIcommRecords[pendingMPIRequestId] );
  
  node->setReferencedStreamId( mpiIsendPartner ); 
  
  //invalidate temporary stored request and partner ID
  pendingMPIRequestId = std::numeric_limits< uint64_t >::max();
  mpiIsendPartner = std::numeric_limits< uint64_t >::max();
}

/**
 * Sets node-specific data for the given MPI_Wait leave node.
 * Consumes the pending OTF2 request ID. Both, MPI_Wait and MPI_Test can
 * complete a non-blocking MPI communication. In an OTF2 trace, completed 
 * communication operations are between ENTER and LEAVE of MPI_Wait[all],
 * MPI_Test[all].
 * 
 * If there are no pending request, no communication operation has completed 
 * here.
 * 
 * @param node the graph node of the MPI_Wait leave record
 */
void
EventStream::setMPIWaitNodeData( GraphNode* node )
{  
  if( pendingRequests.size( ) == 1 )
  {
    uint64_t pendingReqId = pendingRequests.back();
    
    // the request ID has to be already in the map from Irecv or Isend record
    if( mpiIcommRecords.count( pendingReqId ) >0 )
    {
      // set MPIIcommRecord data as node-specific data
      node->setData( &mpiIcommRecords[ pendingReqId ] );

      // request ID is consumed, therefore pop it from the vector
      pendingRequests.pop_back();
    }
    else
    {
      UTILS_MSG( true, "[%"PRIu64"] MPI_Wait node: Could not find communication"
                 " record with request ID %"PRIu64" and communication partner %"
                 PRIu64, this->id, pendingReqId );
    }
  }
  else if( pendingRequests.size( ) == 0 )
  {
    // assign waiting time to this node, as it is unnecessary
    node->setCounter( WAITING_TIME, 
                      node->getTime() - node->getGraphPair().first->getTime() );
  }
  else
  {
    UTILS_MSG( true, "List of pending OTF2 request IDs > 1 (#%llu) at %s", 
               pendingRequests.size( ), node->getUniqueName().c_str() );
  }
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
  //UTILS_ASSERT( pendingRequests.size() > 0, 
  //              "List of pending OTF2 request IDs !> 0.\n");
  
  if( pendingRequests.size() > 0 )
  {
    // create a copy of the pending requests
    MPIIcommRequestList* copyList = new MPIIcommRequestList( pendingRequests );
    node->setData( copyList );

    pendingRequests.clear();
  }
  else
  {
    // assign waiting time to this node, as it is unnecessary
    node->setCounter( WAITING_TIME, 
                      node->getTime() - node->getGraphPair().first->getTime() );
    
    // make sure the WaitAllRule is not triggered
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
EventStream::handleMPITest( GraphNode* node )
{  
  if( pendingRequests.size( ) == 1 )
  {
    // remove the record
    mpiIcommRecords.erase( pendingRequests.back() );

    // request ID is consumed, therefore pop it from the vector
    pendingRequests.pop_back( );
  }
  else if( pendingRequests.size( ) == 0 )
  {
    // assign waiting time to this node, as it is unnecessary
    node->setCounter( WAITING_TIME, 
                      node->getTime() - node->getGraphPair().first->getTime() );
  }
  else
  {
    UTILS_MSG( true, "List of pending OTF2 request IDs > 1 (#%llu) at %s", 
               pendingRequests.size( ), node->getUniqueName().c_str() );
  }
}

/**
 * Consumes the pending OTF2 request IDs and removes the associated 
 * communication records.
 * 
 * @param node the graph node of the MPI_Testall leave record
 */
void
EventStream::handleMPITestall( GraphNode* node )
{  
  if( pendingRequests.size() > 0 )
  {
    // iterate over all associated requests
    EventStream::MPIIcommRequestList::const_iterator it = pendingRequests.begin();
    for( ; it != pendingRequests.end(); ++it )
    {
      // remove associated record, as data is not used
      removePendingMPIRequest( *it );
    }
    
    pendingRequests.clear();
  }
  else
  {
    // assign waiting time to this node, as it is unnecessary
    node->setCounter( WAITING_TIME, 
                      node->getTime() - node->getGraphPair().first->getTime() );
  }
}

/**
 * Return whether we have pending MPI requests or not.
 * 
 * @return true, if we have pending MPI requests in the list.
 */
bool
EventStream::havePendingMPIRequests( )
{
  return !(mpiIcommRecords.empty());
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
                     "[%"PRIu64"] Finish requests (%p) associated with OTF2 "
                     "request ID %llu \n", this->id, it->second, requestId);
      
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
      
      mpiIcommRecords.erase( it );

      return true;
    }
    else
      it++;
  }
  
  UTILS_MSG( true, "[%"PRIu64"] OTF2 MPI request ID %"PRIu64" could not be found."
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
EventStream::MPIIcommRecord*
EventStream::getPendingMPIIcommRecord( uint64_t requestId )
{
  // invalidate node-specific data for the MPI_Isend or MPI_Irecv
  try {
    return &( mpiIcommRecords.at(requestId) );
  }
  catch (const std::out_of_range& oor) 
  {
    UTILS_MSG( true, 
               "[%"PRIu64"] OTF2 MPI request ID %"PRIu64" could not be found. "
               "Has already completed? (%s)", this->id, requestId, oor.what() );
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
EventStream::removePendingMPIRequest( uint64_t requestId )
{ 
  // "Because all elements in a map container are unique, count can only 
  //  return 1 (if the element is found) or zero (otherwise)."
  if( mpiIcommRecords.count( requestId ) > 0 )
  {
    // invalidate node-specific data for the MPI_Isend or MPI_Irecv
    mpiIcommRecords[ requestId ].leaveNode->setData(NULL);
    mpiIcommRecords.erase( requestId );
  }
  else
  {
    UTILS_MSG( true, "[%"PRIu64"] OTF2 MPI request ID %"PRIu64" could not be "
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
EventStream::waitForPendingMPIRequests( GraphNode* node )
{ 
  //std::vector < MPI_Request > tmpRequests;
 
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();

  while ( it != mpiIcommRecords.end( ) )
  {
    if ( it->second.leaveNode == node )
    {
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM,
                     "[%"PRIu64"] Finish requests (%p) associated with OTF2 "
                     "request ID %"PRIu64" in waitForPendingMPIRequests()",
                     this->id, it->second, it->second.requestId );
      
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
 * Wait for open MPI_Request handles. 
 */
void
EventStream::waitForAllPendingMPIRequests( )
{  
  MPIIcommRecordMap::iterator it = mpiIcommRecords.begin();
  
  UTILS_MSG( mpiIcommRecords.size() > 0,
             "[%"PRIu64"] Number of pending MPI request handles at "
             "MPI_Finalize: %lu", this->id, mpiIcommRecords.size() );

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
  }

  // clear the map of pending non-blocking MPI operations
  mpiIcommRecords.clear(); // invalidates all references and pointers for this container
}

/**
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
      
      UTILS_DBG_MSG( DEBUG_MPI_ICOMM, 
                     "[%"PRIu64"] Finished requests (%p) with OTF2 request ID"
                     " %"PRIu64" in testAllPendingMPIRequests()\n", 
                     this->id, it->second, it->second.requestId);
      
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

/**
 * Walk backwards from the given node. The StreamWalkCallback identifies the end
 * of the walk back.
 * 
 * @param node start node of the back walk
 * @param callback callback function that detects the end of the walk and 
 *                 adds userData on the walk
 * @param userData StreamWalkInfo that contains a node list and the list waiting 
 *                 time
 * 
 * @return true, if the walk back is successful, otherwise false.
 */
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
  
  // print a warning if the node could not be found and use a sequential search
  if ( *iter != node ) 
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_TIME, 
               "Binary search did not find %s in stream %lu. "
               "Perform sequential search for convenience ...", 
               node->getUniqueName( ).c_str( ), node->getStreamId( ) );
    iter = find( nodes.rbegin(), nodes.rend(), node );
  }
  
  // make sure that we found a node
  UTILS_ASSERT( *iter == node, "no %s in stream %lu",
                node->getUniqueName( ).c_str( ), node->getStreamId( ) );

  // iterate backwards over the list of nodes
  for (; iter != nodes.rend( ); ++iter )
  {
    // stop iterating (and adding nodes to the list and increasing waiting time) 
    // when e.g. MPI leave node found
    result = callback( userData, *iter );
    if ( result == false )
    {
      return false;
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

  // iterate forward over the list of nodes
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

// TODO: This function might not be correct implemented.
EventStream::SortedGraphNodeList::const_reverse_iterator
EventStream::findNode( GraphNode* node ) const
{
  // the vector is empty
  if ( nodes.size( ) == 0 )
  {
    return nodes.rend( );
  }

  // there is only one node in the vector
  if ( nodes.size( ) == 1 )
  {
    return nodes.rbegin( );
  }

  // set start boundaries for the search
  size_t indexMin = 0;
  size_t indexMax = nodes.size( ) - 1;
  
  size_t indexPrevMin = indexMin;
  size_t indexPrevMax = indexMax;
  
  size_t indexPrev = 0;
  size_t indexPrev2 = 0;

  // do a binary search
  do
  {
    indexPrev2 = indexPrev;
    indexPrev = indexPrevMax - ( indexPrevMax - indexPrevMin ) / 2;
    size_t index = indexMax - ( indexMax - indexMin ) / 2;

    UTILS_ASSERT( index < nodes.size( ), "index %lu indexMax %lu indexMin %lu", 
                  index, indexMax, indexMin );

    // if we found the node at index ('middle' element)
    // for uneven elements, index points on the element after the half
    if ( nodes[index] == node )
    {
      return nodes.rbegin( ) + ( nodes.size( ) - index - 1 );
    }

    // indexMin == indexMax == index
    // only the nodes[index] element was left, which did not match
    // we can leave the loop
    if ( indexMin == indexMax )
    {
      std::cerr << "Stream " << node->getStreamId() << " Looking for node " 
                << node->getUniqueName( ) << " - Wrong node found! Index (" 
                << index << ") node on break: "
                << nodes[index]->getUniqueName( ) << std::endl;

      std::cerr << "Node sequence:" << std::endl;
      for(size_t i = index - 3; i < index + 4; i++)
      {
        if( nodes[i] )
          std::cerr << nodes[i]->getUniqueName( ) << std::endl;
      }
      
      std::cerr << " Previous compare node [" << indexPrevMin << ":" << indexPrevMax 
                << "]:" << nodes[indexPrev]->getUniqueName( )
                << " with result: " << Node::compareLess( node, nodes[indexPrev] ) 
                << std::endl;
      
      std::cerr << " Pre-Previous compare node: " << nodes[indexPrev2]->getUniqueName( )
                << " with result: " << Node::compareLess( node, nodes[indexPrev2] ) 
                << std::endl;
      //std::cerr << "return nodes.rbegin( ) = " << nodes.rbegin( ) << std::endl;
      //std::cerr << "return nodes.rend( ) = " << nodes.rend( ) << std::endl;
      
      break;
    }

    // use the sorted property of the list to halve the search space
    // if node is before (less) than the node at current index
    // nodes are not the same
    if ( Node::compareLess( node, nodes[index] ) )
    {
      // left side
      indexPrevMax = indexMax;
      indexMax = index - 1;
    }
    else
    {
      // right side
      indexPrevMin = indexMin;
      indexMin = index + 1;
    }

    // if node could not be found
    if ( indexMin > indexMax )
    {
      break;
    }

  }
  while ( true );

  // return iterator to first element, if node could not be found
  return nodes.rend( );
}

void
EventStream::addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node )
{
  nodes.push_back( node );

  lastNode = node;
}

/**
 * Reset stream internal data structures.
 * The routine does not touch the list of nodes!!!
 */
void
EventStream::reset( )
{
  nodesAdded = false;
  
  // Check pending (unsynchronized) CUDA kernels
  if( !(this->pendingKernels.empty()) && Parser::getVerboseLevel() > VERBOSE_BASIC )
  {
    UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
               "[%"PRIu64"] %lz pending kernels found!", 
                     this->id, this->pendingKernels.size() );
    
    if( Parser::getVerboseLevel() > VERBOSE_BASIC )
    {
      for( SortedGraphNodeList::const_iterator it = pendingKernels.begin();
           it != pendingKernels.end(); ++it )
      {
        UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                   "   %s", ( *it )->getUniqueName().c_str() );
        
        //if( !isKernelPending )
        //pendingKernels.erase(  )
      }
    }
    
    // do not clear pending kernels as they might be required in the following interval
    //clearPendingKernels( );
  }
  
  //\todo nodes // currently handled in GraphEngine::createIntermediateBegin( )
  //\todo reset graphData //currently handled GraphEngine::createIntermediateBegin( )
  
  // clear list of unlinked MPI nodes (print to stderr before), the last node is always unlinked!
  if( unlinkedMPINodes.size() > 1 )
  {
    UTILS_MSG( true, "[%"PRIu64"] Clear list of unlinked MPI nodes (%lu)!", 
                     this->id, this->unlinkedMPINodes.size() );
    
    for ( SortedGraphNodeList::const_iterator iter =
            unlinkedMPINodes.begin( ); iter != unlinkedMPINodes.end( ); ++iter )
    {
      UTILS_MSG( true, "[%"PRIu64"]   %s", 
                       this->id, ( *iter )->getUniqueName().c_str() );
    }
    
    unlinkedMPINodes.clear();
  }
  
  // clear list of pending MPI blocking communication records
  mpiCommRecords.clear();
  
  // reset temporary values for non-blocking MPI communication
  pendingMPIRequestId = std::numeric_limits< uint64_t >::max( );
  mpiIsendPartner = std::numeric_limits< uint64_t >::max( );
    
  // reset list of pending request IDs (non-blocking MPI)
  if( !(pendingRequests.empty()) )
  {
    UTILS_MSG( true, "[%"PRIu64"] Clear list of pending OTF2 requests (%lu)!", 
                     this->id, this->pendingRequests.size() );
    pendingRequests.clear();
  }
  
  // clear list of pending non-blocking MPI communication records
  if( !(mpiIcommRecords.empty()) )
  {
    UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE, 
               "[%"PRIu64"] Clear list of pending non-blocking MPI communication "
               "records (%lu)!", this->id, this->mpiIcommRecords.size() );
    
    mpiIcommRecords.clear();
  }
}
