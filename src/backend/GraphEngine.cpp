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
 * - provide interaction with Graph: create/delete nodes/Edges/streams
 * - Streams are event streams within a process/thread (OMP,CUDA). 
 * - Nodes are stored in the graph (per process) and in each event stream (as a pointer)
 * - For every stream there is a start node, additionally, there is a global start node per MPI rank
 * - CPU Data is aggregated during reading of OTF (=graph creation) and added 
 *   to edges between non-CPU events
 * - Sanity-check (not sure if that's used anywhere in the program -> Deprecated)
 * -
 *
 */

#include <stdio.h>
#include <utility>
#include <cstdlib>

#include "GraphEngine.hpp"
#include "FunctionTable.hpp"
#include "common.hpp"
#include "graph/Node.hpp"

using namespace casita;
using namespace casita::io;

GraphEngine::GraphEngine( ) :
  ticksPerSecond( 1000 )
{
  globalSourceNode = newGraphNode( 0, 0, "START", PARADIGM_ALL, RECORD_ATOMIC,
                                   MISC_PROCESS );

}

/**
 * Destructor of GraphEngine is currently called at the end of the program.
 * Therefore, do not clean up anything.
 */
GraphEngine::~GraphEngine( )
{
  /*UTILS_MSG( true, "Destructor of GraphEngine called" );
  
  for ( EventStreamMap::iterator iter = streamsMap.begin();
        iter != streamsMap.end( ); ++iter )
  {
    delete iter->second;
  }*/
}

EventStream*
GraphEngine::newEventStream( uint64_t                     id,
                             uint64_t                     parentId,
                             const std::string            name,
                             EventStream::EventStreamType streamType,
                             bool                         remoteStream )
{
  //\todo: check whether that is always true
  if( id == parentId )
  {
    streamType = EventStream::ES_HOST_MASTER;
  }
  
  EventStream* p = new EventStream( id,
                                    parentId,
                                    name,
                                    streamType,
                                    remoteStream );
  streamsMap[id] = p;

  if ( p->isHostStream() )
  {
    if( p->isHostMasterStream() )
    {
      GraphNode* startNode = newGraphNode( 0, id, name, PARADIGM_ALL,
                                           RECORD_ATOMIC, MISC_PROCESS );
      p->addGraphNode( startNode, NULL );
      newEdge( globalSourceNode, startNode );
    }

    streamGroup.addHostStream( p );
  }
  else
  {
    /* e->setWeight(e->getWeight() - 1); */

    if ( streamType == EventStream::ES_DEVICE )
    {
      streamGroup.addDeviceStream( p );
      
      // try to figure out the device ID for OpenMP target threads
      int deviceId = -1;
      if( strstr( name.c_str(), "OMP target thread [" ) )
      {
        deviceId = atoi( name.c_str()+19 );
      }
      
      p->setDeviceId( deviceId );
    }
    else if ( streamType == EventStream::ES_DEVICE_NULL )
    {
      streamGroup.setNullStream( p );
    }
  }

  // initialize CPU data for this stream (full reset is done in addCPUEvent() )
  cpuDataPerProcess[id].numberOfEvents = 0;
  
  return p;
}

Graph&
GraphEngine::getGraph( )
{
  return graph;
}

Graph*
GraphEngine::getGraph( Paradigm p )
{
  return graph.getSubGraph( p );
}

EventStream*
GraphEngine::getStream( uint64_t id ) const
{
  EventStreamMap::const_iterator iter = streamsMap.find( id );
  if ( iter != streamsMap.end( ) )
  {
    return iter->second;
  }
  else
  {
    return NULL;
  }
}

void
GraphEngine::getStreams( EventStreamGroup::EventStreamList& streams ) const
{
  streamGroup.getAllStreams( streams );
}

void
GraphEngine::getLocalStreams( EventStreamGroup::EventStreamList& streams )
const
{
  streamGroup.getAllStreams( streams );
  for ( EventStreamGroup::EventStreamList::iterator iter = streams.begin( );
        iter != streams.end( ); )
  {
    if ( ( *iter )->isRemoteStream( ) )
    {
      iter = streams.erase( iter );
    }
    else
    {
      ++iter;
    }
  }
}

const EventStreamGroup::EventStreamList&
GraphEngine::getHostStreams( ) const
{
  return streamGroup.getHostStreams( );
}

const EventStreamGroup::EventStreamList&
GraphEngine::getDeviceStreams( ) const
{
  return streamGroup.getDeviceStreams( );
}

void
GraphEngine::getDeviceStreams( 
  int deviceId, 
  EventStreamGroup::EventStreamList& deviceStreams ) const
{
  if( deviceId == -1 )
    return;
  
  streamGroup.getDeviceStreams( deviceId, deviceStreams );
}

void
GraphEngine::getAllDeviceStreams( EventStreamGroup::EventStreamList& deviceStreams ) const
{
  streamGroup.getAllDeviceStreams( deviceStreams );
}

bool
GraphEngine::hasInEdges( GraphNode* n )
{
  return graph.hasInEdges( n );
}

bool
GraphEngine::hasOutEdges( GraphNode* n )
{
  return graph.hasOutEdges( n );
}

const Graph::EdgeList&
GraphEngine::getInEdges( GraphNode* n ) const
{
  if ( graph.hasInEdges( n ) )
  {
    return graph.getInEdges( n );
  }
  else
  {
    return emptyEdgeList;
  }
}

const Graph::EdgeList&
GraphEngine::getOutEdges( GraphNode* n ) const
{
  if ( graph.hasOutEdges( n ) )
  {
    return graph.getOutEdges( n );
  }
  else
  {
    return emptyEdgeList;
  }
}

GraphNode*
GraphEngine::newGraphNode( uint64_t          time,
                           uint64_t          streamId,
                           const std::string name,
                           Paradigm          paradigm,
                           RecordType        recordType,
                           int               nodeType )
{
  GraphNode* n = new GraphNode( time,
                                streamId,
                                name,
                                paradigm,
                                recordType,
                                nodeType );
  graph.addNode( n );
  return n;
}

EventNode*
GraphEngine::newEventNode( uint64_t                      time,
                           uint64_t                      streamId,
                           uint64_t                      eventId,
                           EventNode::FunctionResultType fResult,
                           const std::string             name,
                           Paradigm                      paradigm,
                           RecordType                    recordType,
                           int                           nodeType )
{
  EventNode* n = new EventNode( time,
                                streamId,
                                eventId,
                                fResult,
                                name,
                                paradigm,
                                recordType,
                                nodeType );
  graph.addNode( n );
  return n;
}

Edge*
GraphEngine::newEdge( GraphNode* source, GraphNode* target, 
                      int properties, Paradigm* edgeType )
{
  if( target->getId() == 500 ){
    UTILS_MSG( true, "bla");
  }
  
  Paradigm paradigm = PARADIGM_ALL;
  if ( edgeType )
  {
    paradigm = *edgeType;
  }
  else
  {
    if ( source->getParadigm( ) == target->getParadigm( ) )
    {
      paradigm = source->getParadigm( );
    }
  }

  Edge* newEdge = new Edge( source, target,
                            target->getTime( ) - source->getTime( ), 
                            properties, paradigm );
  //std::cerr << "[" << n1->getStreamId() << "] Add Edge " << n1->getUniqueName() 
  //          << " to " << n2->getUniqueName() << std::endl;
  graph.addEdge( newEdge );

  return newEdge;
}

/**
 * Get the edge (object) between the source and the target node.
 * Search the out edges only as both, in- and out-edge vectors should contain 
 * the edge (see addEdge()).
 * 
 *   
 * @param source start node of the edge
 * @param target end node of the edge
 * 
 * @return the edge between source and target node
 */
Edge*
GraphEngine::getEdge( GraphNode* source, GraphNode* target )
{
  // iterate over outgoing edges of source node
  const Graph::EdgeList& edgeList = getOutEdges( source );
  for ( Graph::EdgeList::const_iterator iter = edgeList.begin( );
        iter != edgeList.end( ); ++iter )
  {
    if ( ( *iter )->getEndNode( ) == target )
    {
      return *iter;
    }
  }
  
  return NULL;
}

void
GraphEngine::removeEdge( Edge* e )
{
  graph.removeEdge( e );
  delete e;
}

GraphNode*
GraphEngine::getSourceNode( ) const
{
  return globalSourceNode;
}

GraphNode*
GraphEngine::getFirstTimedGraphNode( Paradigm paradigm ) const
{
  GraphNode* firstNode = NULL;
  EventStreamGroup::EventStreamList streams;
  streamGroup.getAllStreams( streams );

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    EventStream* p = *iter;
    GraphNode* firstStreamGNode = NULL;
    
    // if the paradigms is MPI, we can use this shortcut
    if( paradigm == PARADIGM_MPI )
    {
      firstStreamGNode = p->getFirstNode( PARADIGM_MPI );
    }
    else
    {
      EventStream::SortedGraphNodeList& nodes = p->getNodes();
      for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin();
            nIter != nodes.end(); ++nIter )
      {
        GraphNode* n = *nIter;
        if ( ( n->getTime() > 0 ) && ( !n->isAtomic() ) )
        {
          if ( n->hasParadigm( paradigm ) )
          {
            firstStreamGNode = n;
            break;
          }
        }
      }
    }
    
    

    if ( firstStreamGNode )
    {
      if ( firstNode == NULL )
      {
        firstNode = firstStreamGNode;
      }
      else
      {
        if ( firstStreamGNode->getTime( ) < firstNode->getTime( ) )
        {
          firstNode = firstStreamGNode;
        }
      }
    }
  }
  
  streams.clear();

  return firstNode;
}

/**
 * Obtain the last (largest time stamp) graph node for a given paradigm.
 * 
 * @param paradigm
 * 
 * @return 
 */
GraphNode*
GraphEngine::getLastGraphNode( Paradigm paradigm ) const
{
  GraphNode* lastNode = NULL;
  EventStreamGroup::EventStreamList streams;
  streamGroup.getAllStreams( streams ); 

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin( );
        iter != streams.end( ); ++iter )
  {
    EventStream* p = *iter;
    GraphNode*   lastStreamGNode = p->getLastNode( paradigm );

    if ( lastStreamGNode )
    {
      if ( lastNode == NULL )
      {
        lastNode = lastStreamGNode;
      }
      else
      {
        //if ( lastStreamGNode->getTime() > lastNode->getTime() )
        if ( Node::compareLess( lastNode, lastStreamGNode ) )
        {
          lastNode = lastStreamGNode;
        }
      }
    }
  }
  
  streams.clear();

  return lastNode;
}

void
GraphEngine::getAllNodes( EventStream::SortedGraphNodeList& allNodes ) const
{
  EventStreamGroup::EventStreamList streams;
  getStreams( streams );

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin( );
        iter != streams.end( ); ++iter )
  {
    EventStream* p = *iter;
    if ( p->getNodes( ).size( ) > 0 )
    {
      allNodes.insert( allNodes.end( ), p->getNodes( ).begin( ),
                       p->getNodes( ).end( ) );
    }
  }
  
  streams.clear( );

  std::sort( allNodes.begin( ), allNodes.end( ), Node::compareLess );
}

AnalysisMetric&
GraphEngine::getCtrTable( )
{
  return ctrTable;
}

void
GraphEngine::reset( )
{
  /*
  // debug output
  if( pendingGraphNodeStackMap.size() > 0 )
  {
    for(GraphNodeStackMap::const_iterator mapIt = pendingGraphNodeStackMap.begin();
        mapIt != pendingGraphNodeStackMap.end(); ++mapIt)
    {
      UTILS_MSG( mapIt->second.size(), 
                 "  Stream %llu: stack size: %lu, top node: %s (%p)", 
                mapIt->first, mapIt->second.size(),
                mapIt->second.top()->getUniqueName().c_str(),
                topGraphNodeStack( mapIt->first ) );
    }
  }

  resetCounters( );

  const EventStreamGroup::EventStreamList& hostStreams =
    streamGroup.getHostStreams( );
  for ( EventStreamGroup::EventStreamList::const_iterator iter =
          hostStreams.begin( );
        iter != hostStreams.end( ); )
  {
    EventStream* p = *iter;

    if ( p->isRemoteStream( ) )
    {
      EventStream::SortedGraphNodeList& nodes = p->getNodes( );
      for ( EventStream::SortedGraphNodeList::const_iterator nIter =
              nodes.begin( );
            nIter != nodes.end( ); ++nIter )
      {
        GraphNode* node = (GraphNode*)( *nIter );
        const Graph::EdgeList& edges = graph.getInEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = edges.begin( );
              eIter != edges.end( ); )
        {
          Graph::EdgeList::const_iterator next = eIter;
          ++next;

          delete*eIter;
          graph.removeEdge( *eIter );

          eIter = next;
        }
      }

      iter = streamGroup.removeHostStream( p );
      delete p;
    }
    else
    {
      ++iter;
    }
  }*/
}

void
GraphEngine::resetCounters( )
{
  EventStreamGroup::EventStreamList streams;
  getLocalStreams( streams );

  for ( EventStreamGroup::EventStreamList::const_iterator pIter = streams.begin( );
        pIter != streams.end( ); ++pIter )
  {
    EventStream::SortedGraphNodeList nodes = ( *pIter )->getNodes( );
    for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin( );
          nIter != nodes.end( ); ++nIter )
    {
      ( *nIter )->removeCounters( );
    }
  }
  
  streams.clear();
}

uint64_t
GraphEngine::getTimerResolution( )
{
  return ticksPerSecond;
}

void
GraphEngine::setTimerResolution( uint64_t ticksPerSecond )
{
  this->ticksPerSecond = ticksPerSecond;
}

uint64_t
GraphEngine::getDeltaTicks( )
{
  return getTimerResolution( ) * SYNC_DELTA / ( 1000 * 1000 );
}

void
GraphEngine::sanityCheckEdge( Edge* edge, uint32_t mpiRank )
{
  uint64_t expectedTime;
  if ( edge->isReverseEdge( ) )
  {
    expectedTime = 0;
  }
  else
  {
    expectedTime = 
      edge->getEndNode()->getTime() - edge->getStartNode()->getTime();
  }

  if ( edge->getDuration( ) != expectedTime )
  {
    throw RTException(
            "[%u] Sanity check failed: edge %s has wrong duration (expected %lu, found %lu)",
            mpiRank,
            edge->getName( ).c_str( ),
            expectedTime,
            edge->getDuration( ) );
  }

  if ( !edge->isBlocking( ) && edge->getStartNode( )->isWaitstate( ) &&
       edge->getStartNode( )->isEnter( ) &&
       edge->getEndNode( )->isWaitstate( ) &&
       edge->getEndNode( )->isLeave( ) )
  {
    throw RTException(
            "[%u] Sanity check failed: edge %s is not blocking but should be",
            mpiRank, edge->getName( ).c_str( ) );
  }

}

void
GraphEngine::runSanityCheck( uint32_t mpiRank )
{
  EventStreamGroup::EventStreamList streams;
  getStreams( streams );

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin( );
        iter != streams.end( ); ++iter )
  {
    EventStream::SortedGraphNodeList& nodes = ( *iter )->getNodes( );
    for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin( );
          nIter != nodes.end( ); ++nIter )
    {
      GraphNode* node = (GraphNode*)( *nIter );

      if ( hasInEdges( node ) )
      {

        Graph::EdgeList inEdges = getInEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = inEdges.begin( );
              eIter != inEdges.end( ); ++eIter )
        {
          sanityCheckEdge( *eIter, mpiRank );
        }
      }

      if ( hasOutEdges( node ) )
      {
        Graph::EdgeList outEdges = getOutEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = outEdges.begin( );
              eIter != outEdges.end( ); ++eIter )
        {
          sanityCheckEdge( *eIter, mpiRank );
        }
      }
    }
  }
  
  streams.clear();
}

/**
 * Add information from a CPU (non-paradigm) event.
 * 
 * @param time the time stamp of the event
 * @param stream the stream the event occurred
 */
void
GraphEngine::addCPUEvent( uint64_t time, uint64_t stream, bool isLeave )
{
  EdgeCPUData& cpuData = cpuDataPerProcess[stream];

  if ( cpuData.numberOfEvents == 0 )
  {
    cpuData.startTime = time;
    cpuData.exclEvtRegTime = 0;
  }
  
  // get the last exclusive region time at region leave
  if( isLeave )
  {
    cpuData.exclEvtRegTime += cpuData.endTime - time;
  }

  cpuData.numberOfEvents++;
  cpuData.endTime = time;
}

/**
 * Adds a node to the graph. Also adds edges.
 * 
 * @param node
 * @param stream
 */
void
GraphEngine::addNewGraphNodeInternal( GraphNode* node, EventStream* stream )
{
  GraphNode::ParadigmNodeMap predNodeMap, nextNodeMap;
  
  UTILS_ASSERT( node->getStreamId() == stream->getId(), 
                "Cannot add graph node with stream ID %" PRIu64 " to stream "
                "with ID %" PRIu64, node->getStreamId(), stream->getId() );

  if( !stream->getLastNode() || Node::compareLess(stream->getLastNode(), node) )
  {
    //std::cerr << "last node: ";
    //std::cerr << stream->getLastNode( )->getUniqueName() << std::endl;
    // if the last node in the list is "less" than the current, 
    // push it at the end of the vector
    stream->addGraphNode( node, &predNodeMap );
  }
  else
  {
    stream->insertGraphNode( node, predNodeMap, nextNodeMap );
  }

  // to support nesting we use a stack to keep track of open activities
  GraphNode* stackNode = topGraphNodeStack( stream->getId() );

  if ( node->isLeave() )
  {
    if ( stackNode == NULL )
    {
      throw RTException( "StackNode NULL and found leave event %s.\n",
                         node->getUniqueName().c_str() );
    }
    else
    {
      UTILS_ASSERT( stackNode->getRecordType() != node->getRecordType(),
        "[%" PRIu64 "] Partner graph nodes with identical types are not allowed!"
        " new node: %s <-> stack node: %s",
        stream->getId(), node->getUniqueName().c_str(), 
        stackNode->getUniqueName().c_str(), stackNode );
      
      node->setPartner( stackNode );
      stackNode->setPartner( node );

      popGraphNodeStack( node->getStreamId() );

      // use the stack to get the caller/parent of this node
      node->setCaller( topGraphNodeStack( node->getStreamId() ) );
    }
  }
  else if ( node->isEnter() )
  {
    // use the stack to get the caller/parent of this node ( might be NULL)
    node->setCaller( stackNode );

    pushGraphNodeStack( node, node->getStreamId() );
  }

  /*
   * Link the node to its direct pred/next node and to the pred/next node
   * of its paradigm, if these are not the same.
   */

  // get direct predecessor and successor
  GraphNode* directPredecessor = NULL;
  GraphNode* directSuccessor   = NULL;
  for ( size_t p_index = 0; p_index < NODE_PARADIGM_COUNT; ++p_index )
  {
    Paradigm paradigm = (Paradigm)( 1 << p_index );
    GraphNode::ParadigmNodeMap::const_iterator predPnmIter = 
      predNodeMap.find( paradigm );
    GraphNode::ParadigmNodeMap::const_iterator nextPnmIter = 
      nextNodeMap.find( paradigm );

    // if we have a paradigm predecessor node AND 
    // (there is no direct predecessor OR the direct predecessor is before paradigm predecessor)
    if ( predPnmIter != predNodeMap.end() && 
         ( !directPredecessor || Node::compareLess( directPredecessor, predPnmIter->second ) ) )
    {
      // set the direct predecessor to the paradigm predecessor
      directPredecessor = predPnmIter->second;
      //std::cerr << "directPredecessor found: ";
      //std::cerr << directPredecessor->getUniqueName() << std::endl;
    }

    // if we have a paradigm successor AND
    // (there is no direct successor OR the paradigm predecessor is before the direct successor)
    if( nextPnmIter != nextNodeMap.end() && 
        ( !directSuccessor || Node::compareLess( nextPnmIter->second, directSuccessor ) ) )
    {
      //set the direct successor to the paradigm successor
      directSuccessor = nextPnmIter->second;
    }
  }
  
  EdgeCPUData& cpuData = cpuDataPerProcess[stream->getId()];

  bool directPredLinked = false;
  bool directSuccLinked = false;
  if ( directPredecessor )
  {
    Paradigm nodeParadigm = node->getParadigm( );
    Paradigm predParadigm = directPredecessor->getParadigm( );

    for ( size_t p_index = 0; p_index < NODE_PARADIGM_COUNT; ++p_index )
    {
      // 
      Paradigm paradigm = (Paradigm)( 1 << p_index );
      if ( ( paradigm & nodeParadigm ) != nodeParadigm )
      {
        continue;
      }

      GraphNode::ParadigmNodeMap::const_iterator predPnmIter = predNodeMap.find(
        paradigm );
      if ( predPnmIter != predNodeMap.end( ) )
      {
        GraphNode* pred     = predPnmIter->second;
        int        edgeProp = EDGE_NONE;

        // check if this leave node is the end of a blocking operation
        if ( pred->isEnter() && node->isLeave() )
        {
          if ( pred->isWaitstate() && node->isWaitstate() )
          {
            edgeProp |= EDGE_IS_BLOCKING;
          }
        }

        /* link to this predecessor */
        Edge* temp = newEdge( pred, node, edgeProp, &paradigm );
        //sanityCheckEdge( temp, stream->getId() );

        UTILS_ASSERT( !( cpuData.numberOfEvents && ( cpuData.startTime > cpuData.endTime ) ),
                      "Violation of time order for CPU events at '%s' (%",
                      temp->getName( ).c_str( ) );

        temp->addCPUData( cpuData.numberOfEvents,
                          //cpuData.startTime, cpuData.endTime,
                          cpuData.exclEvtRegTime );

        /* check if this already is the direct predecessor */
        if ( directPredecessor == pred )
        {
          directPredLinked = true;
        }

        if ( directSuccessor )
        {
          GraphNode::ParadigmNodeMap::const_iterator nextPnmIter =
            nextNodeMap.find( paradigm );
          if ( nextPnmIter != nextNodeMap.end( ) )
          {
            GraphNode* succ    = nextPnmIter->second;
            Edge*      oldEdge = getEdge( pred, succ );
            if ( !oldEdge )
            {
              throw RTException( "No edge between %s (p %u) and %s (p %u)",
                                 pred->getUniqueName( ).c_str( ),
                                 pred->getStreamId( ),
                                 succ->getUniqueName( ).c_str( ),
                                 succ->getStreamId( ) );
            }
            removeEdge( oldEdge );
            /* link to direct successor */
            /* can't be a blocking edge, as we never insert leave nodes */
            /* before enter nodes from the same function */
            newEdge( node, succ, EDGE_NONE, &paradigm );
            //sanityCheckEdge( temp, stream->getId() );

            if ( directSuccessor == succ )
            {
              directSuccLinked = true;
            }
          }
        }
      }
    }

    if ( !directPredLinked )
    {
      int edgeProp = EDGE_NONE;

      if ( directPredecessor->isEnter( ) && node->isLeave( ) )
      {
        if ( directPredecessor->isWaitstate( ) && node->isWaitstate( ) )
        {
          edgeProp |= EDGE_IS_BLOCKING;
        }
      }

      // link to direct predecessor
      Edge* temp = newEdge( directPredecessor, node, edgeProp, &predParadigm );
      //sanityCheckEdge( temp, stream->getId() );

      UTILS_ASSERT( !( cpuData.numberOfEvents && ( cpuData.startTime > cpuData.endTime ) ),
                    "Violation of time order for CPU events at '%s'",
                    temp->getName( ).c_str( ) );

      temp->addCPUData( cpuData.numberOfEvents,
                        //cpuData.startTime, cpuData.endTime,
                        cpuData.exclEvtRegTime );
    }

    if ( directSuccessor )
    {
      Paradigm succParadigm = directSuccessor->getParadigm( );

      if ( !directSuccLinked )
      {
        Edge* oldEdge = getEdge( directPredecessor, directSuccessor );
        if ( oldEdge )
        {
          removeEdge( oldEdge );
        }

        /* link to direct successor */
        newEdge( node, directSuccessor, EDGE_NONE, &succParadigm );
        //sanityCheckEdge( temp, stream->getId() );
      }

    }
  }

  // reset the number of CPU events for the following edge
  cpuData.numberOfEvents = 0;

}

GraphNode*
GraphEngine::addNewGraphNode( uint64_t       time,
                              EventStream*   stream,
                              const char*    name,
                              Paradigm       paradigm,
                              RecordType     recordType,
                              int            nodeType )
{
  GraphNode* node = newGraphNode( time, stream->getId( ), name,
                                  paradigm, recordType, nodeType );
  addNewGraphNodeInternal( node, stream );

  return node;
}

EventNode*
GraphEngine::addNewEventNode( uint64_t                      time,
                              uint64_t                      eventId,
                              EventNode::FunctionResultType fResult,
                              EventStream*                  stream,
                              const char*                   name,
                              FunctionDescriptor*           desc )
{
  EventNode* node = newEventNode( time, stream->getId( ), eventId,
                                  fResult, name, desc->paradigm, desc->recordType, 
                                  desc->functionType );
  addNewGraphNodeInternal( node, stream );
  return node;
}

GraphNode*
GraphEngine::topGraphNodeStack( uint64_t streamId )
{
  if ( pendingGraphNodeStackMap[streamId].empty( ) )
  {
    return NULL;
  }
  return pendingGraphNodeStackMap[streamId].top( );
}

void
GraphEngine::popGraphNodeStack( uint64_t streamId )
{
  pendingGraphNodeStackMap[streamId].pop( );
}

void
GraphEngine::pushGraphNodeStack( GraphNode* node, uint64_t streamId )
{
  pendingGraphNodeStackMap[streamId].push( node );
}
