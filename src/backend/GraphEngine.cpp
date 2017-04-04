/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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
#include "common.hpp"

using namespace casita;

GraphEngine::GraphEngine() :
  ticksPerSecond( 1000 )
{
  globalSourceNode = newGraphNode( 0, 0, "START", PARADIGM_ALL, RECORD_ATOMIC,
                                   MISC_PROCESS );

}

/**
 * Destructor of GraphEngine is currently called at the end of the program.
 * Therefore, do not clean up anything.
 */
GraphEngine::~GraphEngine()
{
  /*UTILS_MSG( true, "Destructor of GraphEngine called" );
  
  for ( EventStreamMap::iterator iter = streamsMap.begin();
        iter != streamsMap.end(); ++iter )
  {
    delete iter->second;
  }*/
}

EventStream*
GraphEngine::newEventStream( uint64_t                     id,
                             uint64_t                     parentId,
                             const std::string            name,
                             EventStream::EventStreamType streamType )
{
  //\todo: check whether that is always true
  if( id == parentId )
  {
    streamType = EventStream::ES_HOST_MASTER;
  }
  
  EventStream* p = new EventStream( id,
                                    parentId,
                                    name,
                                    streamType );
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
      // try to figure out the device ID for OpenMP target threads
      int deviceId = -1;
      if( strstr( name.c_str(), "OMP target thread [" ) )
      {
        deviceId = atoi( name.c_str() + 19 );
      }
      else if( strstr( name.c_str(), "CUDA[" ) ) // CUDA streams
      {
        deviceId = atoi( name.c_str() + 5 );
      }
      else if( strstr( name.c_str(), "MIC [" ) ) // deprecated (libmpti)
      {
        deviceId = atoi( name.c_str() + 5 );
      }
      else // OpenCL devices
      {
        size_t pos = name.find_last_of( "[" );
        if( pos !=  string::npos )
        {
          deviceId = atoi( name.c_str() + pos + 1 );
        }
      }
      
      p->setDeviceId( deviceId );
      
      // device ID has to be set to generate stream vectors per device
      streamGroup.addDeviceStream( p );
    }
  }

  // initialize CPU data for this stream (full reset is done in addCPUEvent() )
  cpuDataPerProcess[id].numberOfEvents = 0;
  
  return p;
}

Graph&
GraphEngine::getGraph()
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
  if ( iter != streamsMap.end() )
  {
    return iter->second;
  }
  else
  {
    return NULL;
  }
}

EventStreamGroup&
GraphEngine::getStreamGroup()
{
  return streamGroup;
}

const EventStreamGroup::EventStreamList&
GraphEngine::getStreams() const
{
  return streamGroup.getAllStreams();
}

const EventStreamGroup::EventStreamList&
GraphEngine::getHostStreams() const
{
  return streamGroup.getHostStreams();
}

const EventStreamGroup::EventStreamList&
GraphEngine::getDeviceStreams() const
{
  return streamGroup.getDeviceStreams();
}

size_t
GraphEngine::getNumDeviceStreams() const
{
  return streamGroup.getDeviceStreams().size();
}

const EventStreamGroup::EventStreamList&
GraphEngine::getDeviceStreams( int deviceId )
{
  return streamGroup.getDeviceStreams( deviceId );
}

void
GraphEngine::getAllDeviceStreams( EventStreamGroup::EventStreamList& deviceStreams ) const
{
  streamGroup.getAllDeviceStreams( deviceStreams );
}

bool
GraphEngine::haveDeviceNullStreamOnly() const
{
  return streamGroup.deviceWithNullStreamOnly();
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
  Paradigm paradigm = PARADIGM_ALL;
  if ( edgeType )
  {
    paradigm = *edgeType;
  }
  else
  {
    if ( source->getParadigm() == target->getParadigm() )
    {
      paradigm = source->getParadigm();
    }
    else // recently added this else branch
    {
      paradigm = (Paradigm) ( source->getParadigm() | target->getParadigm() );
    }
  }
  
  Edge* newEdge = new Edge( source, target,
                            target->getTime() - source->getTime(), 
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
  for ( Graph::EdgeList::const_iterator iter = edgeList.begin();
        iter != edgeList.end(); ++iter )
  {
    if ( ( *iter )->getEndNode() == target )
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
GraphEngine::getSourceNode() const
{
  return globalSourceNode;
}

GraphNode*
GraphEngine::getFirstTimedGraphNode( Paradigm paradigm ) const
{
  GraphNode* firstNode = NULL;
  
  const EventStreamGroup::EventStreamList streams = streamGroup.getAllStreams();

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    EventStream* p = *iter;
    GraphNode* firstStreamGNode = NULL;
    
    // if the paradigms is MPI, we can use this shortcut
    if( paradigm == PARADIGM_MPI )
    {
      firstStreamGNode = p->getFirstParadigmNode( PARADIGM_MPI );
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
        if ( firstStreamGNode->getTime() < firstNode->getTime() )
        {
          firstNode = firstStreamGNode;
        }
      }
    }
  }

  return firstNode;
}

/**
 * Obtain the last (largest time stamp) graph node for a given paradigm or 
 * set of paradigms.
 * 
 * @param paradigm
 * 
 * @return 
 */
GraphNode*
GraphEngine::getLastGraphNode( Paradigm paradigm ) const
{
  GraphNode* lastNode = NULL;
  const EventStreamGroup::EventStreamList streams = streamGroup.getAllStreams();

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    EventStream* p = *iter;
    GraphNode* lastStreamGNode = p->getLastNode( paradigm );

    if ( lastStreamGNode )
    {
      if ( lastNode == NULL )
      {
        lastNode = lastStreamGNode;
      }
      else
      {
        if ( Node::compareLess( lastNode, lastStreamGNode ) )
        {
          lastNode = lastStreamGNode;
        }
      }
    }
  }

  return lastNode;
}

void
GraphEngine::getAllNodes( EventStream::SortedGraphNodeList& allNodes ) const
{
  const EventStreamGroup::EventStreamList streams = getStreams();

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    EventStream* p = *iter;
    if ( p->getNodes().size() > 0 )
    {
      allNodes.insert( allNodes.end(), p->getNodes().begin(), p->getNodes().end() );
    }
  }

  std::sort( allNodes.begin(), allNodes.end(), Node::compareLess );
}

AnalysisMetric&
GraphEngine::getCtrTable()
{
  return ctrTable;
}

void
GraphEngine::reset()
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

  resetCounters();

  const EventStreamGroup::EventStreamList& hostStreams =
    streamGroup.getHostStreams();
  for ( EventStreamGroup::EventStreamList::const_iterator iter =
          hostStreams.begin();
        iter != hostStreams.end(); )
  {
    EventStream* p = *iter;

    if ( p->isRemoteStream() )
    {
      EventStream::SortedGraphNodeList& nodes = p->getNodes();
      for ( EventStream::SortedGraphNodeList::const_iterator nIter =
              nodes.begin();
            nIter != nodes.end(); ++nIter )
      {
        GraphNode* node = (GraphNode*)( *nIter );
        const Graph::EdgeList& edges = graph.getInEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = edges.begin();
              eIter != edges.end(); )
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
GraphEngine::resetCounters()
{
  const EventStreamGroup::EventStreamList streams = getStreams();

  for ( EventStreamGroup::EventStreamList::const_iterator pIter = streams.begin();
        pIter != streams.end(); ++pIter )
  {
    EventStream::SortedGraphNodeList nodes = ( *pIter )->getNodes();
    for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin();
          nIter != nodes.end(); ++nIter )
    {
      ( *nIter )->removeCounters();
    }
  }
}

uint64_t
GraphEngine::getTimerResolution()
{
  return ticksPerSecond;
}

void
GraphEngine::setTimerResolution( uint64_t ticksPerSecond )
{
  this->ticksPerSecond = ticksPerSecond;
}

uint64_t
GraphEngine::getDeltaTicks()
{
  return getTimerResolution() * SYNC_DELTA / ( 1000 * 1000 );
}

void
GraphEngine::sanityCheckEdge( Edge* edge, uint32_t mpiRank )
{
  uint64_t expectedTime;
  if ( edge->isReverseEdge() )
  {
    expectedTime = 0;
  }
  else
  {
    expectedTime = 
      edge->getEndNode()->getTime() - edge->getStartNode()->getTime();
  }

  if ( edge->getDuration() != expectedTime )
  {
    throw RTException(
      "[%u] Sanity check failed: edge %s has wrong duration (expected %lu, found %lu)",
      mpiRank, edge->getName().c_str(),
      expectedTime, edge->getDuration() );
  }

  if ( !edge->isBlocking() && edge->getStartNode()->isWaitstate() &&
       edge->getStartNode()->isEnter() &&
       edge->getEndNode()->isWaitstate() &&
       edge->getEndNode()->isLeave() )
  {
    throw RTException(
            "[%u] Sanity check failed: edge %s should be blocking",
            mpiRank, edge->getName().c_str() );
  }

}

void
GraphEngine::runSanityCheck( uint32_t mpiRank )
{
  const EventStreamGroup::EventStreamList streams = getStreams();

  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    EventStream::SortedGraphNodeList& nodes = ( *iter )->getNodes();
    for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin();
          nIter != nodes.end(); ++nIter )
    {
      GraphNode* node = (GraphNode*)( *nIter );

      if ( hasInEdges( node ) )
      {

        Graph::EdgeList inEdges = getInEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
              eIter != inEdges.end(); ++eIter )
        {
          sanityCheckEdge( *eIter, mpiRank );
        }
      }

      if ( hasOutEdges( node ) )
      {
        Graph::EdgeList outEdges = getOutEdges( node );
        for ( Graph::EdgeList::const_iterator eIter = outEdges.begin();
              eIter != outEdges.end(); ++eIter )
        {
          sanityCheckEdge( *eIter, mpiRank );
        }
      }
    }
  }
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
  
  uint64_t streamId = stream->getId();
  
  UTILS_ASSERT( node->getStreamId() == streamId, 
                "Cannot add graph node with stream ID %" PRIu64 " to stream "
                "with ID %" PRIu64, node->getStreamId(), stream->getId() );
  
  

  // insert/add node to sorted stream node list and initialize map of 
  // predecessors and successors
  if( !stream->getLastNode() || Node::compareLess( stream->getLastNode(), node ) )
  {
    //std::cerr << "last node: ";
    //std::cerr << stream->getLastNode()->getUniqueName() << std::endl;
    // if the last node in the list is "less" than the current, 
    // push it at the end of the vector and get the paradigm predecessor nodes
    stream->addGraphNode( node, &predNodeMap );
  }
  else
  {
    // this happens when additional nodes are added during the analysis (rules)
    stream->insertGraphNode( node, predNodeMap, nextNodeMap );
  }

  // to support nesting we use a stack to keep track of open activities
  GraphNode* stackNode = topGraphNodeStack( streamId );

  // if we added a leave node its partner (enter) has to be on top of the stack
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
        streamId, node->getUniqueName().c_str(), 
        stackNode->getUniqueName().c_str(), stackNode );
      
      node->setPartner( stackNode );
      stackNode->setPartner( node );

      popGraphNodeStack( streamId );

      // use the stack to get the caller/parent of this node (might be NULL)
      node->setCaller( topGraphNodeStack( streamId ) );
    }
  }
  else if ( node->isEnter() )
  {
    // use the stack to get the caller/parent of this node (might be NULL)
    node->setCaller( stackNode );

    // add the node to the stack as a region is opened
    pushGraphNodeStack( node, streamId );
  }

  /*
   * Link the node to its direct pred/next node and to the pred/next node
   * of its paradigm, if these are not the same.
   */

  // get direct predecessor and successor
  GraphNode* directPredecessor = NULL;
  GraphNode* directSuccessor   = NULL;
  
  // iterate over paradigms
  for ( size_t pIdx = 0; pIdx < NODE_PARADIGM_COUNT; ++pIdx )
  {
    Paradigm paradigm = (Paradigm)( 1 << pIdx );
    GraphNode::ParadigmNodeMap::const_iterator paradigmPredIter = 
      predNodeMap.find( paradigm );
    GraphNode::ParadigmNodeMap::const_iterator paradigmSuccessorIter = 
      nextNodeMap.find( paradigm );

    // if we have a paradigm predecessor node AND 
    // (there is no direct predecessor OR the direct predecessor is before paradigm predecessor)
    if ( paradigmPredIter != predNodeMap.end() && 
         ( !directPredecessor || 
           Node::compareLess( directPredecessor, paradigmPredIter->second ) ) )
    {
      // set the direct predecessor to the paradigm predecessor
      directPredecessor = paradigmPredIter->second;
      //std::cerr << "directPredecessor for " << node->getUniqueName() << " found: ";
      //std::cerr << directPredecessor->getUniqueName() << std::endl;
    }

    // if we have a paradigm successor AND
    // (there is no direct successor OR the paradigm predecessor is before the direct successor)
    if( paradigmSuccessorIter != nextNodeMap.end() && 
        ( !directSuccessor || 
          Node::compareLess( paradigmSuccessorIter->second, directSuccessor ) ) )
    {
      //set the direct successor to the paradigm successor
      directSuccessor = paradigmSuccessorIter->second;
    }
  }
  
  EdgeCPUData& cpuData = cpuDataPerProcess[ streamId ];

  bool directPredLinked = false;
  bool directSuccLinked = false;
  
  // the direct predecessor is NULL, if no node of its paradigm is preceeding 
  // in this stream, hence most nodes will execute the following block to create
  // an edge to the predecessor of the same paradigm
  Paradigm nodeParadigm = node->getParadigm();
  if ( directPredecessor )
  {
    Paradigm predParadigm = directPredecessor->getParadigm();

    // paradigm predecessor edges are only needed for MPI, as these edges 
    // are used to generate sub graphs
    if( nodeParadigm == PARADIGM_MPI )
    {
      // iterate over the paradigms
      for ( size_t pIdx = 0; pIdx < NODE_PARADIGM_COUNT; ++pIdx )
      {
        Paradigm paradigm = (Paradigm)( 1 << pIdx );

        // ignore paradigms that the node does not have (a node might have 
        // multiple paradigms, e.g. PARADIGM_ALL)
        if ( ( paradigm & nodeParadigm ) != nodeParadigm )
        {
          continue;
        }

        GraphNode::ParadigmNodeMap::const_iterator predPnmIter = 
          predNodeMap.find( paradigm );

        // if no predecessor for the current paradigm was found, continue ...
        if ( predPnmIter == predNodeMap.end() )
        {
          continue;
        }

        // as we have a predecessor in the iteration's paradigm, we create an edge

        GraphNode* pred     = predPnmIter->second;
        int        edgeProp = EDGE_NONE;

        // make the edge blocking, if current node and predecessor are wait states
        if ( pred->isEnter() && node->isLeave() && 
             pred->isWaitstate() && node->isWaitstate() )
        {
          edgeProp |= EDGE_IS_BLOCKING;
        }

        // create an intra-paradigm, intra-stream edge
        Edge* pEdge = newEdge( pred, node, edgeProp, &paradigm );

        UTILS_ASSERT( !( cpuData.numberOfEvents && ( cpuData.startTime > cpuData.endTime ) ),
                      "Violation of time order for CPU events at '%s' (%",
                      pEdge->getName().c_str() );

        pEdge->addCPUData( cpuData.numberOfEvents,
                          cpuData.exclEvtRegTime );

        // check if this already is the direct predecessor
        if ( directPredecessor == pred )
        {
          directPredLinked = true;
        }

        if ( directSuccessor )
        {
          GraphNode::ParadigmNodeMap::const_iterator nextPnmIter =
            nextNodeMap.find( paradigm );
          if ( nextPnmIter != nextNodeMap.end() )
          {
            GraphNode* succ    = nextPnmIter->second;
            Edge*      oldEdge = getEdge( pred, succ );
            if ( !oldEdge )
            {
              throw RTException( "No edge between %s (p %u) and %s (p %u)",
                                 pred->getUniqueName().c_str(),
                                 pred->getStreamId(),
                                 succ->getUniqueName().c_str(),
                                 succ->getStreamId() );
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
    
    // if the predecessor of the same paradigm is not the direct predecessor
    if ( !directPredLinked )
    {
      int edgeProp = EDGE_NONE;

      if ( directPredecessor->isEnter() && node->isLeave() && 
           directPredecessor->isWaitstate() && node->isWaitstate() )
      {
        edgeProp |= EDGE_IS_BLOCKING;
      }

      // link to direct predecessor (other paradigm)
      Edge* temp = newEdge( directPredecessor, node, edgeProp, &predParadigm );
      //sanityCheckEdge( temp, stream->getId() );

      UTILS_ASSERT( !( cpuData.numberOfEvents && ( cpuData.startTime > cpuData.endTime ) ),
                    "Violation of time order for CPU events at '%s'",
                    temp->getName().c_str() );

      temp->addCPUData( cpuData.numberOfEvents,
                        cpuData.exclEvtRegTime );
    }

    if ( directSuccessor )
    {
      Paradigm succParadigm = directSuccessor->getParadigm();

      if ( !directSuccLinked )
      {
        Edge* oldEdge = getEdge( directPredecessor, directSuccessor );
        if ( oldEdge )
        {
          removeEdge( oldEdge );
        }

        // link to direct successor 
        newEdge( node, directSuccessor, EDGE_NONE, &succParadigm );
        //sanityCheckEdge( temp, stream->getId() );
      }

    }
  }

  // reset the number of CPU events for the following edge
  cpuData.numberOfEvents = 0;
  
  // create edges from device kernel enter nodes to previous nodes on the device
  if( Parser::getInstance().getProgramOptions().linkKernels &&
      node->isEnter() && ( node->isCUDAKernel() || node->isOpenCLKernel() ) )
  {
    GraphNode* directPredKernel = NULL;
    // direct predecessor (on the same stream) should be a leave
    if( directPredecessor && directPredecessor->isLeave() )
    {
      // set it as direct predecessor if no closer one on another stream is found
      directPredKernel = directPredecessor;
    }

    // look for later leave nodes on other device streams
    const EventStreamGroup::EventStreamList& devStreams = 
      streamGroup.getDeviceStreams( stream->getDeviceId() );
    
    for ( EventStreamGroup::EventStreamList::const_iterator it = devStreams.begin(); 
          it != devStreams.end(); ++it )
    {
      EventStream* devStream = *it;
      
      // ignore kernels on the same stream
      if( devStream->getId() == streamId )
      {
        continue;
      }
      
      // get last node of the offloading paradigm for current stream
      GraphNode* predKernel = 
        devStream->getLastParadigmNode( node->getParadigm() );
      if( !predKernel )
      {
        continue;
      }
      
      // use the kernel leave node (if already available)
      predKernel = predKernel->getGraphPair().second;
      if( !predKernel )
      {
        continue;
      }

      // ignore the potential preceding kernel, if it overlaps with the source kernel
      if( predKernel->getTime() > node->getTime() )
      {
        continue;
      }
      
      //UTILS_WARNING( "%s: last kernel %s", node->getUniqueName().c_str(),
      //               predKernel->getUniqueName().c_str() );

      // if no direct predecessor is found yet or the current last kernel
      // is after the current predecessor
      if( !directPredKernel || Node::compareLess( directPredKernel, predKernel ) )
      {
        directPredKernel = predKernel;
      }
    }

    // if we found a direct predecessor that is different from the one on the
    // same stream and does not link to the current source kernel itself
    if( directPredKernel && directPredKernel != directPredecessor 
        && node != directPredKernel )
    {
      node->setLinkLeft( directPredKernel );
      
      //UTILS_WARNING( "%s link left to %s", node->getUniqueName().c_str(),
      //               directPredKernel->getUniqueName().c_str() );
    }
  }
}

GraphNode*
GraphEngine::addNewGraphNode( uint64_t       time,
                              EventStream*   stream,
                              const char*    name,
                              Paradigm       paradigm,
                              RecordType     recordType,
                              int            nodeType )
{
  GraphNode* node = newGraphNode( time, stream->getId(), name,
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
  EventNode* node = newEventNode( time, stream->getId(), eventId,
                                  fResult, name, desc->paradigm, desc->recordType, 
                                  desc->functionType );
  addNewGraphNodeInternal( node, stream );
  return node;
}

GraphNode*
GraphEngine::topGraphNodeStack( uint64_t streamId )
{
  if ( pendingGraphNodeStackMap[streamId].empty() )
  {
    return NULL;
  }
  return pendingGraphNodeStackMap[streamId].top();
}

void
GraphEngine::popGraphNodeStack( uint64_t streamId )
{
  pendingGraphNodeStackMap[streamId].pop();
}

void
GraphEngine::pushGraphNodeStack( GraphNode* node, uint64_t streamId )
{
  pendingGraphNodeStackMap[streamId].push( node );
}
