/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
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
  hasFirstCriticalNode( false ),
  hasLastEvent( false ),
  lastNode( NULL ),
  lastEventTime( 0 ),
  isFiltering( false ),
  filterStartTime( 0 ),
  predictionOffset ( 0 )
{
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    graphData[i].firstNode = NULL;
    graphData[i].lastNode  = NULL;
  }
  
  // set the initial values for first enter and last leave
  streamPeriod.first = UINT64_MAX;
  streamPeriod.second = 0;
}

EventStream::~EventStream()
{
  for ( SortedGraphNodeList::iterator iter = nodes.begin();
        iter != nodes.end(); ++iter )
  {
    delete( *iter );
  }
}

uint64_t
EventStream::getId() const
{
  return id;
}

uint64_t
EventStream::getParentId() const
{
  return parentId;
}

const char*
EventStream::getName() const
{
  return name.c_str();
}

EventStream::EventStreamType
EventStream::getStreamType() const
{
  return streamType;
}

void
EventStream::setStreamType( EventStream::EventStreamType type )
{
  streamType = type;
}

bool
EventStream::isHostStream() const
{
  return streamType & ES_HOST;
}

bool
EventStream::isMpiStream() const
{
  return streamType & ES_MPI;
}

bool
EventStream::isDeviceStream() const
{
  return streamType & ( ES_DEVICE | ES_DEVICE_NULL );
}

bool
EventStream::isDeviceNullStream() const
{
  return streamType & ES_DEVICE_NULL;
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
EventStream::isFirstCritical()
{
  return hasFirstCriticalNode;
}

/**
 * Does this stream contains the global last event (of the trace)?
 * 
 * @return true, if the critical path ends on this stream
 */
bool&
EventStream::hasLastGlobalEvent()
{
  return hasLastEvent;
}

GraphNode*
EventStream::getLastNode() const
{
  return lastNode;
  //return getLastNode( PARADIGM_ALL );
}

/**
 * Get the last GraphNode for a given paradigm. The paradigm might be a
 * collection of multiple paradigms. The last node is returned.
 * 
 * @param paradigm
 * 
 * @return 
 */
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
 * Get the last node of a given paradigm. 
 * Faster than getLastNode( Paradigm paradigm ), but works only for a single 
 * paradigm and NOT for e.g. PARADIGM_ALL.
 * 
 * @param paradigm
 * @return 
 */
GraphNode*
EventStream::getLastParadigmNode( Paradigm paradigm ) const
{
  return graphData[(int)log2( paradigm )].lastNode;
}

/**
 * Get the first node of a given paradigm.
 * This works only for a single paradigm and NOT for e.g. PARADIGM_ALL.
 * 
 * @param paradigm
 * @return 
 */
GraphNode*
EventStream::getFirstParadigmNode( Paradigm paradigm ) const
{
  return graphData[(int)log2( paradigm )].firstNode;
}

void
EventStream::setLastEventTime( uint64_t time )
{
  lastEventTime = time;
}

uint64_t
EventStream::getLastEventTime() const
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

/**
 * Add a node to the stream's vector of sorted graph nodes.
 * Fill the paradigm predecessor node map.
 * 
 * @param node
 * @param predNodes
 */
void
EventStream::addGraphNode( GraphNode*                  node,
                           GraphNode::ParadigmNodeMap* predNodes )
{
  // set changed flag
  nodesAdded = true;

  GraphNode* oldNode[ NODE_PARADIGM_COUNT ];

  // iterate over paradigms
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {    
    // get paradigm type
    Paradigm oparadigm = (Paradigm)( 1 << i );

    oldNode[ i ] = getLastParadigmNode( oparadigm );
    
    // if predecessor node map should be read and old node is not NULL
    if ( predNodes && oldNode[ i ] )
    {
      // insert current last node of the given paradigm
      //predNodes->insert( std::make_pair( oparadigm, oldNode[ i ] ) );
      ( *predNodes )[ oparadigm ] = oldNode[ i ];
    }

    // if the node has the current iteration's paradigm
    if ( node->hasParadigm( oparadigm ) )
    {
      // ensure node order
      if ( oldNode[ i ] &&
           Node::compareLess( node, oldNode[ i ] ) )
      {
        throw RTException(
                "Can't add graph node (%s) before last graph node (%s)",
                node->getUniqueName().c_str(),
                oldNode[i]->getUniqueName().c_str() );
      }

      // set the first node in the stream, if it was not set yet
      if ( graphData[ i ].firstNode == NULL )
      {
        graphData[ i ].firstNode = node;
      }

      // set the last node for the current paradigm in this stream
      graphData[ i ].lastNode = node;
    }
  }

  // push the node to the internal vector
  addNodeInternal( nodes, node );

  // for MPI nodes
  if ( node->getParadigm() == PARADIGM_MPI )
  {
    GraphNode* lastLocalNode = getLastNode();
    
    //std::cerr << "[" << this->id << "] " << node->getUniqueName() 
    //          << "setLinkLeft: " << lastLocalNode->getUniqueName() << std::endl;
    
    // set the left link to the last local node (for CPA)
    node->setLinkLeft( lastLocalNode );
    
    // save MPI nodes as they do not have a right link yet
    unlinkedMPINodes.push_back( node );
  }

  // set the right link of all MPI nodes in the unlinked list to the current node
  if ( node->isEnter() )
  {
    for ( SortedGraphNodeList::const_iterator iter =
            unlinkedMPINodes.begin();
          iter != unlinkedMPINodes.end(); ++iter )
    {
      //std::cerr << "XXXXXRight  " << ( *iter )->getUniqueName() << " -> " << node->getUniqueName() << std::endl;
      ( *iter )->setLinkRight( node );
    }
    unlinkedMPINodes.clear();
  }
}

void
EventStream::insertGraphNode( GraphNode*                  node,
                              GraphNode::ParadigmNodeMap& predNodes,
                              GraphNode::ParadigmNodeMap& nextNodes )
{
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "Insert node %s on stream %"PRIu64, 
             node->getUniqueName().c_str(), this->getId() );
  
  // set changed flag
  nodesAdded = true;
  
  // set the last-node field
  if ( !lastNode || Node::compareLess( lastNode, node ) )
  {
    lastNode = node;
  }

  // add the node to the sorted nodes list
  SortedGraphNodeList::iterator result = nodes.end();
  for ( SortedGraphNodeList::iterator iter = nodes.begin();
        iter != nodes.end(); ++iter )
  {
    SortedGraphNodeList::iterator next = iter;
    ++next;

    // if next is end of list, then push the node at the end of the vector
    if ( next == nodes.end() )
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
    while ( current != nodes.begin() )
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

    while ( next != nodes.end() )
    {
      if ( ( *next )->hasParadigm( ( Paradigm )paradigm ) )
      {
        nextNode = *next;
        hasNextNode[ paradigm_index ] = true;
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
EventStream::hasNewNodes()
{
  return nodesAdded;
}

EventStream::SortedGraphNodeList&
EventStream::getNodes()
{
  return nodes;
}

void
EventStream::clearNodes()
{
  // clear the nodes list (do not delete the nodes themselves)
  nodes.clear();
  
  // set the first and last Node to NULL
  for ( size_t i = 0; i < NODE_PARADIGM_COUNT; ++i )
  {
    graphData[i].firstNode = NULL;
    graphData[i].lastNode  = NULL;
  }
  
  lastNode = NULL;
}

void
EventStream::setFilter( bool enable, uint64_t time )
{
  isFiltering = enable;
  
  if( enable )
  {
    filterStartTime = time;
  }
  else
  {
    predictionOffset += time - filterStartTime;
  }
}

uint64_t&
EventStream::getPredictionOffset()
{
  return predictionOffset;
}

bool 
EventStream::isFilterOn()
{
  return isFiltering;
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
               node->getUniqueName().c_str(), node->getStreamId() );
    iter = find( nodes.rbegin(), nodes.rend(), node );
  }
  
  // make sure that we found a node
  UTILS_ASSERT( *iter == node, "no %s in stream %lu",
                node->getUniqueName().c_str(), node->getStreamId() );

  // iterate backwards over the list of nodes
  for (; iter != nodes.rend(); ++iter )
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
  SortedGraphNodeList::const_iterator iter = iter_tmp.base();

  // iterate forward over the list of nodes
  for (; iter != nodes.end(); ++iter )
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
  if ( nodes.size() == 0 )
  {
    return nodes.rend();
  }

  // there is only one node in the vector
  if ( nodes.size() == 1 )
  {
    return nodes.rbegin();
  }

  // set start boundaries for the search
  size_t indexMin = 0;
  size_t indexMax = nodes.size() - 1;
  
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

    UTILS_ASSERT( index < nodes.size(), "index %lu indexMax %lu indexMin %lu", 
                  index, indexMax, indexMin );

    // if we found the node at index ('middle' element)
    // for uneven elements, index points on the element after the half
    if ( nodes[index] == node )
    {
      return nodes.rbegin() + ( nodes.size() - index - 1 );
    }

    // indexMin == indexMax == index
    // only the nodes[index] element was left, which did not match
    // we can leave the loop
    if ( indexMin == indexMax )
    {
      std::cerr << "Stream " << node->getStreamId() << " Looking for node " 
                << node->getUniqueName() << " - Wrong node found! Index (" 
                << index << ") node on break: "
                << nodes[index]->getUniqueName() << std::endl;

      std::cerr << "Node sequence:" << std::endl;
      for(size_t i = index - 3; i < index + 4; i++)
      {
        if( nodes[i] )
          std::cerr << nodes[i]->getUniqueName() << std::endl;
      }
      
      std::cerr << " Previous compare node [" << indexPrevMin << ":" << indexPrevMax 
                << "]:" << nodes[indexPrev]->getUniqueName()
                << " with result: " << Node::compareLess( node, nodes[indexPrev] ) 
                << std::endl;
      
      std::cerr << " Pre-Previous compare node: " << nodes[indexPrev2]->getUniqueName()
                << " with result: " << Node::compareLess( node, nodes[indexPrev2] ) 
                << std::endl;
      //std::cerr << "return nodes.rbegin() = " << nodes.rbegin() << std::endl;
      //std::cerr << "return nodes.rend() = " << nodes.rend() << std::endl;
      
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
  return nodes.rend();
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
EventStream::reset()
{
  nodesAdded = false;
  
  // clear list of unlinked MPI nodes (print to stderr before), the last node is always unlinked!
  if( unlinkedMPINodes.size() > 1 )
  {
    UTILS_OUT( "[%"PRIu64"] Clear list of unlinked MPI nodes (%lu)!", 
               this->id, this->unlinkedMPINodes.size() );
    
    for ( SortedGraphNodeList::const_iterator iter =
            unlinkedMPINodes.begin(); iter != unlinkedMPINodes.end(); ++iter )
    {
      UTILS_OUT( "[%"PRIu64"]   %s", 
                 this->id, ( *iter )->getUniqueName().c_str() );
    }
    
    unlinkedMPINodes.clear();
  }
}
