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
 * What this file does:
 * - Basic interaction with an event stream: add/remove/insert nodes, getter, get Attributes about eventstream
 * - walk forward/backward through stream (and call callback for each node on that walk)
 * - manage pending/consuming kernels (CUDA)
 * - manage pending MPIRecords
 *
 */

#include "EventStream.hpp"
#include "utils/ErrorUtils.hpp"

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
  lastNode( NULL )
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

EventStream::MPICommRecordList
EventStream::getPendingMPIRecords( )
{
  MPICommRecordList copyList;
  copyList.assign( mpiCommRecords.begin( ), mpiCommRecords.end( ) );
  mpiCommRecords.clear( );
  return copyList;
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
