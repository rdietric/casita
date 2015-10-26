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
 * - basic interaction with the Graph: add/delete nodes/edges
 * - get longest Path
 *
 */

#include <sstream>
#include <fstream>

#include "graph/Graph.hpp"
#include "graph/Edge.hpp"
#include "graph/GraphNode.hpp"
#include "common.hpp"

using namespace casita;

Graph::Graph( )
{
  isSubGraph = false;
}

Graph::Graph( bool subGraph )
{
  isSubGraph = subGraph;
}

Graph::~Graph( )
{
  //if ( isSubGraph )
  //  std::cerr << "subgraph destructor does not delete edges, but the vectors" << std::endl;
  
  cleanup( !isSubGraph );
}

/**
 * Clear all lists in this graph object and deallocate/delete edges. 
 * Nodes are not deleted.
 */
void
Graph::cleanup( bool deleteEdges )
{
  //std::cerr << "Cleanup graph -- delete edges: " << deleteEdges << std::endl;
  
  // iterate over the out-edge lists of all node entries
  for ( NodeEdges::iterator iter = outEdges.begin( );
        iter != outEdges.end( ); ++iter )
  {
    if( deleteEdges ){
      // delete the edges for this node (which are in the list)
      for ( EdgeList::const_iterator eIter = iter->second.begin( );
            eIter != iter->second.end( ); ++eIter )
      {
        if(*eIter)
        {
          //std::cerr << "Remove edge: " << (*eIter)->getStartNode()->getUniqueName() << " to "
          //          << (*eIter)->getEndNode()->getUniqueName() << std::endl;
          delete *eIter;
        }
      }
    }
    
    // clear the edge list itself
    iter->second.clear();
  }
  
  // iterate over the in-edge lists of all node entries
  for ( NodeEdges::iterator iter = inEdges.begin( );
        iter != inEdges.end( ); ++iter )
  {
    // clear the edge list itself
    iter->second.clear();
  }
  
  // clear all node vectors
  outEdges.clear( );
  inEdges.clear( );
  nodes.clear( );
}

void
Graph::addNode( GraphNode* node )
{
  nodes.push_back( node );
}

void
Graph::addEdge( Edge* edge )
{
  //std::cerr << "Added Edge " << edge->getStartNode()->getUniqueName() << " to "
  //          << edge->getEndNode()->getUniqueName() << std::endl;
  inEdges[edge->getEndNode( )].push_back( edge );
  outEdges[edge->getStartNode( )].push_back( edge );

/*  
  if(edge->getEndNode( )->getId() == 9)
  {
    std::cerr  << "[" << edge->getEndNode( )->getStreamId() << "]" << edge->getEndNode( )->getUniqueName() << " has inEdges? ";
    std::cerr << hasInEdges( edge->getEndNode( ) ) << std::endl;
  }
  
  if( edge->getStartNode( )->getId() == 9 )
  {
    std::cerr  << "[" << edge->getStartNode( )->getStreamId() << "]" << edge->getStartNode( )->getUniqueName() << " has inEdges? ";
    std::cerr << hasInEdges( edge->getStartNode( ) ) << std::endl;
  }*/
}

/**
 * Removes the given edge from the list of in and out edges.
 */
void
Graph::removeEdge( Edge* edge )
{
  GraphNode* start     = edge->getStartNode( );
  GraphNode* end       = edge->getEndNode( );
  
  //std::cerr << "Remove edge: " << edge->getStartNode()->getUniqueName() << " to "
  //          << edge->getEndNode()->getUniqueName() << std::endl;

  EdgeList&  out_edges = outEdges[start];
  EdgeList&  in_edges  = inEdges[end];

  for ( EdgeList::iterator iter = out_edges.begin( );
        iter != out_edges.end( ); ++iter )
  {
    if ( ( *iter )->getEndNode( ) == end )
    {
      out_edges.erase( iter );
      break;
    }
  }
  for ( EdgeList::iterator iter = in_edges.begin( );
        iter != in_edges.end( ); ++iter )
  {
    if ( ( *iter )->getStartNode( ) == start )
    {
      in_edges.erase( iter );
      break;
    }
  }
}

bool
Graph::hasInEdges( GraphNode* node ) const
{
  return inEdges.find( node ) != inEdges.end( );
}

bool
Graph::hasOutEdges( GraphNode* node ) const
{
  return outEdges.find( node ) != outEdges.end( );
}

const Graph::EdgeList&
Graph::getInEdges( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = inEdges.find( node );
  if ( iter != inEdges.end( ) )
  {
    return iter->second;
  }
  throw RTException( "Node %s not found in in-edge list",
                     node->getUniqueName( ).c_str( ) );
}

Graph::EdgeList
Graph::getInEdges( GraphNode* node, Paradigm paradigm ) const
{
  EdgeList edges;
  NodeEdges::const_iterator iter = inEdges.find( node );
  if ( iter != inEdges.end( ) )
  {
    for ( EdgeList::const_iterator eIter = iter->second.begin( );
          eIter != iter->second.end( ); ++eIter )
    {
      if ( ( *eIter )->getStartNode( )->hasParadigm( paradigm ) )
      {
        edges.push_back( *eIter );
      }
    }
  }

  return edges;
}

const Graph::EdgeList&
Graph::getOutEdges( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = outEdges.find( node );
  if ( iter != outEdges.end( ) )
  {
    return iter->second;
  }
  throw RTException( "Node %s not found in out-edge list",
                     node->getUniqueName( ).c_str( ) );
}

Graph::EdgeList
Graph::getOutEdges( GraphNode* node, Paradigm paradigm ) const
{
  EdgeList edges;
  NodeEdges::const_iterator iter = outEdges.find( node );
  if ( iter != outEdges.end( ) )
  {
    for ( EdgeList::const_iterator eIter = iter->second.begin( );
          eIter != iter->second.end( ); ++eIter )
    {
      if ( ( *eIter )->getEndNode( )->hasParadigm( paradigm ) )
      {
        edges.push_back( *eIter );
      }
    }
  }

  return edges;
}

const Graph::NodeList&
Graph::getNodes( ) const
{
  return nodes;
}

/**
 * Generates a sub graph for the given paradigm (including node and edge list).
 * Node and edge lists are newly generated.
 * 
 * @param paradigm
 * @return the sub graph for the given paradigm
 */
Graph*
Graph::getSubGraph( Paradigm paradigm )
{
  // make sure to deallocate the graph
  Graph* subGraph = new Graph( true );

  for ( NodeList::const_iterator iter = nodes.begin( );
        iter != nodes.end( ); ++iter )
  {
    GraphNode* node = *iter;

    // add only nodes of the given paradigm
    if ( !node->hasParadigm( paradigm ) )
    {
      continue;
    }

    subGraph->addNode( node );

    if ( hasOutEdges( node ) )
    {
      EdgeList edges = getOutEdges( node, paradigm );
      for ( EdgeList::const_iterator eIter = edges.begin( );
            eIter != edges.end( ); ++eIter )
      {
        Edge* edge = *eIter;

        if ( edge->hasEdgeType( paradigm ) )
        {
          subGraph->addEdge( edge );
        }
      }
    }
  }

  return subGraph;
}

bool
Graph::compareDistancesLess( GraphNode* n1, GraphNode* n2,
                             DistanceMap& distanceMap )
{
  assert( n1 );
  assert( n2 );

  uint64_t dist1 = distanceMap[n1];
  uint64_t dist2 = distanceMap[n2];

  if ( dist1 != dist2 )
  {
    return dist1 < dist2;
  }
  else
  {
    return n1->getId( ) < n2->getId( );
  }
}

void
Graph::sortedInsert( GraphNode* n, std::list< GraphNode* >& nodes,
                     DistanceMap& distanceMap )
{
  uint64_t distance_n = distanceMap[n];

  std::list< GraphNode* >::iterator iter = nodes.begin( );
  while ( iter != nodes.end( ) )
  {
    std::list< GraphNode* >::iterator current = iter;
    ++iter;

    if ( iter == nodes.end( ) || distanceMap[*iter] >= distance_n )
    {
      nodes.insert( current, n );
      return;
    }
  }

  nodes.push_front( n );
}

void
Graph::getLongestPath( GraphNode* start, GraphNode* end,
                       GraphNode::GraphNodeList& path ) const
{
  const uint64_t infinite  = std::numeric_limits< uint64_t >::max( );
  std::map< GraphNode*, GraphNode* > preds;
  std::map< GraphNode*, uint64_t > distance;
  std::list< GraphNode* > pendingNodes;

  const uint64_t startTime = start->getTime( );
  const uint64_t endTime   = end->getTime( );

  for ( Graph::NodeList::const_iterator iter = nodes.begin( );
        iter != nodes.end( ); ++iter )
  {
    uint64_t nodeTime = ( *iter )->getTime( );
    if ( nodeTime < startTime || nodeTime > endTime )
    {
      continue;
    }

    GraphNode* gn     = *iter;
    /** initialize infinite distances */
    distance[gn] = infinite;
    /** initialize predecessors (-> start is pred of start) */
    preds[gn]    = gn;

    if ( gn != start )
    {
      pendingNodes.push_back( gn );
    }
  }

  distance[start] = 0;
  pendingNodes.push_front( start );

  /* pendingNodes is already sorted after construction */

  while ( !pendingNodes.empty( ) )
  {
    GraphNode* current_node      = pendingNodes.front( );
    uint64_t   current_node_dist = distance[current_node];
    if ( current_node_dist == infinite )
    {
      break;
    }

    pendingNodes.pop_front( );
    distance.erase( current_node );

    if ( hasOutEdges( current_node ) )
    {
      const Graph::EdgeList& outEdges = getOutEdges( current_node );

      for ( Graph::EdgeList::const_iterator iter = outEdges.begin( );
            iter != outEdges.end( ); ++iter )
      {
        Edge*      edge      = *iter;
        GraphNode* neighbour = edge->getEndNode( );
        if ( ( ( neighbour == end ) || ( neighbour->getTime( ) < endTime ) )
             && ( distance.find( neighbour ) != distance.end( ) )
             && ( !edge->isBlocking( ) ) && ( !edge->isReverseEdge( ) ) )
        {
          uint64_t alt_distance = current_node_dist + edge->getWeight( );
          if ( alt_distance < distance[neighbour] )
          {
            pendingNodes.remove( neighbour );
            distance[neighbour] = alt_distance;

            sortedInsert( neighbour, pendingNodes, distance );
            preds[neighbour]    = current_node;
          }
        }
      }
    }
  }

  GraphNode* currentNode = end;
  while ( currentNode != start )
  {
    /* get all ingoing nodes for current node, ignore blocking and */
    /* reverse edges */
    GraphNode::GraphNodeList possibleInNodes;

    if ( hasInEdges( currentNode ) )
    {
      const Graph::EdgeList& inEdges = getInEdges( currentNode );
      for ( Graph::EdgeList::const_iterator eIter = inEdges.begin( );
            eIter != inEdges.end( ); ++eIter )
      {
        Edge* edge = *eIter;
        if ( !( edge->isBlocking( ) ) && !( edge->isReverseEdge( ) ) )
        {
          GraphNode* inNode = edge->getStartNode( );

          /* if the targetNode is on a critical path, add it to the */
          /* possible target nodes */
          GraphNode* pred   = preds[inNode];
          if ( pred == start || pred != inNode )
          {
            possibleInNodes.push_back( inNode );
          }

        }
      }
    }

    /* choose among the ingoing nodes the one which is closest */
    /* as next current node */
    if ( possibleInNodes.size( ) > 0 )
    {
      GraphNode* closestNode = NULL;
      for ( GraphNode::GraphNodeList::const_iterator pInIter =
              possibleInNodes.begin( );
            pInIter != possibleInNodes.end( ); ++pInIter )
      {
        if ( !closestNode )
        {
          closestNode = *pInIter;
        }
        else
        {
          if ( Node::compareLess( closestNode, *pInIter ) )
          {
            closestNode = *pInIter;
          }
        }
      }
      /* add current node to critical path and choose next current
       * node */
      path.push_front( currentNode );
      currentNode = closestNode;
    }
    else
    {
      break;
    }
  }
  path.push_front( start );
}
