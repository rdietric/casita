/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013, 2014, 2016-2018,
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
#include <algorithm>

#include "graph/Graph.hpp"
#include "graph/Edge.hpp"
#include "graph/GraphNode.hpp"
#include "common.hpp"
#include "utils/ErrorUtils.hpp"

using namespace casita;

Graph::Graph()
{
  isSubGraph = false;
}

Graph::Graph( bool subGraph )
{
  isSubGraph = subGraph;
}

Graph::~Graph()
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
  for ( NodeEdges::iterator iter = outEdges.begin();
        iter != outEdges.end(); ++iter )
  {
    if( deleteEdges ){
      // delete the edges for this node (which are in the list)
      for ( EdgeList::const_iterator eIter = iter->second.begin();
            eIter != iter->second.end(); ++eIter )
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
  for ( NodeEdges::iterator iter = inEdges.begin();
        iter != inEdges.end(); ++iter )
  {
    // clear the edge list itself
    iter->second.clear();
  }
  
  // clear all node vectors
  outEdges.clear();
  inEdges.clear();
  nodes.clear();
}

void
Graph::addNode( GraphNode* node )
{
  nodes.push_back( node );
}

/**
 * Adds an edge to the graph. The end node is added to in-edges vector, the 
 * start node is added to out-edges vector.
 * 
 * @param edge the edge that is added to the graph
 */
void
Graph::addEdge( Edge* edge )
{
  //std::cerr << "Added Edge " << edge->getStartNode()->getUniqueName() << " to "
  //          << edge->getEndNode()->getUniqueName() << std::endl;
  // push this edge as in-edge to the target/end node
  inEdges[edge->getEndNode()].push_back( edge );
  outEdges[edge->getStartNode()].push_back( edge );

/*  
  if(edge->getEndNode()->getId() == 9)
  {
    std::cerr  << "[" << edge->getEndNode()->getStreamId() << "]" << edge->getEndNode()->getUniqueName() << " has inEdges? ";
    std::cerr << hasInEdges( edge->getEndNode() ) << std::endl;
  }
  
  if( edge->getStartNode()->getId() == 9 )
  {
    std::cerr  << "[" << edge->getStartNode()->getStreamId() << "]" << edge->getStartNode()->getUniqueName() << " has inEdges? ";
    std::cerr << hasInEdges( edge->getStartNode() ) << std::endl;
  }*/
}

/**
 * Removes the given edge from the list of in and out edges.
 * 
 * @param edge
 */
void
Graph::removeEdge( Edge* edge )
{
  GraphNode* startNode = edge->getStartNode();
  GraphNode* endNode   = edge->getEndNode();
  
  //std::cerr << "Remove edge: " << edge->getStartNode()->getUniqueName() << " to "
  //          << edge->getEndNode()->getUniqueName() << std::endl;

  EdgeList& out_edges = outEdges[startNode];
  EdgeList& in_edges  = inEdges[endNode];

  for ( EdgeList::iterator iter = out_edges.begin();
        iter != out_edges.end(); ++iter )
  {
    if ( ( *iter )->getEndNode() == endNode )
    {
      out_edges.erase( iter );
      break;
    }
  }
  for ( EdgeList::iterator iter = in_edges.begin();
        iter != in_edges.end(); ++iter )
  {
    if ( ( *iter )->getStartNode() == startNode )
    {
      in_edges.erase( iter );
      break;
    }
  }
}

const Graph::EdgeList&
Graph::getInEdges( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = inEdges.find( node );
  if ( iter != inEdges.end() )
  {
    return iter->second;
  }
  throw RTException( "Node %s not found in in-edge list",
                     node->getUniqueName().c_str() );
}

/**
 * Using this function can avoid an additional find by previously calling
 * hasInEdges().
 * 
 * @param node
 * @return 
 */
const Graph::EdgeList*
Graph::getInEdgesPtr( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = inEdges.find( node );
  if ( iter != inEdges.end() )
  {
    return &( iter->second );
  }
  else
  {
    return NULL;
  }
}

/**
 * 
 * @param node
 * @return 
 */
const Graph::EdgeList*
Graph::getOutEdges( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = outEdges.find( node );
  if ( iter != outEdges.end() )
  {
    return &(iter->second);
  }
  else
  {
    return NULL;
  }
}

const Graph::NodeList&
Graph::getNodes() const
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

  for ( NodeList::const_iterator iter = nodes.begin();
        iter != nodes.end(); ++iter )
  {
    GraphNode* node = *iter;

    // add only nodes of the given paradigm
    if ( !node->hasParadigm( paradigm ) )
    {
      continue;
    }

    subGraph->addNode( node );

    const EdgeList* edges = getOutEdges( node );
    if( edges )
    {
      for ( EdgeList::const_iterator eIter = edges->begin();
              eIter != edges->end(); ++eIter )
      {
        Edge* edge = *eIter;

        // add only edges with the requested paradigm
        if ( edge->hasEdgeType( paradigm ) &&
               edge->getEndNode()->hasParadigm( paradigm ) )
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
    return n1->getId() < n2->getId();
  }
}

/**
 * Insert a node into the given list so that node distances are increasing from
 * the start to the end of the list.
 * 
 * @param node        node to be inserted
 * @param nodeKist    list of nodes where node is inserted
 * @param distanceMap map of node distances
 */
void
Graph::sortedInsert( GraphNode* node, std::list< GraphNode* >& nodeList,
                     DistanceMap& distanceMap )
{
  uint64_t nodeDistance = distanceMap[node];

  std::list< GraphNode* >::iterator iter = nodeList.begin();
  while ( iter != nodeList.end() )
  {
    std::list< GraphNode* >::iterator current = iter;
    ++iter;

    // if this is the last node OR distance of next node is greater than of current node
    if ( iter == nodeList.end() || distanceMap[*iter] >= nodeDistance )
    {
      // insert before the current iterator
      nodeList.insert( current, node );
      return;
    }
  }

  // if no node with greater or equal distance was found, insert node as first node
  nodeList.push_front( node );
}

void
Graph::printPath( const GraphNode::GraphNodeList& path ) const
{
  for ( GraphNode::GraphNodeList::const_iterator cpNode = path.begin();
        cpNode != path.end(); ++cpNode )
  {
    UTILS_OUT( "  %s", (*cpNode)->getUniqueName().c_str() );
  }
}

void
Graph::printInEdges( GraphNode* node ) const
{
  UTILS_OUT( "  <- in-edges for %s:", node->getUniqueName().c_str() );
  const Graph::EdgeList& inEdges = getInEdges( node );
  for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
        eIter != inEdges.end(); ++eIter )
  {
    Edge* edge = *eIter;
    UTILS_OUT( "     %s", edge->getName().c_str() );
  }
}

void
Graph::printCircle( GraphNode* node, GraphNode::GraphNodeList& nodeList ) const
{
  UTILS_OUT( "Detected circular dependency in local critical path analysis"
             " at %s. Print node list: " , node->getUniqueName().c_str() );
  for( GraphNode::GraphNodeList::const_iterator it = nodeList.begin();
       it != nodeList.end(); ++it )
  {
    printInEdges( *it );
    
    if( node == *it )
    {
      return;
    }
  }
}

/**
 * Detect the longest/critical path between the given start and stop node.
 * 
 * @param startNode start node
 * @param endNode end node
 * @param path the list of critical nodes / the longest path
 */
void
Graph::getCriticalPath( GraphNode* startNode, GraphNode* endNode,
                        GraphNode::GraphNodeList& path ) const
{
  const uint64_t INFINITE = UINT64_MAX;
  bool loop_check = Parser::getInstance().getProgramOptions().cpaLoopCheck;

  // assume that the node list nodes is sorted by time

  GraphNode* currentNode = endNode;
  
  // revers-iterate over the graph nodes as long as the the start node is before the current node
  //while ( currentNode != startNode )
  while ( Node::compareLess( startNode, currentNode ) )
  {
    //UTILS_WARNING("Process node: %s", currentNode->getUniqueName().c_str() );
    
    const Graph::EdgeList* inEdges = getInEdgesPtr( currentNode );
    // make sure that there are edges
    if ( inEdges )
    {
      uint64_t maxWeight = 0; // reset max weight
      GraphNode* predecessorNode = NULL; // predecessor not yet found
      
      // iterate over the in edges of the node (ignore blocking)
      for ( Graph::EdgeList::const_iterator eIter = inEdges->begin();
            eIter != inEdges->end(); ++eIter )
      {
        Edge* edge = *eIter;
        uint64_t curWeight = edge->getWeight();
        /*
        UTILS_MSG( currentNode->getStreamId() == 0 && currentNode->getId() == 11348, 
          //&& currentNode->getId() < 11424 && strcmp( currentNode->getName(), "cuStreamSynchronize" ) == 0, 
                   "%s has potential path to %s (weight: %llu, reverse: %d, blocking: %d)", 
                   currentNode->getUniqueName().c_str(),
                   edge->getStartNode()->getUniqueName().c_str(), curWeight, 
                   edge->isReverseEdge(), edge->isBlocking() );
        */       
        // if edge is not blocking AND weight is more than current but not infinite
        // (weight is complementary to duration)
        // reverse edges have to be inter process to avoid endless loops
        if ( ( edge->isReverseEdge() && edge->isInterProcessEdge() ) || 
             ( !edge->isBlocking() && curWeight > maxWeight && curWeight != INFINITE ) )
        {
          // force loop check, if we find a reverse edge
          if( edge->isReverseEdge() )
          {
            UTILS_WARN_ONCE( "Force critical path loop check!" );
            loop_check = true;
          }
          
          maxWeight = curWeight;
          predecessorNode = edge->getStartNode();
        }
      }
      
      // if we found the shortest predecessor edge, set end node as next node
      if( predecessorNode )
      {
        /*UTILS_MSG( startNode->getStreamId() == 0 && startNode->getId() >= 277 && 
                   endNode->getId() <= 360, "%s is on the CP to %s (weight: %llu)", 
                   currentNode->getUniqueName().c_str(),
                   predecessorNode->getUniqueName().c_str(), maxWeight);*/

        if( loop_check && GraphNode::search( currentNode, path ) )
        {
          //UTILS_WARNING( "Circular loop detected in local critical path analysis at %s! ",
          //               currentNode->getUniqueName().c_str() );
          
          printCircle( currentNode, path );
          
          currentNode = path.front();
          predecessorNode = currentNode; // ignore following endless loop check
        }
        else
        {
          path.push_front( currentNode );
        }
        
        // check for endless loop between two nodes
        if( currentNode != predecessorNode ) 
        {
          currentNode = predecessorNode;
          continue;
        }
        else
        {
          UTILS_WARNING( "Stop critical path loop on %s which has an edge to itself", 
                         currentNode->getUniqueName().c_str() );
          break;
        }
      }
    }
    
    // use direct predecessor of currentNode (on same stream, if currentNode has a caller)
    Graph::NodeList::const_reverse_iterator rit = 
      GraphNode::findNode( currentNode, nodes );
    GraphNode* predecessorNode = *(++rit);
    
    // if current node has a caller (there are more event on this stream)
    if( currentNode->getCaller() )
    {
      // move to the previous node on same stream
      while( currentNode->getStreamId() != predecessorNode->getStreamId() )
      {
        predecessorNode = *(++rit);
      }
    }
    
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "%s has no in edges. Use predecessor: %s", 
               currentNode->getUniqueName().c_str(),
               predecessorNode->getUniqueName().c_str() );
    
    // add the current node to the critical nodes
    if( loop_check && GraphNode::search( currentNode, path ) )
    {
      UTILS_WARNING( "Try to insert %s twice on the critical path!",
                     currentNode->getUniqueName().c_str() );
      
      printCircle( currentNode, path );
    }
    else
    {
      path.push_front( currentNode );
    }
    
    if( currentNode != predecessorNode ) // check for endless loop
    {
      currentNode = predecessorNode;
    }
    else
    {
      break;
    }
  }
  
  // add the start node to the critical nodes
  path.push_front( startNode );
}
