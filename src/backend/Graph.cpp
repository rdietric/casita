/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016
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

bool
Graph::hasInEdges( GraphNode* node ) const
{
  return inEdges.find( node ) != inEdges.end();
}

bool
Graph::hasOutEdges( GraphNode* node ) const
{
  return outEdges.find( node ) != outEdges.end();
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

Graph::EdgeList
Graph::getInEdges( GraphNode* node, Paradigm paradigm ) const
{
  EdgeList edges;
  NodeEdges::const_iterator iter = inEdges.find( node );
  if ( iter != inEdges.end() )
  {
    for ( EdgeList::const_iterator eIter = iter->second.begin();
          eIter != iter->second.end(); ++eIter )
    {
      if ( ( *eIter )->getStartNode()->hasParadigm( paradigm ) )
      {
        edges.push_back( *eIter );
      }
    }
  }

  return edges;
}

/**
 * 
 * @param node
 * @return 
 */
const Graph::EdgeList&
Graph::getOutEdges( GraphNode* node ) const
{
  NodeEdges::const_iterator iter = outEdges.find( node );
  if ( iter != outEdges.end() )
  {
    return iter->second;
  }
  throw RTException( "Node %s not found in out-edge list",
                     node->getUniqueName().c_str() );
}

Graph::EdgeList
Graph::getOutEdges( GraphNode* node, Paradigm paradigm ) const
{
  EdgeList edges;
  NodeEdges::const_iterator iter = outEdges.find( node );
  if ( iter != outEdges.end() )
  {
    for ( EdgeList::const_iterator eIter = iter->second.begin();
          eIter != iter->second.end(); ++eIter )
    {
      if ( ( *eIter )->getEndNode()->hasParadigm( paradigm ) )
      {
        edges.push_back( *eIter );
      }
    }
  }

  return edges;
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

    if ( hasOutEdges( node ) )
    {
      EdgeList edges = getOutEdges( node, paradigm );
      for ( EdgeList::const_iterator eIter = edges.begin();
            eIter != edges.end(); ++eIter )
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

/**
 * Detect the longest/critical path between the given start and stop node.
 * (Does not need the OpenMP latestEnterNode to barrierEndNode edge.)
 * Seems to be broken somehow!
 * 
 * @param start start node
 * @param end end node
 * @param path the list of critical nodes / the longest path
 */
void
Graph::getLongestPath( GraphNode* startNode, GraphNode* endNode,
                       GraphNode::GraphNodeList& path ) const
{
  const uint64_t INFINITE  = UINT64_MAX;
  
  std::map< GraphNode*, GraphNode* > predecessorMap;
  
  // store distances from start node to the map entry node (key)
  std::map< GraphNode*, uint64_t > nodeDistanceMap; 
  std::list< GraphNode* > pendingNodes;

  const uint64_t endTime = endNode->getTime();
  
  //bool test = false;

  // iterate over the nodes of the graph and initialize predecessor relations,
  // distances and pending nodes
  for ( Graph::NodeList::const_iterator iter = nodes.begin();
        iter != nodes.end(); ++iter )
  {
    GraphNode* node = *iter;
    
    // ignore nodes that are not in the time interval
    if ( Node::compareLess( node, startNode ) || Node::compareLess( endNode, node ) )
    {
      continue;
    }
    
    // initialize infinite distances
    nodeDistanceMap[node] = INFINITE;
    
    // initialize predecessors (predecessor is predecessor of itself)
    predecessorMap[node] = node;

    // initialize pending nodes
    if ( node != startNode ) // should always be true 
    {/*
      UTILS_MSG( node->getStreamId() == 0 && node->getId() >= 277 &&
                 node->getId() <= 360,
                 "[0] List node on CP? %s", node->getUniqueName().c_str() );
      
      if( node->getStreamId() == 0 && node->getId() == 315 )
        test = true;
      */
      pendingNodes.push_back( node );
    }
  }
  

  // set distance of start node to zero
  nodeDistanceMap[startNode] = 0;
  
  // make the start node (of the section) the first in the pending nodes list
  pendingNodes.push_front( startNode );

  /* pendingNodes is already sorted after construction */
  
  // iterate over the nodes in the interval in chronologic order
  // until the list is empty
  while ( !pendingNodes.empty() )
  {
    GraphNode* currentNode         = pendingNodes.front();
    uint64_t   currentNodeDistance = nodeDistanceMap[currentNode];
    /*
    UTILS_MSG( currentNode->getStreamId() == 0 && currentNode->getId() >= 277 &&
               currentNode->getId() <= 360, "[0] forward check %s (pending %lu)", 
               currentNode->getUniqueName().c_str(), pendingNodes.size());
    */
    // all pending nodes are initialized with infinite distance except the start node
    if ( currentNodeDistance == INFINITE )
    {
      break;
    }

    // remove the current node from the pending nodes and distance entry
    pendingNodes.pop_front();
    nodeDistanceMap.erase( currentNode );

    // if the current node has out edges
    if ( hasOutEdges( currentNode ) )
    {
      const Graph::EdgeList& outEdges = getOutEdges( currentNode );

      // iterate over the out edges
      for ( Graph::EdgeList::const_iterator iter = outEdges.begin();
            iter != outEdges.end(); ++iter )
      {
        Edge* edge = *iter;

        // get the node the edge is pointing to (move forward in time)
        GraphNode* successor = edge->getEndNode();

        // if target is the end node or within the interval (time < interval end time)
        // AND target is in the distance map AND edge is not blocking AND not reverse
        if ( ( ( successor == endNode ) || ( successor->getTime() < endTime ) )
             && ( nodeDistanceMap.find( successor ) != nodeDistanceMap.end() ) 
             && ( !edge->isBlocking() ) && ( !edge->isReverseEdge() ) ) 
        {
          // compute the distance to the successor
          uint64_t alt_distance = currentNodeDistance + edge->getWeight();
          
          // if the distance to the successor node is less than the already 
          // stored successor's distance (distances are initialized with infinite)
          if ( alt_distance < nodeDistanceMap[successor] )
          {
            // remove the neighbor from the pending nodes and assign a distance
            pendingNodes.remove( successor );
            nodeDistanceMap[successor] = alt_distance;

            // orders the list pendingNodes by distances, starting with the smallest
            sortedInsert( successor, pendingNodes, nodeDistanceMap );
            
            // the neighbors predecessor is the current node
            predecessorMap[successor] = currentNode;
          }
        }
      }
    }
  }

  // start CP search from the end node and stop if the start node is reached
  GraphNode* currentNode = endNode;
  while ( currentNode != startNode )
  {
    // get all ingoing nodes for current node, ignore blocking and reverse edges
    GraphNode::GraphNodeList possibleInNodes;

    if ( hasInEdges( currentNode ) )
    {
      const Graph::EdgeList& inEdges = getInEdges( currentNode );
      for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
            eIter != inEdges.end(); ++eIter )
      {
        Edge* edge = *eIter;
        if ( !( edge->isBlocking() ) && !( edge->isReverseEdge() ) )
        {
          GraphNode* inNode = edge->getStartNode();

          // if the targetNode is on a critical path, add it to the possible target nodes
          GraphNode* pred   = predecessorMap[inNode];
          if ( pred == startNode || pred != inNode )
          {
            possibleInNodes.push_back( inNode );
          }

        }
      }
    }

    // choose among the ingoing nodes the one which is closest as next current node
    if ( possibleInNodes.size() > 0 )
    {
      GraphNode* closestNode = NULL;
      for ( GraphNode::GraphNodeList::const_iterator pInIter =
              possibleInNodes.begin();
            pInIter != possibleInNodes.end(); ++pInIter )
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
      // add current node to critical path and choose next current node
      path.push_front( currentNode );
      currentNode = closestNode;
    }
    else
    {
      break;
    }
  }
  path.push_front( startNode );
  
  /*if(test)
  {
    UTILS_MSG( true, "Print critical path:" );
    for ( GraphNode::GraphNodeList::const_iterator cpNode = path.begin();
        cpNode != path.end(); ++cpNode )
    {
      GraphNode* node = *cpNode;

      UTILS_MSG( true, "%s", node->getUniqueName().c_str() );
    }
  }*/
}

void
Graph::printPath( const GraphNode::GraphNodeList& path ) const
{
  for ( GraphNode::GraphNodeList::const_iterator cpNode = path.begin();
        cpNode != path.end(); ++cpNode )
  {
    UTILS_MSG( true, "  %s", (*cpNode)->getUniqueName().c_str() );
  }
}

void
Graph::printInEdges( GraphNode* node ) const
{
  UTILS_MSG( true, "  <- in-edges for %s:", node->getUniqueName().c_str() );
  const Graph::EdgeList& inEdges = getInEdges( node );
  for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
        eIter != inEdges.end(); ++eIter )
  {
    Edge* edge = *eIter;
    UTILS_MSG( true, "     %s", edge->getName().c_str() );
  }
}

void
Graph::printCircle( GraphNode* node, GraphNode::GraphNodeList& nodeList ) const
{
  UTILS_MSG( true, "Detected circular dependency in local critical path analysis"
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

  // assume that the node list nodes is sorted by time

  GraphNode* currentNode = endNode;
  
  // revers-iterate over the graph nodes as long as the the start node is before the current node
  //while ( currentNode != startNode )
  while ( Node::compareLess( startNode, currentNode ) )
  {
    //UTILS_WARNING("Process node: %s", currentNode->getUniqueName().c_str() );
    
    // make sure that there are edges
    if ( hasInEdges( currentNode ) )
    {
      uint64_t maxWeight = 0; // reset max weight
      GraphNode* predecessorNode = NULL; // predecessor not yet found
      
      // iterate over the in edges of the node (ignore blocking)
      const Graph::EdgeList& inEdges = getInEdges( currentNode );
      for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
            eIter != inEdges.end(); ++eIter )
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
        // revert edges have to be inter process to avoid endless loops
        if ( ( edge->isReverseEdge() && edge->isInterProcessEdge() ) || 
             ( !edge->isBlocking() && curWeight > maxWeight && curWeight != INFINITE ) )
        {
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

        if( Parser::getInstance().getProgramOptions().cpaLoopCheck && 
            GraphNode::search( currentNode, path ) )
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
    if( Parser::getInstance().getProgramOptions().cpaLoopCheck && 
        GraphNode::search( currentNode, path ) )
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
