/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include <set>
#include <deque>

#include "GraphNode.hpp"
#include "Edge.hpp"

namespace casita
{
  class Graph
  {
    public:
      typedef std::vector< Edge* > EdgeList;
      typedef std::set< Edge* > EdgeSet;
      typedef std::vector< GraphNode* > NodeList;
      typedef std::map< GraphNode*, EdgeList > NodeEdges;

      Graph( );
      Graph( bool isSubGraph );
      virtual
      ~Graph( );

      /**
       * Clear all lists in this graph object and deallocate/delete edges.
       */
      void
      cleanup( bool deleteEdges );

      void
      addNode( GraphNode* node );

      void
      addEdge( Edge* edge );

      void
      removeEdge( Edge* edge );

      Graph*
      getSubGraph( Paradigm paradigm );

      const EdgeList&
      getInEdges( GraphNode* node ) const;

      const EdgeList*
      getInEdgesPtr( GraphNode* node ) const;

      void
      printPath( const GraphNode::GraphNodeList& path ) const;

      const EdgeList*
      getOutEdges( GraphNode* node ) const;

      const NodeList&
      getNodes( ) const;

      void
      getCriticalPath( GraphNode* start, GraphNode* end,
          GraphNodeQueue& path ) const;

    protected:
      NodeList  nodes;
      NodeEdges inEdges, outEdges;
      bool      isSubGraph;

      typedef std::map< GraphNode*, uint64_t > DistanceMap;

      static bool
      compareDistancesLess( GraphNode* n1, GraphNode* n2,
          DistanceMap& distanceMap );

      static void
      sortedInsert( GraphNode* n, std::list< GraphNode* >& nodes,
          DistanceMap& distanceMap );

    private:
      void
      printInEdges( GraphNode* node ) const;

      void
      printCircle( GraphNode* node, GraphNodeQueue& nodeList ) const;

  };

}
