/* 
 * File:   Graph.hpp
 * Author: felix
 *
 * Created on May 7, 2013, 3:24 PM
 */

#ifndef GRAPH_HPP
#define	GRAPH_HPP

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include <set>

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>

#include "GraphNode.hpp"
#include "Edge.hpp"

namespace cdm
{
    typedef boost::property< boost::vertex_name_t, std::string,
    boost::property< boost::vertex_distance_t, uint64_t,
    boost::property< boost::vertex_color_t, std::string > > > VertexProperty;

    typedef boost::property<boost::edge_weight_t, uint64_t,
    boost::property<boost::edge_weight2_t, uint64_t,
    boost::property< boost::edge_color_t, std::string > > > EdgeProperty;

    typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS,
    VertexProperty, EdgeProperty> BoostGraphType;

    class Graph
    {
    public:
        typedef std::vector<Edge*> EdgeList;
        typedef std::set<Edge*> EdgeSet;
        typedef std::vector<GraphNode*> NodeList;
        typedef std::map<GraphNode*, EdgeList> NodeEdges;

        Graph();
        virtual ~Graph();

        void addNode(GraphNode* node);
        void addEdge(Edge *edge);
        void removeEdge(Edge *edge);

        bool hasInEdges(GraphNode *node) const;
        bool hasOutEdges(GraphNode *node) const;

        const EdgeList &getInEdges(GraphNode *node) const;
        EdgeList getInEdges(GraphNode *node, Paradigm paradigm) const;
        const EdgeList &getOutEdges(GraphNode *node) const;
        EdgeList getOutEdges(GraphNode *node, Paradigm paradigm) const;

        const NodeList &getNodes() const;

        BoostGraphType *getBoostGraph(const std::set<GraphNode*> *cpnodes);
        Graph *getSubGraph(Paradigm paradigm);

        void getLongestPath(GraphNode *start, GraphNode *end,
                GraphNode::GraphNodeList &path);

        void saveToFile(const std::string filename,
                const GraphNode::GraphNodeSet *cpnodes);
    protected:
        NodeList nodes;
        NodeEdges inEdges, outEdges;
        
        static GraphNode *getNearestNode(const GraphNode::GraphNodeSet &nodes,
                std::map<GraphNode*, uint64_t> &distances);
    };

}

#endif	/* GRAPH_HPP */

