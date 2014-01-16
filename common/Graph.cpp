/* 
 * File:   Graph.cpp
 * Author: felix
 * 
 * Created on May 7, 2013, 3:25 PM
 */

#include <sstream>
#include <fstream>

#include "graph/Graph.hpp"
#include "graph/Edge.hpp"
#include "graph/GraphNode.hpp"
#include "common.hpp"

#include <boost/graph/graphviz.hpp>

using namespace cdm;

typedef boost::graph_traits<BoostGraphType>::edge_descriptor EdgeDescr;
typedef boost::graph_traits<BoostGraphType>::vertex_descriptor VertexDescr;
typedef std::pair<EdgeDescr, bool> EdgeResult;

Graph::Graph()
{

}

Graph::~Graph()
{

}

void Graph::addNode(GraphNode* node)
{
    nodes.push_back(node);
}

void Graph::addEdge(Edge* edge)
{
    inEdges[edge->getEndNode()].push_back(edge);
    outEdges[edge->getStartNode()].push_back(edge);
}

void Graph::removeEdge(Edge *edge)
{
    GraphNode *start = edge->getStartNode();
    GraphNode *end = edge->getEndNode();

    EdgeList &out_edges = outEdges[start];
    EdgeList &in_edges = inEdges[end];

    for (EdgeList::iterator iter = out_edges.begin();
            iter != out_edges.end(); ++iter)
    {
        if ((*iter)->getEndNode() == end)
        {
            out_edges.erase(iter);
            break;
        }
    }

    for (EdgeList::iterator iter = in_edges.begin();
            iter != in_edges.end(); ++iter)
    {
        if ((*iter)->getStartNode() == start)
        {
            in_edges.erase(iter);
            break;
        }
    }
}

bool Graph::hasInEdges(GraphNode *node) const
{
    return inEdges.find(node) != inEdges.end();
}

bool Graph::hasOutEdges(GraphNode *node) const
{
    return outEdges.find(node) != outEdges.end();
}

const Graph::EdgeList &Graph::getInEdges(GraphNode *node) const
{
    NodeEdges::const_iterator iter = inEdges.find(node);
    if (iter != inEdges.end())
        return iter->second;

    throw RTException("Node %s not found in in-edge list",
            node->getUniqueName().c_str());
}

Graph::EdgeList Graph::getInEdges(GraphNode *node, GraphNodeType g) const
{
    EdgeList edges;
    NodeEdges::const_iterator iter = inEdges.find(node);
    if (iter != inEdges.end())
    {
        for (EdgeList::const_iterator eIter = iter->second.begin();
                eIter != iter->second.end(); ++eIter)
        {
            if ((*eIter)->getStartNode()->hasGraphNodeType(g))
                edges.push_back(*eIter);
        }
    }

    return edges;
}

const Graph::EdgeList &Graph::getOutEdges(GraphNode *node) const
{
    NodeEdges::const_iterator iter = outEdges.find(node);
    if (iter != outEdges.end())
        return iter->second;

    throw RTException("Node %s not found in out-edge list",
            node->getUniqueName().c_str());
}

Graph::EdgeList Graph::getOutEdges(GraphNode *node, GraphNodeType g) const
{
    EdgeList edges;
    NodeEdges::const_iterator iter = outEdges.find(node);
    if (iter != outEdges.end())
    {
        for (EdgeList::const_iterator eIter = iter->second.begin();
                eIter != iter->second.end(); ++eIter)
        {
            if ((*eIter)->getEndNode()->hasGraphNodeType(g))
                edges.push_back(*eIter);
        }
    }

    return edges;
}

const Graph::NodeList &Graph::getNodes() const
{
    return nodes;
}

BoostGraphType *Graph::getBoostGraph(const std::set<GraphNode*> *cpnodes)
{
    // builds a boost graph on-the-fly
    BoostGraphType *g = new BoostGraphType();

    std::map<GraphNode*, VertexDescr> vertexMap;

    for (NodeList::const_iterator iter = nodes.begin();
            iter != nodes.end(); ++iter)
    {
        VertexDescr v = boost::add_vertex(*g);

        if ((*iter)->isMPI())
            boost::put(boost::vertex_color_t(), *g, v, "grey");
        else
            boost::put(boost::vertex_color_t(), *g, v, "black");

        boost::put(boost::vertex_name_t(), *g, v, (*iter)->getUniqueName().c_str());

        vertexMap[*iter] = v;
    }

    if (cpnodes)
    {
        for (std::set<GraphNode*>::const_iterator iter = cpnodes->begin();
                iter != cpnodes->end(); ++iter)
        {
            boost::put(boost::vertex_color_t(), *g, vertexMap[*iter], "red");
        }
    }

    for (NodeList::const_iterator iter = nodes.begin();
            iter != nodes.end(); ++iter)
    {
        if (hasInEdges(*iter))
        {
            const EdgeList& inEdges = getInEdges(*iter);
            for (EdgeList::const_iterator iter = inEdges.begin();
                    iter != inEdges.end(); ++iter)
            {
                Edge *edge = *iter;
                VertexDescr v1 = vertexMap[edge->getStartNode()];
                VertexDescr v2 = vertexMap[edge->getEndNode()];
                EdgeResult e = boost::add_edge(v1, v2, *g);

                boost::put(boost::edge_weight_t(), *g, e.first, edge->getWeight());
                boost::put(boost::edge_weight2_t(), *g, e.first, edge->getDuration());
                boost::put(boost::edge_color_t(), *g, e.first, "black");

                if (edge->isBlocking())
                    boost::put(boost::edge_color_t(), *g, e.first, "blue");

                if (edge->isReverseEdge())
                    boost::put(boost::edge_color_t(), *g, e.first, "green");
            }
        }
    }

    return g;
}

Graph *Graph::getSubGraph(GraphNodeType g)
{
    VT_TRACER("getSubGraph");
    Graph *subGraph = new Graph();

    for (NodeList::const_iterator iter = nodes.begin();
            iter != nodes.end(); ++iter)
    {
        GraphNode *node = *iter;
        if (!node->hasGraphNodeType(g))
            continue;

        subGraph->addNode(node);

        if (hasOutEdges(node))
        {
            EdgeList edges = getOutEdges(node, g);
            for (EdgeList::const_iterator eIter = edges.begin();
                    eIter != edges.end(); ++eIter)
            {
                Edge *edge = *eIter;

                if ((edge->getEdgeType() == GRAPH_MAX) || (edge->getEdgeType() == g))
                    subGraph->addEdge(*eIter);
            }
        }
    }

    return subGraph;
}

static bool compareDistancesLess(GraphNode *n1, GraphNode *n2)
{
    VT_TRACER("compareDistancesLess");

    if (!n1 || !n2)
        do
        {
            printf("compare to NULL\n");
        } while (0);
    std::map<GraphNode*, uint64_t> *distances =
            (std::map<GraphNode*, uint64_t> *)(n1->getData());

    uint64_t dist1 = (*distances)[n1];
    uint64_t dist2 = (*distances)[n2];

    if (dist1 != dist2)
        return dist1 < dist2;
    else
        return n1->getId() < n2->getId();
}

typedef struct
{

    bool operator()(GraphNode *n1, GraphNode *n2) const
    {
        return compareDistancesLess(n1, n2);
    }

} compareDistancesLess_t;

void Graph::getLongestPath(GraphNode *start, GraphNode *end,
        GraphNode::GraphNodeList & path)
{
    VT_TRACER("getLongestPath");
    typedef std::set<GraphNode*, compareDistancesLess_t> GraphNodeDistanceSet;

    const uint64_t infinite = std::numeric_limits<uint64_t>::max();
    std::map<GraphNode*, GraphNode*> preds;
    std::map<GraphNode*, uint64_t> distance;
    GraphNodeDistanceSet pendingNodes;

    const uint64_t startTime = start->getTime();
    const uint64_t endTime = end->getTime();

    for (Graph::NodeList::const_iterator iter = nodes.begin();
            iter != nodes.end(); ++iter)
    {
        uint64_t nodeTime = (*iter)->getTime();
        if (nodeTime < startTime || nodeTime > endTime)
            continue;

        GraphNode *gn = *iter;
        distance[gn] = infinite;
        preds[gn] = gn;
        gn->setData(&distance);
        pendingNodes.insert(gn);
    }

    pendingNodes.erase(start);
    distance[start] = 0;
    pendingNodes.insert(start);

    while (pendingNodes.size() > 0)
    {
        VT_TRACER("getLongestPath:processNode");
        GraphNode *current_node = *(pendingNodes.begin());
        if (distance[current_node] == infinite)
            break;

        pendingNodes.erase(current_node);

        if (hasOutEdges(current_node))
        {
            VT_TRACER("getLongestPath:hasOutEdges");
            const Graph::EdgeList &outEdges = getOutEdges(current_node);

            for (Graph::EdgeList::const_iterator iter = outEdges.begin();
                    iter != outEdges.end(); ++iter)
            {
                Edge *edge = *iter;
                GraphNode *neighbour = edge->getEndNode();
                if (((neighbour == end) || (neighbour->getTime() < endTime))
                        && (pendingNodes.find(neighbour) != pendingNodes.end())
                        && (!edge->isBlocking()) && (!edge->isReverseEdge()))
                {
                    uint64_t alt_distance = distance[current_node] + edge->getWeight();
                    if (alt_distance < distance[neighbour])
                    {
                        pendingNodes.erase(neighbour);
                        distance[neighbour] = alt_distance;
                        pendingNodes.insert(neighbour);
                        preds[neighbour] = current_node;
                    }
                }
            }
        }
    }

    GraphNode *currentNode = end;
    while (currentNode != start)
    {
        VT_TRACER("getLongestPath:findBestPath");
        // get all ingoing nodes for current node, ignore blocking and
        // reverse edges
        GraphNode::GraphNodeList possibleInNodes;

        if (hasInEdges(currentNode))
        {
            const Graph::EdgeList &inEdges = getInEdges(currentNode);
            for (Graph::EdgeList::const_iterator eIter = inEdges.begin();
                    eIter != inEdges.end(); ++eIter)
            {
                Edge *edge = *eIter;
                if (!(edge->isBlocking()) && !(edge->isReverseEdge()))
                {
                    GraphNode *inNode = edge->getStartNode();

                    // if the targetNode is on a critical path, add it to the
                    // possible target nodes
                    GraphNode *pred = preds[inNode];
                    if (pred == start || pred != inNode)
                    {
                        possibleInNodes.push_back(inNode);
                    }

                }
            }
        }

        // choose among the ingoing nodes the one which is closest
        // as next current node
        if (possibleInNodes.size() > 0)
        {
            possibleInNodes.sort(Node::compareLess);
            // add current node to critical path and choose next current node
            path.push_front(currentNode);
            currentNode = *(possibleInNodes.rbegin());
        } else
            break;
    }
    path.push_front(start);
}

void Graph::saveToFile(const std::string filename, const std::set<GraphNode*> *cpnodes)
{
    BoostGraphType *g = getBoostGraph(cpnodes);

    boost::dynamic_properties dp;
    dp.property("node_id", boost::get(boost::vertex_index_t(), *g));
    dp.property("label", boost::get(boost::vertex_name_t(), *g));
    dp.property("color", boost::get(boost::vertex_color_t(), *g));

    dp.property("label", boost::get(boost::edge_weight2_t(), *g));
    dp.property("color", boost::get(boost::edge_color_t(), *g));

    std::ofstream file;
    file.open(filename.c_str());
    if (!file.is_open())
        printf("Failed to open output file for writing.");
    else
    {
        boost::write_graphviz_dp(file, *g, dp);
        file.close();
    }

    delete g;
}
