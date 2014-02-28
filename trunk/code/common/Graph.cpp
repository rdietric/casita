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

Graph::EdgeList Graph::getInEdges(GraphNode *node, Paradigm paradigm) const
{
    EdgeList edges;
    NodeEdges::const_iterator iter = inEdges.find(node);
    if (iter != inEdges.end())
    {
        for (EdgeList::const_iterator eIter = iter->second.begin();
                eIter != iter->second.end(); ++eIter)
        {
            if ((*eIter)->getStartNode()->hasParadigm(paradigm))
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

Graph::EdgeList Graph::getOutEdges(GraphNode *node, Paradigm paradigm) const
{
    EdgeList edges;
    NodeEdges::const_iterator iter = outEdges.find(node);
    if (iter != outEdges.end())
    {
        for (EdgeList::const_iterator eIter = iter->second.begin();
                eIter != iter->second.end(); ++eIter)
        {
            if ((*eIter)->getEndNode()->hasParadigm(paradigm))
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

Graph *Graph::getSubGraph(Paradigm paradigm)
{
    VT_TRACER("getSubGraph");
    Graph *subGraph = new Graph();

    for (NodeList::const_iterator iter = nodes.begin();
            iter != nodes.end(); ++iter)
    {
        GraphNode *node = *iter;

        if (!node->hasParadigm(paradigm))
            continue;

        subGraph->addNode(node);

        if (hasOutEdges(node))
        {
            EdgeList edges = getOutEdges(node, paradigm);
            for (EdgeList::const_iterator eIter = edges.begin();
                    eIter != edges.end(); ++eIter)
            {
                Edge *edge = *eIter;

                if (edge->hasEdgeType(paradigm))
                    subGraph->addEdge(*eIter);
            }
        }
    }

    return subGraph;
}

bool Graph::compareDistancesLess(GraphNode *n1, GraphNode *n2,
        DistanceMap& distanceMap)
{
    assert(n1);
    assert(n2);

    uint64_t dist1 = distanceMap[n1];
    uint64_t dist2 = distanceMap[n2];

    if (dist1 != dist2)
        return dist1 < dist2;
    else
        return n1->getId() < n2->getId();
}

void Graph::sortedInsert(GraphNode *n, std::list<GraphNode*> &nodes,
        DistanceMap& distanceMap)
{
    uint64_t distance_n = distanceMap[n];

    std::list<GraphNode*>::iterator iter = nodes.begin();
    while (iter != nodes.end())
    {
        std::list<GraphNode*>::iterator current = iter;
        ++iter;
        
        if (iter == nodes.end() || distanceMap[*iter] >= distance_n)
        {
            nodes.insert(current, n);
            return;
        }
    }

    nodes.push_front(n);
}

void Graph::getLongestPath(GraphNode *start, GraphNode *end,
        GraphNode::GraphNodeList & path) const
{
    VT_TRACER("getLongestPath");

    const uint64_t infinite = std::numeric_limits<uint64_t>::max();
    std::map<GraphNode*, GraphNode*> preds;
    std::map<GraphNode*, uint64_t> distance;
    std::list<GraphNode*> pendingNodes;

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

        if (gn != start)
            pendingNodes.push_back(gn);
    }

    distance[start] = 0;
    pendingNodes.push_front(start);

    // pendingNodes is already sorted after construction

    while (!pendingNodes.empty())
    {
        VT_TRACER("getLongestPath:processNode");
        GraphNode *current_node = pendingNodes.front();
        uint64_t current_node_dist = distance[current_node];
        if (current_node_dist == infinite)
            break;

        pendingNodes.pop_front();
        distance.erase(current_node);

        if (hasOutEdges(current_node))
        {
            //VT_TRACER("getLongestPath:hasOutEdges");
            const Graph::EdgeList &outEdges = getOutEdges(current_node);

            for (Graph::EdgeList::const_iterator iter = outEdges.begin();
                    iter != outEdges.end(); ++iter)
            {
                Edge *edge = *iter;
                GraphNode *neighbour = edge->getEndNode();
                if (((neighbour == end) || (neighbour->getTime() < endTime))
                        && (distance.find(neighbour) != distance.end())
                        && (!edge->isBlocking()) && (!edge->isReverseEdge()))
                {
                    uint64_t alt_distance = current_node_dist + edge->getWeight();
                    if (alt_distance < distance[neighbour])
                    {
                        pendingNodes.remove(neighbour);
                        distance[neighbour] = alt_distance;

                        sortedInsert(neighbour, pendingNodes, distance);
                        preds[neighbour] = current_node;
                    }
                }
            }
        }
    }

    GraphNode *currentNode = end;
    while (currentNode != start)
    {
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
            GraphNode *closestNode = NULL;
            for (GraphNode::GraphNodeList::const_iterator pInIter = possibleInNodes.begin();
                    pInIter != possibleInNodes.end(); ++pInIter)
            {
                if (!closestNode)
                    closestNode = *pInIter;
                else
                {
                    if (Node::compareLess(closestNode, *pInIter))
                        closestNode = *pInIter;
                }
            }
            // add current node to critical path and choose next current node
            path.push_front(currentNode);
            currentNode = closestNode;
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
