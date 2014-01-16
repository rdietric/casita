/* 
 * File:   TraceData.cpp
 * Author: felix
 * 
 * Created on May 8, 2013, 11:26 AM
 */

#include <stdio.h>

#include "TraceData.hpp"
#include "FunctionTable.hpp"
#include "otf/OTF1TraceWriter.hpp"
#include "common.hpp"

using namespace cdm;
using namespace cdm::io;

TraceData::TraceData() :
ticksPerSecond(1000)
{
    globalSourceNode = newGraphNode(0, 0, "START", NT_RT_ATOMIC | NT_FT_MIXED);

    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_BLAME);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_WAITSTATE);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_CRITICALPATH);
}

TraceData::~TraceData()
{
    for (ActivityList::iterator iter = activities.begin();
            iter != activities.end(); ++iter)
    {
        delete (*iter);
    }

    for (ProcessMap::iterator iter = processMap.begin();
            iter != processMap.end(); ++iter)
    {
        delete iter->second;
    }
}

Process* TraceData::newProcess(uint32_t id, uint32_t parentId,
        const std::string name, Process::ProcessType processType,
        GraphNodeType g, bool remoteProcess)
{
    Process *p = new Process(id, parentId, name, processType, remoteProcess);
    processMap[id] = p;

    if (processType == Process::PT_HOST)
    {
        GraphNode *startNode = newGraphNode(0, id, name,
                NT_FT_PROCESS | NT_RT_ATOMIC | NT_FT_MIXED);
        p->addGraphNode(startNode, NULL, NULL);
        newEdge(globalSourceNode, startNode, false);

        allocation.addHostProcess(p);
    } else
    {
        //e->setWeight(e->getWeight() - 1);

        if (processType == Process::PT_DEVICE)
            allocation.addDeviceProcess(p);
        else
            allocation.setNullStream(p);
    }

    return p;
}

int TraceData::getFunctionType(const char *name, Process *process)
{
    NodeType apiType = FunctionTable::getAPIFunctionType(name);

    switch (apiType)
    {
        case NT_FT_COLLSYNC:
            return NT_FT_COLLSYNC | NT_FT_SYNC;

        case NT_FT_SYNC:
        case NT_FT_LAUNCH:
        case NT_FT_EV_LAUNCH:
        case NT_FT_EV_SYNC:
        case NT_FT_QUERY:
        case NT_FT_EV_QUERY:
        case NT_FT_STREAMWAIT:

        case NT_FT_MPI_COLL:
        case NT_FT_MPI_WAIT:
        case NT_FT_MPI_SENDRECV:
        case NT_FT_MPI_RECV:
        case NT_FT_MPI_SEND:
        case NT_FT_MPI_MISC:
            return apiType;

        default:
            break;
    }
    // not a CUDA API function

    // kernel ?
    if (process->isDeviceNullProcess())
        return (NT_FT_KERNEL | NT_FT_SYNC | NT_FT_COLLSYNC);
    if (process->isDeviceProcess())
        return NT_FT_KERNEL;

    // anything else
    return NT_FT_CPU;
}

Graph& TraceData::getGraph()
{
    return graph;
}

Graph* TraceData::getGraph(GraphNodeType g)
{
    return graph.getSubGraph(g);
}

Process* TraceData::getProcess(uint32_t id) const
{
    ProcessMap::const_iterator iter = processMap.find(id);
    if (iter != processMap.end())
        return iter->second;
    else
        return NULL;
}

void TraceData::getProcesses(Allocation::ProcessList& procs) const
{
    allocation.getAllProcesses(procs);
}

void TraceData::getLocalProcesses(Allocation::ProcessList& procs) const
{
    allocation.getAllProcesses(procs);
    for (Allocation::ProcessList::iterator iter = procs.begin();
            iter != procs.end();)
    {
        if ((*iter)->isRemoteProcess())
        {
            iter = procs.erase(iter);
        } else
            ++iter;
    }
}

void TraceData::getProcesses(Allocation::ProcessList& procs, GraphNodeType g) const
{
    allocation.getAllProcesses(procs, g);
}

const Allocation::ProcessList& TraceData::getHostProcesses() const
{
    return allocation.getHostProcesses();
}

const Allocation::ProcessList& TraceData::getDeviceProcesses() const
{
    return allocation.getDeviceProcesses();
}

void TraceData::getAllDeviceProcesses(Allocation::ProcessList& deviceProcs) const
{
    allocation.getAllDeviceProcesses(deviceProcs);
}

bool TraceData::hasInEdges(GraphNode *n)
{
    return graph.hasInEdges(n);
}

bool TraceData::hasOutEdges(GraphNode *n)
{
    return graph.hasOutEdges(n);
}

const Graph::EdgeList& TraceData::getInEdges(GraphNode *n) const
{
    if (graph.hasInEdges(n))
        return graph.getInEdges(n);
    else
        return emptyEdgeList;
}

const Graph::EdgeList& TraceData::getOutEdges(GraphNode *n) const
{
    if (graph.hasOutEdges(n))
        return graph.getOutEdges(n);
    else
        return emptyEdgeList;
}

Node* TraceData::newNode(uint64_t time, uint32_t processId,
        const std::string name, int nodeType)
{
    return new Node(time, processId, name, nodeType);
}

GraphNode* TraceData::newGraphNode(uint64_t time, uint32_t processId,
        const std::string name, int nodeType)
{
    GraphNode *n = new GraphNode(time, processId, name, nodeType);
    graph.addNode(n);
    return n;
}

EventNode* TraceData::newEventNode(uint64_t time, uint32_t processId,
        uint32_t eventId, EventNode::FunctionResultType fResult,
        const std::string name, int nodeType)
{
    EventNode *n = new EventNode(time,
            processId, eventId, fResult, name, nodeType);
    graph.addNode(n);
    return n;
}

Edge* TraceData::newEdge(GraphNode* n1, GraphNode *n2, bool isBlocking,
        GraphNodeType *edgeType)
{
    GraphNodeType g = GRAPH_MAX;
    if (edgeType)
        g = *edgeType;
    else
    {
        if (n1->getGraphNodeType() == n2->getGraphNodeType())
            g = n1->getGraphNodeType();
    }

    Edge *e = new Edge(n1, n2, n2->getTime() - n1->getTime(), isBlocking, g);
    graph.addEdge(e);

    return e;
}

Edge* TraceData::getEdge(GraphNode *source, GraphNode *target)
{
    const Graph::EdgeList &edgeList = getOutEdges(source);
    for (Graph::EdgeList::const_iterator iter = edgeList.begin();
            iter != edgeList.end(); ++iter)
    {
        if ((*iter)->getEndNode() == target)
            return *iter;
    }

    return NULL;
}

void TraceData::removeEdge(Edge *e)
{
    graph.removeEdge(e);
    delete e;
}

GraphNode* TraceData::getSourceNode() const
{
    return globalSourceNode;
}

Node* TraceData::getLastNode() const
{
    Node *lastNode = NULL;
    Allocation::ProcessList procs;
    allocation.getAllProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Node *lastProcNode = (*iter)->getLastNode();
        if (lastProcNode)
        {
            if (lastNode == NULL)
                lastNode = lastProcNode;
            else
                if (lastProcNode->getTime() > lastNode->getTime())
                lastNode = lastProcNode;
        }
    }

    return lastNode;
}

GraphNode* TraceData::getLastGraphNode() const
{
    GraphNode *lastNode = NULL;

    for (int i = 0; i < GRAPH_MAX; ++i)
    {
        GraphNode *lastNodeG = getLastGraphNode((GraphNodeType) i);
        if (!lastNode)
            lastNode = lastNodeG;
        else
        {
            if (Node::compareLess(lastNode, lastNodeG))
                lastNode = lastNodeG;
        }
    }

    return lastNode;
}

GraphNode* TraceData::getFirstTimedGraphNode(GraphNodeType g) const
{
    GraphNode *firstNode = NULL;
    Allocation::ProcessList procs;
    allocation.getAllProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Process *p = *iter;
        Process::SortedNodeList &nodes = p->getNodes();
        GraphNode *firstProcGNode = NULL;
        
        for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                nIter != nodes.end(); ++nIter)
        {
            Node *n = *nIter;
            if ((n->isGraphNode()) && (n->getTime() > 0) && (!n->isAtomic()))
            {
                GraphNode *gn = (GraphNode*)n;
                if (gn->hasGraphNodeType(g))
                {
                    firstProcGNode = gn;
                    break;
                }
            }
        }

        if (firstProcGNode)
        {
            if (firstNode == NULL)
                firstNode = firstProcGNode;
            else
                if (firstProcGNode->getTime() < firstNode->getTime())
                firstNode = firstProcGNode;
        }
    }

    return firstNode;
}

GraphNode* TraceData::getLastGraphNode(GraphNodeType g) const
{
    GraphNode *lastNode = NULL;
    Allocation::ProcessList procs;
    allocation.getAllProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Process *p = *iter;
        GraphNode *lastProcGNode = p->getLastGraphNode(g);

        if (lastProcGNode)
        {
            if (lastNode == NULL)
                lastNode = lastProcGNode;
            else
                if (lastProcGNode->getTime() > lastNode->getTime())
                lastNode = lastProcGNode;
        }
    }

    return lastNode;
}

void TraceData::getAllNodes(Process::SortedNodeList& allNodes) const
{
    Allocation::ProcessList procs;
    getProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Process *p = *iter;
        if (p->getNodes().size() > 0)
            allNodes.insert(allNodes.end(), p->getNodes().begin(), p->getNodes().end());
    }

    std::sort(allNodes.begin(), allNodes.end(), Node::compareLess);
}

TraceData::ActivityList &TraceData::getActivities()
{
    return activities;
}

CounterTable &TraceData::getCtrTable()
{
    return ctrTable;
}

void TraceData::reset()
{
    resetCounters();

    const Allocation::ProcessList &hostProcs = allocation.getHostProcesses();
    for (Allocation::ProcessList::const_iterator iter = hostProcs.begin();
            iter != hostProcs.end();)
    {
        Process *p = *iter;

        if (p->isRemoteProcess())
        {
            Process::SortedNodeList& nodes = p->getNodes();
            for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                    nIter != nodes.end(); ++nIter)
            {
                if (!((*nIter)->isGraphNode()))
                    continue;

                GraphNode *node = (GraphNode*) (*nIter);
                const Graph::EdgeList& edges = graph.getInEdges(node);
                for (Graph::EdgeList::const_iterator eIter = edges.begin();
                        eIter != edges.end();)
                {
                    Graph::EdgeList::const_iterator next = eIter;
                    ++next;

                    delete *eIter;
                    graph.removeEdge(*eIter);

                    eIter = next;
                }
            }

            iter = allocation.removeHostProcess(p);
            delete p;
        } else
            ++iter;
    }
}

void TraceData::resetCounters()
{
    Allocation::ProcessList procs;
    getLocalProcesses(procs);

    for (Allocation::ProcessList::const_iterator pIter = procs.begin();
            pIter != procs.end(); ++pIter)
    {
        Process::SortedNodeList nodes = (*pIter)->getNodes();
        for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                nIter != nodes.end(); ++nIter)
        {
            (*nIter)->removeCounters();
        }
    }
}

uint64_t TraceData::getTimerResolution()
{
    return ticksPerSecond;
}

void TraceData::setTimerResolution(uint64_t ticksPerSecond)
{
    this->ticksPerSecond = ticksPerSecond;
}

uint64_t TraceData::getDeltaTicks()
{
    return getTimerResolution() * SYNC_DELTA / (1000 * 1000);
}

void TraceData::getCriticalPath(GraphNode *sourceNode, GraphNode * lastNode,
        GraphNode::GraphNodeList *cpath, GraphNodeType g)
{
    if (g == GRAPH_MAX)
        graph.getLongestPath(sourceNode, lastNode, *cpath);
    else
    {
        Graph *subgraph = graph.getSubGraph(g);
        subgraph->getLongestPath(sourceNode, lastNode, *cpath);
        delete subgraph;
    }
}

void TraceData::sanityCheckEdge(Edge *edge, uint32_t mpiRank)
{
    uint64_t expectedTime;
    if (edge->isReverseEdge())
        expectedTime = 0;
    else
        expectedTime = edge->getEndNode()->getTime() - edge->getStartNode()->getTime();

    if (edge->getDuration() != expectedTime)
    {
        throw RTException("[%u] Sanity check failed: edge %s has wrong duration (expected %lu, found %lu)",
                mpiRank, edge->getName().c_str(), expectedTime, edge->getDuration());
    }

    if (edge->isIntraProcessEdge() &&
            getProcess(edge->getStartNode()->getProcessId())->isHostProcess() &&
            edge->getDuration() != edge->getInitialDuration())
    {
        throw RTException("[%u] Sanity check failed: edge %s has not its initial duration",
                mpiRank, edge->getName().c_str());
    }

    if (!edge->isBlocking() && edge->getStartNode()->isWaitstate() &&
            edge->getStartNode()->isEnter() && edge->getEndNode()->isWaitstate() &&
            edge->getEndNode()->isLeave())
    {
        throw RTException("[%u] Sanity check failed: edge %s is not blocking but should be",
                mpiRank, edge->getName().c_str());
    }
}

void TraceData::runSanityCheck(uint32_t mpiRank)
{
    Allocation::ProcessList procs;
    getProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Process::SortedNodeList& nodes = (*iter)->getNodes();
        for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                nIter != nodes.end(); ++nIter)
        {
            if ((*nIter)->isGraphNode())
            {
                GraphNode *node = (GraphNode*) (*nIter);

                if (hasInEdges(node))
                {

                    Graph::EdgeList inEdges = getInEdges(node);
                    for (Graph::EdgeList::const_iterator eIter = inEdges.begin();
                            eIter != inEdges.end(); ++eIter)
                    {
                        sanityCheckEdge(*eIter, mpiRank);
                    }
                }

                if (hasOutEdges(node))
                {
                    Graph::EdgeList outEdges = getOutEdges(node);
                    for (Graph::EdgeList::const_iterator eIter = outEdges.begin();
                            eIter != outEdges.end(); ++eIter)
                    {
                        sanityCheckEdge(*eIter, mpiRank);
                    }
                }
            }
        }
    }
}

ITraceWriter::ProcessGroup TraceData::processTypeToGroup(Process::ProcessType pt)
{
    switch (pt)
    {
        case Process::PT_DEVICE:
            return ITraceWriter::PG_DEVICE;
        case Process::PT_DEVICE_NULL:
            return ITraceWriter::PG_DEVICE_NULL;
        default:
            return ITraceWriter::PG_HOST;
    }
}

void TraceData::saveAllocationToFile(const char* filename,
        bool enableWaitStates, bool verbose)
{
    Allocation::ProcessList allProcs;
    getProcesses(allProcs);

    ITraceWriter *writer = new OTF1TraceWriter(VT_CUPTI_CUDA_STREAMREF_KEY,
            VT_CUPTI_CUDA_EVENTREF_KEY, VT_CUPTI_CUDA_CURESULT_KEY);
    writer->open(filename, 100, allProcs.size(), ticksPerSecond);

    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    std::set<uint32_t> knownFunctions;

    for (CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin();
            ctrIter != ctrIdSet.end(); ++ctrIter)
    {
        writer->writeDefCounter(*ctrIter,
                ctrTable.getCounter(*ctrIter)->name,
                OTF_COUNTER_TYPE_ABS | OTF_COUNTER_SCOPE_NEXT);

    }

    for (Allocation::ProcessList::const_iterator pIter = allProcs.begin();
            pIter != allProcs.end(); ++pIter)
    {
        Process *p = *pIter;
        uint32_t pId = p->getId();
        writer->writeDefProcess(pId, p->getParentId(), p->getName(),
                processTypeToGroup(p->getProcessType()));

        Process::SortedNodeList &nodes = p->getNodes();
        for (Process::SortedNodeList::const_iterator iter = nodes.begin();
                iter != nodes.end(); ++iter)
        {
            Node *node = *iter;
            if (node->isMPI())
                continue;

            if (verbose)
            {
                printf("[%12lu:%12.8fs] %60s in %8u (FID %u)\n", node->getTime(),
                        (double) (node->getTime()) / (double) ticksPerSecond,
                        node->getUniqueName().c_str(),
                        node->getProcessId(), node->getFunctionId());
            }

            if (node->isEnter() || node->isLeave() || node->isMarker())
            {
                if (node->isEnter() || node->isLeave())
                {
                    uint32_t functionId = node->getFunctionId();
                    if (knownFunctions.find(functionId) == knownFunctions.end())
                    {
                        knownFunctions.insert(functionId);

                        ITraceWriter::FunctionGroup fg = ITraceWriter::FG_CUDA_API;
                        if (node->isKernel())
                            fg = ITraceWriter::FG_KERNEL;

                        if (node->isWaitstate())
                            fg = ITraceWriter::FG_WAITSTATE;

                        if (node->isCPU())
                            fg = ITraceWriter::FG_APPLICATION;

                        writer->writeDefFunction(functionId, node->getName(), fg);
                    }
                }

                if (!node->isWaitstate() || enableWaitStates)
                    writer->writeNode(node, ctrTable, node == p->getLastGraphNode());
            }

        }
    }

    writer->close();
    delete writer;
}

void TraceData::addNewGraphNodeInternal(GraphNode *node, Process *process,
        Edge **resultEdgeCUDA, Edge **resultEdgeMPI)
{
    GraphNodePtr predNodes[GRAPH_MAX];
    GraphNodePtr nextNodes[GRAPH_MAX];
    typedef Edge* EdgePtr;
    EdgePtr edges[GRAPH_MAX];

    for (int g = 0; g < GRAPH_MAX; ++g)
    {
        predNodes[g] = NULL;
        nextNodes[g] = NULL;
        edges[g] = NULL;
    }

    if (!process->getLastNode() || Node::compareLess(process->getLastNode(), node))
    {
        process->addGraphNode(node, &(predNodes[0]), &(predNodes[1]));
    } else
    {
        process->insertGraphNode(node, &(predNodes[0]), &(nextNodes[0]),
                &(predNodes[1]), &(nextNodes[1]));
    }

    //std::cout << "added node " << node->getUniqueName().c_str() << std::endl;

    for (int g = 0; g < GRAPH_MAX; ++g)
    {
        GraphNodeType gType = (GraphNodeType) g;
        if (predNodes[g])
        {
            if ((predNodes[g]->getGraphNodeType() == node->getGraphNodeType()) &&
                    ((predNodes[g]->isEnter() && node->isEnter()) ||
                    (predNodes[g]->isLeave() && node->isLeave())))
            {
                throw RTException("Node matching failed in process %u (%s): no matching "
                        "ENTER/LEAVE pair found, old: '%s' new: '%s'",
                        process->getId(),
                        process->getName(),
                        predNodes[g]->getUniqueName().c_str(),
                        node->getUniqueName().c_str());
            }

            bool isBlocking = false;

            if (predNodes[g]->isEnter() && node->isLeave())
            {
                node->setPartner(predNodes[g]);
                predNodes[g]->setPartner(node);

                isBlocking = (predNodes[g]->isWaitstate() && node->isWaitstate());

                if (!process->isRemoteProcess())
                    activities.push_back(new Activity(predNodes[g], node));
            }

            edges[g] = newEdge(predNodes[g], node, isBlocking, &gType);
            //std::cout << " linked to " << predNodes[g]->getUniqueName().c_str() << std::endl;

            if (nextNodes[g])
            {
                Edge *oldEdge = getEdge(predNodes[g], nextNodes[g]);
                if (!oldEdge)
                {
                    graph.saveToFile("error_dump.dot", NULL);
                    throw RTException("No edge between %s (p %u) and %s (p %u)",
                            predNodes[g]->getUniqueName().c_str(),
                            predNodes[g]->getProcessId(),
                            nextNodes[g]->getUniqueName().c_str(),
                            nextNodes[g]->getProcessId());
                }
                removeEdge(oldEdge);
                // can't be a blocking edge, as we never insert leave nodes
                // before enter nodes from the same function
                newEdge(node, nextNodes[g], false, &gType);
            }
        }
    }

    if (resultEdgeCUDA)
        *resultEdgeCUDA = edges[0];

    if (resultEdgeMPI)
        *resultEdgeMPI = edges[1];
}

GraphNode *TraceData::addNewGraphNode(uint64_t time, Process *process,
        const char *name, int nodeType, Edge **resultEdgeCUDA, Edge **resultEdgeMPI)
{
    GraphNode *node = newGraphNode(time, process->getId(), name, nodeType);
    addNewGraphNodeInternal(node, process, resultEdgeCUDA, resultEdgeMPI);

    return node;
}

EventNode *TraceData::addNewEventNode(uint64_t time, uint32_t eventId,
        EventNode::FunctionResultType fResult, Process *process,
        const char *name, int nodeType, Edge **resultEdgeCUDA, Edge **resultEdgeMPI)
{
    EventNode *node = newEventNode(time, process->getId(), eventId,
            fResult, name, nodeType);
    addNewGraphNodeInternal(node, process, resultEdgeCUDA, resultEdgeMPI);
    return node;
}