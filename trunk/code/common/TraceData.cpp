/* 
 * File:   TraceData.cpp
 * Author: felix
 * 
 * Created on May 8, 2013, 11:26 AM
 */

#include <stdio.h>
#include <utility>

#include "TraceData.hpp"
#include "FunctionTable.hpp"
#include "otf/OTF1TraceWriter.hpp"
#include "common.hpp"
#include "include/graph/Node.hpp"

using namespace cdm;
using namespace cdm::io;

TraceData::TraceData() :
ticksPerSecond(1000)
{
    globalSourceNode = newGraphNode(0, 0, "START", PARADIGM_ALL, RECORD_ATOMIC, MISC_PROCESS);

    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_BLAME);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_BLAME_LOG10);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_BLAME_STATISTICS);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_WAITSTATE);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_WAITSTATE_LOG10);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_CRITICALPATH);
    ctrTable.addDefaultCounter(ctrTable.getNewCtrId(), CTR_CRITICALPATH_TIME);
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

Process* TraceData::newProcess(uint64_t id, uint64_t parentId,
        const std::string name, Process::ProcessType processType,
        Paradigm paradigm, bool remoteProcess)
{
    Process *p = new Process(id, parentId, name, processType, remoteProcess);
    processMap[id] = p;

    if (processType == Process::PT_HOST)
    {
        GraphNode *startNode = newGraphNode(0, id, name, PARADIGM_ALL,
                RECORD_ATOMIC, MISC_PROCESS);
        p->addGraphNode(startNode, NULL);
        newEdge(globalSourceNode, startNode);

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

bool TraceData::getFunctionType(uint64_t id, const char *name, Process *process, FunctionDescriptor *descr)
{
    assert(name);
    assert(descr);
    assert(process);

    if (FunctionTable::getAPIFunctionType(name, descr))
    {
        switch (descr->paradigm)
        {
            case PARADIGM_CUDA:
                switch (descr->type)
                {
                    case CUDA_COLLSYNC:
                        descr->type = CUDA_COLLSYNC | CUDA_SYNC;
                        return true;

                    case CUDA_SYNC:
                    case CUDA_KERNEL_LAUNCH:
                    case CUDA_EV_LAUNCH:
                    case CUDA_EV_SYNC:
                    case CUDA_QUERY:
                    case CUDA_EV_QUERY:
                    case CUDA_STREAMWAIT:
                        return true;
                }

            case PARADIGM_MPI:
                switch (descr->type)
                {
                    case MPI_COLL:
                    case MPI_ONETOALL:
                    case MPI_ALLTOONE:
                    case MPI_WAIT:
                    case MPI_SENDRECV:
                    case MPI_RECV:
                    case MPI_SEND:
                    case MPI_MISC:
                        return true;
                }
                
            case PARADIGM_VT:
                switch (descr->type)
                {
                    case VT_FLUSH:
                        return true;
                }

            default:
                break;
        }
    }
    // not an MPI or CUDA API function

    if (strstr(name, "omp"))
    {
        descr->paradigm = PARADIGM_OMP;
        if (strstr(name, "barrier"))
            descr->type = OMP_SYNC;
        else
            descr->type = OMP_COMPUTE;
        return true;
    }

    if ((strstr(name, "parallel")) && (strstr(name, "region")))
    {
        descr->paradigm = PARADIGM_OMP;
        descr->type = OMP_PAR_REGION;
        return true;
    }

    // kernel ?
    if (process->isDeviceNullProcess())
    {
        descr->type = (CUDA_KERNEL | CUDA_SYNC | CUDA_COLLSYNC);
        descr->paradigm = PARADIGM_CUDA;
        return true;
    }

    if (process->isDeviceProcess())
    {
        descr->type = CUDA_KERNEL;
        descr->paradigm = PARADIGM_CUDA;
        return true;
    }

    // anything else
    descr->paradigm = PARADIGM_CPU;
    descr->type = MISC_CPU;
    return false;
}

Graph& TraceData::getGraph()
{
    return graph;
}

Graph* TraceData::getGraph(Paradigm p)
{
    return graph.getSubGraph(p);
}

Process* TraceData::getProcess(uint64_t id) const
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

void TraceData::getProcesses(Allocation::ProcessList& procs, Paradigm paradigm) const
{
    allocation.getAllProcesses(procs, paradigm);
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

Node* TraceData::newNode(uint64_t time, uint64_t processId,
        const std::string name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType)
{
    return new Node(time, processId, name, paradigm, recordType, nodeType);
}

GraphNode* TraceData::newGraphNode(uint64_t time, uint64_t processId,
        const std::string name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType)
{
    GraphNode *n = new GraphNode(time, processId, name, paradigm, recordType, nodeType);
    graph.addNode(n);
    return n;
}

EventNode* TraceData::newEventNode(uint64_t time, uint64_t processId,
        uint32_t eventId, EventNode::FunctionResultType fResult,
        const std::string name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType)
{
    EventNode *n = new EventNode(time,
            processId, eventId, fResult, name, paradigm, recordType, nodeType);
    graph.addNode(n);
    return n;
}

Edge* TraceData::newEdge(GraphNode* n1, GraphNode *n2, int properties,
        Paradigm *edgeType)
{
    Paradigm paradigm = PARADIGM_ALL;
    if (edgeType)
        paradigm = *edgeType;
    else
    {
        if (n1->getParadigm() == n2->getParadigm())
            paradigm = n1->getParadigm();
    }

    Edge *e = new Edge(n1, n2, n2->getTime() - n1->getTime(), properties, paradigm);
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
    return getLastGraphNode(PARADIGM_ALL);
}

GraphNode* TraceData::getFirstTimedGraphNode(Paradigm paradigm) const
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
                GraphNode *gn = (GraphNode*) n;
                if (gn->hasParadigm(paradigm))
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

GraphNode* TraceData::getLastGraphNode(Paradigm paradigm) const
{
    GraphNode *lastNode = NULL;
    Allocation::ProcessList procs;
    allocation.getAllProcesses(procs);

    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        Process *p = *iter;
        GraphNode *lastProcGNode = p->getLastGraphNode(paradigm);

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

void TraceData::saveAllocationToFile(std::string filename,
        bool enableWaitStates, bool verbose)
{
    Allocation::ProcessList allProcs;
    getProcesses(allProcs);

    ITraceWriter *writer = new OTF1TraceWriter(VT_CUPTI_CUDA_STREAMREF_KEY,
            VT_CUPTI_CUDA_EVENTREF_KEY, VT_CUPTI_CUDA_CURESULT_KEY);
    writer->open(filename.c_str(), 100, allProcs.size(), ticksPerSecond);

    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    std::set<uint64_t> knownFunctions;

    for (CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin();
            ctrIter != ctrIdSet.end(); ++ctrIter)
    {
        CtrTableEntry *entry = ctrTable.getCounter(*ctrIter);
        if (!entry->isInternal)
            writer->writeDefCounter(*ctrIter, entry->name, entry->otfMode);
    }

    for (Allocation::ProcessList::const_iterator pIter = allProcs.begin();
            pIter != allProcs.end(); ++pIter)
    {
        Process *p = *pIter;
        uint64_t pId = p->getId();
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
                printf("[%12lu:%12.8fs] %60s in %8lu (FID %lu)\n", node->getTime(),
                        (double) (node->getTime()) / (double) ticksPerSecond,
                        node->getUniqueName().c_str(),
                        node->getProcessId(), node->getFunctionId());
            }

            if (node->isEnter() || node->isLeave())
            {
                if (node->isEnter() || node->isLeave())
                {
                    uint64_t functionId = node->getFunctionId();
                    if (knownFunctions.find(functionId) == knownFunctions.end())
                    {
                        knownFunctions.insert(functionId);

                        ITraceWriter::FunctionGroup fg = ITraceWriter::FG_CUDA_API;
                        if (node->isCUDAKernel())
                            fg = ITraceWriter::FG_KERNEL;

                        if (node->isWaitstate())
                            fg = ITraceWriter::FG_WAITSTATE;

                        if (node->isCPU())
                            fg = ITraceWriter::FG_APPLICATION;

                        writer->writeDefFunction(functionId, node->getName(), fg);
                    }
                }

                if (!node->isWaitstate() || enableWaitStates)
                {
                    ++iter;
                    writer->writeNode(node, ctrTable, node == p->getLastGraphNode(),*iter);
                    --iter;
                }
            }

        }
    }

    writer->close();
    delete writer;
}

void TraceData::addNewGraphNodeInternal(GraphNode *node, Process *process,
        Edge::ParadigmEdgeMap *resultEdges)
{
    GraphNode::ParadigmNodeMap predNodeMap, nextNodeMap;
    typedef Edge* EdgePtr;
    EdgePtr edges[NODE_PARADIGM_COUNT];

    for (size_t p = 0; p < NODE_PARADIGM_COUNT; ++p)
    {
        edges[p] = NULL;
    }

    if (!process->getLastNode() || Node::compareLess(process->getLastNode(), node))
    {
        process->addGraphNode(node, &predNodeMap);
    } else
    {
        process->insertGraphNode(node, predNodeMap, nextNodeMap);
    }

    // to support nesting we use a stack to keep track of open activities
    GraphNode * stackNode = topGraphNodeStack(node->getProcessId());
            
    if (node->isLeave())
    {
        if(stackNode == NULL)
        {
            throw RTException("StackNode NULL and found leave event %s.\n",
                    node->getUniqueName().c_str());
        }
        else 
        {
            node->setPartner(stackNode);
            stackNode->setPartner(node);
                
            if (!process->isRemoteProcess())
                activities.push_back(new Activity(stackNode, node));

            popGraphNodeStack(node->getProcessId());
            
            // use the stack to get the caller/parent of this node
            node->setCaller(topGraphNodeStack(node->getProcessId()));
        }
    } 
    else if(node->isEnter())
    {
        // use the stack to get the caller/parent of this node
        node->setCaller(stackNode);
        pushGraphNodeStack(node,node->getProcessId());
    }
    
    for (size_t p_index = 0; p_index < NODE_PARADIGM_COUNT; ++p_index)
    {
        Paradigm paradigm = (Paradigm) (1 << p_index);
        GraphNode::ParadigmNodeMap::const_iterator predPnmIter = predNodeMap.find(paradigm);
        if (predPnmIter != predNodeMap.end())
        {
            GraphNode *predNode = predPnmIter->second;

            int edgeProp = EDGE_NONE;

            if (predNode->isEnter() && node->isLeave())
            {
                if (predNode->isWaitstate() && node->isWaitstate())
                    edgeProp |= EDGE_IS_BLOCKING;
            }
 
            edges[p_index] = newEdge(predNode, node, edgeProp, &paradigm);
            //std::cout << " linked to " << predNodes[g]->getUniqueName().c_str() << std::endl;

            GraphNode::ParadigmNodeMap::const_iterator nextPnmIter = nextNodeMap.find(paradigm);
            if (nextPnmIter != nextNodeMap.end())
            {
                GraphNode *nextNode = nextPnmIter->second;
                Edge *oldEdge = getEdge(predNode, nextNode);
                if (!oldEdge)
                {
                    graph.saveToFile("error_dump.dot", NULL);
                    throw RTException("No edge between %s (p %u) and %s (p %u)",
                            predNode->getUniqueName().c_str(),
                            predNode->getProcessId(),
                            nextNode->getUniqueName().c_str(),
                            nextNode->getProcessId());
                }
                removeEdge(oldEdge);
                // can't be a blocking edge, as we never insert leave nodes
                // before enter nodes from the same function
                newEdge(node, nextNode, EDGE_NONE, &paradigm);
            }
        }

        if (resultEdges)
            resultEdges->insert(std::make_pair(paradigm, edges[p_index]));
    }
}

GraphNode *TraceData::addNewGraphNode(uint64_t time, Process *process,
        const char *name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType, Edge::ParadigmEdgeMap *resultEdges)
{
    GraphNode *node = newGraphNode(time, process->getId(), name,
            paradigm, recordType, nodeType);
    addNewGraphNodeInternal(node, process, resultEdges);

    return node;
}

EventNode *TraceData::addNewEventNode(uint64_t time, uint32_t eventId,
        EventNode::FunctionResultType fResult, Process *process,
        const char *name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType, Edge::ParadigmEdgeMap *resultEdges)
{
    EventNode *node = newEventNode(time, process->getId(), eventId,
            fResult, name, paradigm, recordType, nodeType);
    addNewGraphNodeInternal(node, process, resultEdges);
    return node;
}

GraphNode* TraceData::topGraphNodeStack(uint64_t processId){
    if(pendingGraphNodeStackMap[processId].empty())
        return NULL;
    return pendingGraphNodeStackMap[processId].top();
}

void TraceData::popGraphNodeStack(uint64_t processId){
    pendingGraphNodeStackMap[processId].pop();
}

void TraceData::pushGraphNodeStack(GraphNode* node, uint64_t processId){
    pendingGraphNodeStackMap[processId].push(node);
}