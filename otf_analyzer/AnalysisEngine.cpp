/* 
 * File:   AnalysisEngine.cpp
 * Author: felix
 * 
 * Created on May 15, 2013, 2:14 PM
 */

#include <stdio.h>
#include <mpi.h>

#include "AnalysisEngine.hpp"
#include "common.hpp"
#include "otf/OTF1ParallelTraceWriter.hpp"

using namespace cdm;
using namespace cdm::io;

AnalysisEngine::AnalysisEngine(uint32_t mpiRank, uint32_t mpiSize) :
mpiAnalysis(mpiRank, mpiSize),
maxFunctionId(0),
waitStateFuncId(0)
{
}

AnalysisEngine::~AnalysisEngine()
{
    for (std::vector<AbstractRule*>::iterator iter = rules.begin();
            iter != rules.end(); ++iter)
    {
        delete (*iter);
    }
}

MPIAnalysis& AnalysisEngine::getMPIAnalysis()
{
    return mpiAnalysis;
}

uint32_t AnalysisEngine::getMPIRank() const
{
    return mpiAnalysis.getMPIRank();
}

#ifdef MPI_CP_MERGE
void AnalysisEngine::mergeMPIGraphs()
{
    mpiAnalysis.mergeMPIGraphs(this);
}
#endif

bool AnalysisEngine::rulePriorityCompare(AbstractRule *r1, AbstractRule *r2)
{
    // sort in descending order
    return r2->getPriority() < r1->getPriority();
}

void AnalysisEngine::addFunction(uint32_t funcId, const char *name)
{
    maxFunctionId = std::max(maxFunctionId, funcId);
    functionMap[funcId] = name;
}

uint32_t AnalysisEngine::getNewFunctionId()
{
    return ++maxFunctionId;
}

void AnalysisEngine::setWaitStateFunctionId(uint32_t id)
{
    waitStateFuncId = id;
    functionMap[waitStateFuncId] = "WaitState";
}

const char *AnalysisEngine::getFunctionName(uint32_t id)
{
    std::map<uint32_t, std::string>::const_iterator iter =
            functionMap.find(id);
    if (iter != functionMap.end())
        return iter->second.c_str();
    else
        return NULL;
}

void AnalysisEngine::addRule(AbstractRule *rule)
{
    rules.push_back(rule);
    std::sort(rules.begin(), rules.end(), rulePriorityCompare);
}

void AnalysisEngine::removeRules()
{
    for (std::vector<AbstractRule*>::iterator iter = rules.begin();
            iter != rules.end(); ++iter)
    {
        delete (*iter);
    }
    rules.clear();
}

bool AnalysisEngine::applyRules(Node *node, bool verbose)
{
    VT_TRACER("applyRules");
    bool ruleResult = false;
    for (std::vector<AbstractRule*>::iterator iter = rules.begin();
            iter != rules.end(); ++iter)
    {
        if ((*iter)->apply(this, node))
        {
            if (verbose)
            {
                printf("[%u] * Applied %s to %s (%f)\n",
                        mpiAnalysis.getMPIRank(),
                        (*iter)->getName(),
                        node->getUniqueName().c_str(),
                        getRealTime(node->getTime()));
            }
            ruleResult = true;
        }
    }

    return ruleResult;
}

Process *AnalysisEngine::getNullStream() const
{
    return allocation.getNullStream();
}

void AnalysisEngine::setLastEventLaunch(EventNode *eventLaunchLeave)
{
    eventLaunchMap[eventLaunchLeave->getEventId()] = eventLaunchLeave;
}

EventNode *AnalysisEngine::consumeLastEventLaunchLeave(uint32_t eventId)
{
    IdEventNodeMap::iterator iter = eventLaunchMap.find(eventId);
    if (iter != eventLaunchMap.end())
    {
        EventNode *node = iter->second;
        eventLaunchMap.erase(iter);
        return node;
    } else
        return NULL;
}

EventNode *AnalysisEngine::getLastEventLaunchLeave(uint32_t eventId) const
{
    IdEventNodeMap::const_iterator iter = eventLaunchMap.find(eventId);
    if (iter != eventLaunchMap.end())
    {
        return iter->second;
    } else
        return NULL;
}

void AnalysisEngine::setEventProcessId(uint32_t eventId, uint32_t processId)
{
    eventProcessMap[eventId] = processId;
}

uint32_t AnalysisEngine::getEventProcessId(uint32_t eventId) const
{
    IdIdMap::const_iterator iter = eventProcessMap.find(eventId);
    if (iter != eventProcessMap.end())
    {
        return iter->second;
    } else
        return 0;
}

void AnalysisEngine::addPendingKernelLaunch(GraphNode* launch)
{
    // append at tail (FIFO)
    pendingKernelLaunchMap[launch->getReferencedProcessId()].push_back(launch);
}

GraphNode* AnalysisEngine::consumePendingKernelLaunch(uint32_t kernelProcessId)
{
    KernelLaunchListMap::iterator listIter = pendingKernelLaunchMap.find(kernelProcessId);
    if (listIter == pendingKernelLaunchMap.end())
        return NULL;

    if (listIter->second.size() == 0)
        return NULL;

    // consume from head (FIFO)
    // listIter->second contains enter and leave records
    GraphNode::GraphNodeList::iterator launchIter = listIter->second.begin();
    while ((launchIter != listIter->second.end()) && ((*launchIter)->isLeave()))
        launchIter++;

    // found no enter record
    if (launchIter == listIter->second.end())
        return NULL;

    // erase this enter record
    GraphNode *kernelLaunch = *launchIter;
    listIter->second.erase(launchIter);
    return kernelLaunch;
}

void AnalysisEngine::addStreamWaitEvent(uint32_t waitingDeviceProcId, EventNode *streamWaitLeave)
{
    Process *nullStream = getNullStream();
    if (nullStream && nullStream->getId() == waitingDeviceProcId)
    {
        StreamWaitTagged *swTagged = new StreamWaitTagged();
        swTagged->node = streamWaitLeave;
        nullStreamWaits.push_front(swTagged);
    } else
    {
        // Remove any pending streamWaitEvent with the same event ID since they
        // it is replaced by this new streamWaitLeave.
        EventNode::EventNodeList &eventNodeList = streamWaitMap[waitingDeviceProcId];
        for (EventNode::EventNodeList::iterator iter = eventNodeList.begin();
                iter != eventNodeList.end(); ++iter)
        {
            if ((*iter)->getEventId() == streamWaitLeave->getEventId())
            {
                eventNodeList.erase(iter);
                break;
            }
        }
        
        streamWaitMap[waitingDeviceProcId].push_back(streamWaitLeave);
    }
}

EventNode *AnalysisEngine::getFirstStreamWaitEvent(uint32_t waitingDeviceProcId)
{
    IdEventsListMap::iterator iter = streamWaitMap.find(waitingDeviceProcId);
    // no direct streamWaitEvent found, test if one references a NULL stream
    if (iter == streamWaitMap.end())
    {
        // test if a streamWaitEvent on NULL is not tagged for this device process
        size_t numAllDevProcs = getNumAllDeviceProcesses();
        for (NullStreamWaitList::iterator nullIter = nullStreamWaits.begin();
                nullIter != nullStreamWaits.end();)
        {
            NullStreamWaitList::iterator currentIter = nullIter;
            StreamWaitTagged* swTagged = *currentIter;
            // remove streamWaitEvents that have been tagged by all device processes
            if (swTagged->tags.size() == numAllDevProcs)
            {
                delete (*nullIter);
                nullStreamWaits.erase(nullIter);
            } else
            {
                // if a streamWaitEvent on null stream has not been tagged for 
                // waitingDeviceProcId yet, return its node
                if (swTagged->tags.find(waitingDeviceProcId) == swTagged->tags.end())
                {
                    return swTagged->node;
                }
            }

            ++nullIter;
        }

        return NULL;
    }

    return *(iter->second.begin());
}

EventNode *AnalysisEngine::consumeFirstStreamWaitEvent(uint32_t waitingDeviceProcId)
{
    IdEventsListMap::iterator iter = streamWaitMap.find(waitingDeviceProcId);
    // no direct streamWaitEvent found, test if one references a NULL stream
    if (iter == streamWaitMap.end())
    {
        // test if a streamWaitEvent on NULL is not tagged for this device process
        size_t numAllDevProcs = getNumAllDeviceProcesses();
        for (NullStreamWaitList::iterator nullIter = nullStreamWaits.begin();
                nullIter != nullStreamWaits.end();)
        {
            NullStreamWaitList::iterator currentIter = nullIter;
            StreamWaitTagged* swTagged = *currentIter;
            // remove streamWaitEvents that have been tagged by all device processes
            if (swTagged->tags.size() == numAllDevProcs)
            {
                delete (*nullIter);
                nullStreamWaits.erase(nullIter);
            } else
            {
                // if a streamWaitEvent on null stream has not been tagged for 
                // waitingDeviceProcId yet, tag it and return its node
                if (swTagged->tags.find(waitingDeviceProcId) == swTagged->tags.end())
                {
                    swTagged->tags.insert(waitingDeviceProcId);
                    return swTagged->node;
                }
            }

            ++nullIter;
        }

        return NULL;
    }

    EventNode *node = *(iter->second.begin());
    iter->second.pop_front();
    if (iter->second.size() == 0)
        streamWaitMap.erase(iter);
    return node;
}

void AnalysisEngine::linkEventQuery(EventNode *eventQueryLeave)
{
    EventNode *lastEventQueryLeave = NULL;

    IdEventNodeMap::iterator iter = eventQueryMap.find(eventQueryLeave->getEventId());
    if (iter != eventQueryMap.end())
    {
        lastEventQueryLeave = iter->second;
    }

    eventQueryLeave->setLink(lastEventQueryLeave);
    eventQueryMap[eventQueryLeave->getEventId()] = eventQueryLeave;
}

void AnalysisEngine::removeEventQuery(uint32_t eventId)
{
    eventQueryMap.erase(eventId);
}

GraphNode *AnalysisEngine::getLastLaunchLeave(uint64_t timestamp,
        uint32_t deviceProcId) const
{
    VT_TRACER("getLastLaunchLeave");
    // find last kernel launch (leave record) which launched on 
    // deviceProcId and happened before timestamp
    GraphNode *lastLaunchLeave = NULL;

    for (KernelLaunchListMap::const_iterator listIter = pendingKernelLaunchMap.begin();
            listIter != pendingKernelLaunchMap.end(); ++listIter)
    {
        for (GraphNode::GraphNodeList::const_reverse_iterator launchIter = listIter->second.rbegin();
                launchIter != listIter->second.rend(); ++launchIter)
        {
            GraphNode *gLaunchLeave = *launchIter;

            if (gLaunchLeave->isEnter())
                continue;

            uint32_t refDeviceProcessId =
                    gLaunchLeave->getGraphPair().first->getReferencedProcessId();

            // found the last kernel launch (leave) on this process, break
            if ((refDeviceProcessId == deviceProcId) &&
                    (gLaunchLeave->getTime() <= timestamp))
            {
                // if this is the latest kernel launch leave so far, remember it
                if (!lastLaunchLeave || (gLaunchLeave->getTime() > lastLaunchLeave->getTime()))
                    lastLaunchLeave = gLaunchLeave;
                break;
            }
        }
    }

    return lastLaunchLeave;
}

GraphNode *AnalysisEngine::getLastLeave(uint64_t timestamp, uint32_t procId) const
{
    // find last leave record on process procId before timestamp
    Process *process = getProcess(procId);
    if (!process)
        return NULL;

    Process::SortedNodeList &nodes = process->getNodes();
    for (Process::SortedNodeList::const_reverse_iterator rIter = nodes.rbegin();
            rIter != nodes.rend(); ++rIter)
    {
        Node *node = *rIter;
        if (!node->isLeave() || node->isMPI() || !node->isGraphNode())
            continue;

        if (node->getTime() <= timestamp)
            return (GraphNode*) node;
    }

    return NULL;
}

GraphNode* AnalysisEngine::newGraphNode(uint64_t time, uint32_t processId,
        const std::string name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType)
{
    GraphNode *node = TraceData::newGraphNode(time, processId, name,
            paradigm, recordType, nodeType);
    
    if (node->isWaitstate())
    {
        node->setFunctionId(waitStateFuncId);
    }
    return node;
}

GraphNode* AnalysisEngine::addNewGraphNode(uint64_t time, Process *process,
        const char *name, Paradigm paradigm, NodeRecordType recordType,
        int nodeType, ParadigmEdgeMap *resultEdges)
{
    GraphNode *node = TraceData::addNewGraphNode(time, process, name, paradigm,
            recordType, nodeType, resultEdges);
    
    if (node->isWaitstate())
    {
        node->setFunctionId(waitStateFuncId);
    }
    return node;
}

RemoteGraphNode *AnalysisEngine::addNewRemoteNode(uint64_t time,
        uint32_t remoteProcId, uint32_t remoteNodeId, Paradigm paradigm,
        NodeRecordType recordType, int nodeType, uint32_t mpiRank)
{
    RemoteGraphNode *node = new RemoteGraphNode(time, remoteProcId,
            remoteNodeId, mpiRank, paradigm, recordType, nodeType);

    graph.addNode(node);

    return node;
}

void AnalysisEngine::reset()
{
    TraceData::reset();
    mpiAnalysis.reset();
}

uint64_t AnalysisEngine::getNodeSlack(const std::map<GraphNode*, uint64_t> slackMap,
        GraphNode *node)
{
    std::map<GraphNode*, uint64_t>::const_iterator iter = slackMap.find(node);
    if (iter != slackMap.end())
        return iter->second;
    else
        return 0;
}

uint64_t AnalysisEngine::updateGraphNode(GraphNode *node, uint64_t delta,
        uint64_t fixedSlack, bool verbose)
{
    // get maximal delta, which depends on delta, minimum of
    // all incoming edge durations ...
    /// \todo ... and the activities base value (minimal execution time)

    uint64_t maxSlack = std::numeric_limits<uint64_t>::max();

    const Graph::EdgeList& inEdges = getInEdges(node);
    const Graph::EdgeList& outEdges = getOutEdges(node);

    // max slack is minimum of all in-dependencies (except reverse edges)
    for (Graph::EdgeList::const_iterator iter = inEdges.begin();
            iter != inEdges.end(); ++iter)
    {
        Edge *edge = *iter;
        if (edge->getStartNode()->isAtomic())
            continue;

        maxSlack = std::min(edge->getDuration(), maxSlack);
        if (verbose)
        {
            printf("[%u]   - in-slack is %lu (%f) from %s\n",
                    getMPIRank(), edge->getDuration(),
                    getRealTime(edge->getDuration()),
                    edge->getStartNode()->getUniqueName().c_str());
        }
    }

    for (Graph::EdgeList::const_iterator iter = outEdges.begin();
            iter != outEdges.end(); ++iter)
    {
        Edge *edge = *iter;

        // wait states with inter-process dependencies tied to a specific event
        // (edge duration == 0) cannot be relaxed
        GraphNode::GraphNodePair& edgePair = edge->getNodes();
        if (edge->isInterProcessEdge() &&
                (edgePair.first->isWaitstate()) && (edge->getDuration() == 0))
        {
            maxSlack = 0;
            if (verbose)
                printf("[%u]   - has fixed dependency to %s, slack is %lu (%f)\n",
                    getMPIRank(), edgePair.second->getUniqueName().c_str(),
                    maxSlack, getRealTime(maxSlack));
            break;
        }
    }

    // compute applied delta from input delta and max possible slack
    if (fixedSlack)
        delta = fixedSlack;
    else
        delta = std::min(maxSlack, delta);

    if (delta > 0)
    {
        // reduce node timestamp
        if (verbose)
        {
            printf("[%u]   * moving %s by %lu (%f) to %lu (%f)\n",
                    getMPIRank(), node->getUniqueName().c_str(),
                    delta, getRealTime(delta),
                    node->getTime() - delta,
                    getRealTime(node->getTime() - delta));
        }
        node->reduceTimestamp(delta);

        // reduce incoming edges
        for (Graph::EdgeList::const_iterator iter = inEdges.begin();
                iter != inEdges.end(); ++iter)
        {
            Edge *edge = *iter;
            uint64_t duration = 0;
            if (edge->getDuration() > delta)
                duration = edge->getDuration() - delta;

            if (verbose)
            {
                printf("[%u]   - updating in-edge %s from %lu (%f) to %lu (%f)\n",
                        getMPIRank(), edge->getName().c_str(),
                        edge->getDuration(), getRealTime(edge->getDuration()),
                        duration, getRealTime(duration));
            }

            edge->setDuration(duration);
        }
    }

    return delta;
}

uint64_t AnalysisEngine::updateInEdges(const Graph::EdgeList& inEdges, bool verbose)
{
    uint64_t fixedSlack = 0;
    for (Graph::EdgeList::const_iterator iter = inEdges.begin();
            iter != inEdges.end(); ++iter)
    {
        Edge *edge = *iter;
        if (edge->isReverseEdge())
            continue;

        uint64_t realDuration = edge->getEndNode()->getTime() -
                edge->getStartNode()->getTime();
        if (realDuration < edge->getDuration())
        {
            throw RTException(
                    "Node times or edge duration are invalid for edge %s",
                    edge->getName().c_str());
        }

        uint64_t edgeDelta = realDuration - edge->getDuration();
        if (edgeDelta > 0)
        {
            if (verbose)
            {
                printf("[%u]   - updating in-edge %s from %lu (%f) to %lu (%f)\n",
                        getMPIRank(), edge->getName().c_str(),
                        edge->getDuration(), getRealTime(edge->getDuration()),
                        realDuration, getRealTime(realDuration));
            }

            edge->setDuration(realDuration);

            if (edge->isIntraProcessEdge() &&
                    getProcess(edge->getStartNode()->getProcessId())->isHostProcess())
            {
                if (fixedSlack)
                    throw RTException("Node has more than one intra-process edge");

                fixedSlack = edgeDelta;
                if (verbose)
                {
                    printf("[%u]   - edge %s has fixed slack of %lu\n",
                            getMPIRank(), edge->getName().c_str(), fixedSlack);
                }
            }
        } else
        {
            if (verbose)
            {
                printf("[%u]   - in-edge %s remains constant\n",
                        getMPIRank(), edge->getName().c_str());
            }
        }
    }

    return fixedSlack;
}

void AnalysisEngine::optimizeKernel(std::map<uint32_t, double> optimizationMap, bool verbose)
{
    // get all local processes
    Allocation::ProcessList processes;
    getLocalProcesses(processes);
    size_t numProcesses = processes.size();
    uint32_t mpiRank = mpiAnalysis.getMPIRank();

    typedef Process::SortedNodeList::const_iterator NodeIter;

    // current node for each process
    NodeIter *currentNodeMap = new NodeIter[numProcesses];
    // slack for each node
    std::map<GraphNode*, uint64_t> slackMap;
    // mapping from process ID to index in process list
    std::map<uint32_t, size_t> processIndexMap;
    // set of already processed nodes
    GraphNode::GraphNodeSet processedNodes;
    bool nodesRemaining = true;

    uint32_t lastPercentage = 0;

    // initialize stop nodes to begin of each process
    for (size_t i = 0; i < numProcesses; ++i)
    {
        currentNodeMap[i] = processes[i]->getNodes().begin();
        processIndexMap[processes[i]->getId()] = i;
    }

    uint64_t totalNumNodes = 0;
    for (size_t i = 0; i < numProcesses; ++i)
        totalNumNodes += processes[i]->getNodes().size();

    // traverse nodes of all processes while work remaining
    while (nodesRemaining)
    {
        nodesRemaining = false;
        for (size_t i = 0; i < numProcesses; ++i)
        {
            if (verbose)
                printf("[%u] changing to process %s\n", mpiRank, processes[i]->getName());

            // traverse process till end or unresolved dependency is found
            bool foundDependency = false;
            Process::SortedNodeList &nodes = processes[i]->getNodes();
            while ((currentNodeMap[i] != nodes.end()) && !foundDependency)
            {
                // only interested in graph nodes
                if (!(*currentNodeMap[i])->isGraphNode())
                {
                    // go to next node in this process
                    printf("[%u] skipping %s\n", mpiRank, (*currentNodeMap[i])->toString());
                    currentNodeMap[i]++;
                    continue;
                }

                nodesRemaining = true;
                GraphNode *currentNode = (GraphNode*) (*currentNodeMap[i]);

                // test for ingoing dependencies
                bool hasIngoingDependency = false;
                GraphNode *dependencyNode = NULL;

                const Graph::EdgeList& inEdges = getInEdges(currentNode);
                if (!currentNode->isAtomic() && (inEdges.size() > 0))
                {
                    for (Graph::EdgeList::const_iterator iter = inEdges.begin();
                            iter != inEdges.end(); ++iter)
                    {
                        // check if at least one local input dependency is not resolved
                        GraphNode::GraphNodePair& pair = (*iter)->getNodes();

                        Process *srcProcess = getProcess(pair.first->getProcessId());
                        if (srcProcess && srcProcess->isRemoteProcess())
                        {
                            if (verbose)
                            {
                                printf("[%u]  - ignoring edge %s from remote (ghost) process\n",
                                        mpiRank, (*iter)->getName().c_str());
                            }
                            continue;
                        }

                        if (processedNodes.find(pair.first) == processedNodes.end())
                        {
                            hasIngoingDependency = true;
                            dependencyNode = pair.first;
                            break;
                        }
                    }
                }

                if (hasIngoingDependency)
                {
                    if (verbose)
                    {
                        printf("[%u]  - stopping at %s (depends on %s)\n",
                                mpiRank,
                                currentNode->getUniqueName().c_str(),
                                dependencyNode->getUniqueName().c_str());
                    }

                    // if this is a remote dependency, wait for other analysis
                    // process to resolve it
                    if (dependencyNode->isRemoteNode())
                    {
                        RemoteGraphNode *remNode = (RemoteGraphNode*) dependencyNode;

                        if (verbose)
                        {
                            printf("[%u]  > waiting for remote info for id %u from %u\n",
                                    mpiRank, remNode->getRemoteNodeId(), remNode->getMPIRank());
                        }

                        uint64_t recvBfr;
                        MPI_Status recvStatus;
                        MPI_CHECK(MPI_Recv(&recvBfr, 1, MPI_INTEGER8, remNode->getMPIRank(),
                                remNode->getRemoteNodeId(), MPI_COMM_WORLD, &recvStatus));

                        uint64_t delta = remNode->getTime() - recvBfr;
                        remNode->reduceTimestamp(delta);

                        if (verbose)
                        {
                            printf("[%u]  > received remote info for id %u from %u (%llu)\n",
                                    mpiRank, remNode->getRemoteNodeId(), remNode->getMPIRank(),
                                    (long long unsigned) recvBfr);
                        }

                        // mark as resolved
                        processedNodes.insert(remNode);
                    }

                    foundDependency = true;
                    break;
                }

                if (verbose)
                {
                    printf("[%u]  + processing %s [%f] (slack %lu (%f))\n",
                            mpiRank,
                            currentNode->getUniqueName().c_str(),
                            getRealTime(currentNode->getTime()),
                            getNodeSlack(slackMap, currentNode),
                            getRealTime(getNodeSlack(slackMap, currentNode)));
                }

                /* no unresolved dependencies, process node */

                // update ingoing edges from last round
                uint64_t fixedSlack = updateInEdges(inEdges, verbose);

                uint64_t nodeSlack = getNodeSlack(slackMap, currentNode);

                // test if function is marked for optimization
                if (!fixedSlack && currentNode->isLeave())
                {
                    std::map<uint32_t, double>::const_iterator optFactorIter =
                            optimizationMap.find(currentNode->getFunctionId());

                    if (optFactorIter != optimizationMap.end())
                    {
                        GraphNode::GraphNodePair& pair = currentNode->getGraphPair();
                        Edge *currentEdge = getEdge(pair.first, pair.second);
                        uint64_t opt_delta = currentEdge->getInitialDuration() -
                                (currentEdge->getInitialDuration() * optFactorIter->second);
                        slackMap[currentNode] = opt_delta + nodeSlack;
                        nodeSlack = slackMap[currentNode];
                        if (verbose)
                        {
                            printf("[%u]    + marked for optimization (%lu, %f)\n",
                                    mpiRank, opt_delta, getRealTime(opt_delta));
                        }
                    }
                }

                const Graph::EdgeList& outEdges = getOutEdges(currentNode);

                // update node
                if (nodeSlack > 0 || fixedSlack)
                {
                    uint64_t appliedDelta = updateGraphNode(currentNode,
                            nodeSlack, fixedSlack, verbose);

                    // add appliedDelta to slack of all depending processes

                    for (Graph::EdgeList::const_iterator iter = outEdges.begin();
                            iter != outEdges.end(); ++iter)
                    {
                        Edge *edge = *iter;
                        if (!edge->isReverseEdge())
                        {
                            GraphNode *endNode = edge->getEndNode();
                            uint64_t currentEndSlack = getNodeSlack(slackMap, endNode);
                            uint64_t newSlack = std::max(appliedDelta, currentEndSlack);

                            if (newSlack != currentEndSlack)
                            {
                                if (verbose)
                                {
                                    printf("[%u]    + updating slack of %s from %lu (%f) to %lu (%f)\n",
                                            mpiRank,
                                            endNode->getUniqueName().c_str(),
                                            currentEndSlack, getRealTime(currentEndSlack),
                                            newSlack, getRealTime(newSlack));
                                }
                                slackMap[endNode] = newSlack;
                            }
                        }
                    }
                }

                // if updated node has outgoing remote edges, notify remote analysis
                // processes on update
                for (Graph::EdgeList::const_iterator iter = outEdges.begin();
                        iter != outEdges.end(); ++iter)
                {
                    Edge *edge = *iter;
                    if (edge->isRemoteEdge() && (edge->getEndNode()->isRemoteNode()))
                    {
                        RemoteGraphNode *remoteNode = (RemoteGraphNode*) edge->getEndNode();
                        uint64_t sendBfr = currentNode->getTime();

                        if (verbose)
                        {
                            printf("[%u]  > sending remote info for id %u to %u (%llu)\n",
                                    getMPIRank(), currentNode->getId(), remoteNode->getMPIRank(),
                                    (long long unsigned) sendBfr);
                        }

                        MPI_Send(&sendBfr, 1, MPI_INTEGER8, remoteNode->getMPIRank(),
                                currentNode->getId(), MPI_COMM_WORLD);
                    }
                }

                // mark as processed (for dependency checking))
                processedNodes.insert(currentNode);
                slackMap.erase(currentNode);

                currentNodeMap[i]++;
                uint32_t percentage = (uint64_t) ((double) processedNodes.size() * 100.0 /
                        (double) totalNumNodes);
                if (percentage > lastPercentage + 9)
                {
                    printf("[%u] %lu/%lu nodes\n", mpiRank, processedNodes.size(), totalNumNodes);
                    lastPercentage = percentage;
                }
            }
        }
    }

    delete[] currentNodeMap;
}

size_t AnalysisEngine::getNumAllDeviceProcesses()
{
    return allocation.getNumProcesses() - allocation.getNumHostProcesses();
}

double AnalysisEngine::getRealTime(uint64_t t)
{
    return (double) t / (double) getTimerResolution();
}

void AnalysisEngine::saveParallelAllocationToFile(const char* filename,
        const char *origFilename,
        bool enableWaitStates, bool verbose)
{
    Allocation::ProcessList allProcs;
    getProcesses(allProcs);

    IParallelTraceWriter *writer = new OTF1ParallelTraceWriter(
            VT_CUPTI_CUDA_STREAMREF_KEY,
            VT_CUPTI_CUDA_EVENTREF_KEY,
            VT_CUPTI_CUDA_CURESULT_KEY,
            mpiAnalysis.getMPIRank(),
            mpiAnalysis.getMPISize(),
            origFilename);
    writer->open(filename, 100, allProcs.size(), ticksPerSecond);

    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    for (CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin();
            ctrIter != ctrIdSet.end(); ++ctrIter)
    {
        CtrTableEntry *entry = ctrTable.getCounter(*ctrIter);
        writer->writeDefCounter(*ctrIter,
                entry->name,
                OTF_COUNTER_TYPE_ABS | entry->otfMode);

    }

    for (Allocation::ProcessList::const_iterator pIter = allProcs.begin();
            pIter != allProcs.end(); ++pIter)
    {
        Process *p = *pIter;

        if (p->isRemoteProcess())
            continue;

        uint32_t pId = p->getId();
        if (verbose)
        {
            printf("[%u] def process %u (%s)\n",
                    mpiAnalysis.getMPIRank(),
                    pId, p->getName());
        }
        
        writer->writeDefProcess(pId, p->getParentId(), p->getName(),
                processTypeToGroup(p->getProcessType()));

        Process::SortedNodeList &nodes = p->getNodes();
        for (Process::SortedNodeList::const_iterator iter = nodes.begin();
                iter != nodes.end(); ++iter)
        {
            Node *node = *iter;

            if (node->isEnter() || node->isLeave())
            {
                if ((!node->isPureWaitstate()) || enableWaitStates)
                {
                    if (verbose)
                    {
                        printf("[%u] [%12lu:%12.8fs] %60s in %8u (FID %u)\n",
                                mpiAnalysis.getMPIRank(),
                                node->getTime(),
                                (double) (node->getTime()) / (double) ticksPerSecond,
                                node->getUniqueName().c_str(),
                                node->getProcessId(),
                                node->getFunctionId());
                    }

                    writer->writeNode(node, ctrTable, node == p->getLastGraphNode());

                    /*if (node->isGraphNode())
                    {
                        MPIAnalysis::CriticalSectionsMap::const_iterator sIter =
                                sectionsMap.find((GraphNode*) node);
                        if (sIter != sectionsMap.end())
                        {
                            MPIAnalysis::CriticalPathSection section = sIter->second;
                            writer->writeRMANode(node, section.prevProcessID,
                                    section.nextProcessID);
                        }
                    }*/
                }
            }

        }
    }

    writer->close();
    delete writer;
}