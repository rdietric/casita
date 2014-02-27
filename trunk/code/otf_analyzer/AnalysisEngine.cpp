/* 
 * File:   AnalysisEngine.cpp
 * Author: felix
 * 
 * Created on May 15, 2013, 2:14 PM
 */

#include <stdio.h>
#include <mpi.h>
#include <list>
#include <stack>

#include "AnalysisEngine.hpp"
#include "common.hpp"
#include "otf/OTF1TraceReader.hpp"
#include "otf/OTF1ParallelTraceWriter.hpp"

using namespace cdm;
using namespace cdm::io;

AnalysisEngine::AnalysisEngine(uint32_t mpiRank, uint32_t mpiSize) :
mpiAnalysis(mpiRank, mpiSize),
pendingParallelRegion(NULL),
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

void AnalysisEngine::addFunction(uint64_t funcId, const char *name)
{
    maxFunctionId = std::max(maxFunctionId, funcId);
    functionMap[funcId] = name;
}

uint64_t AnalysisEngine::getNewFunctionId()
{
    return ++maxFunctionId;
}

void AnalysisEngine::setWaitStateFunctionId(uint64_t id)
{
    waitStateFuncId = id;
    functionMap[waitStateFuncId] = "WaitState";
}

const char *AnalysisEngine::getFunctionName(uint64_t id)
{
    std::map<uint64_t, std::string>::const_iterator iter =
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

void AnalysisEngine::setEventProcessId(uint32_t eventId, uint64_t processId)
{
    eventProcessMap[eventId] = processId;
}

uint64_t AnalysisEngine::getEventProcessId(uint32_t eventId) const
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

GraphNode* AnalysisEngine::consumePendingKernelLaunch(uint64_t kernelProcessId)
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

void AnalysisEngine::addStreamWaitEvent(uint64_t waitingDeviceProcId, EventNode *streamWaitLeave)
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

EventNode *AnalysisEngine::getFirstStreamWaitEvent(uint64_t waitingDeviceProcId)
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

EventNode *AnalysisEngine::consumeFirstStreamWaitEvent(uint64_t waitingDeviceProcId)
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
        uint64_t deviceProcId) const
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

            uint64_t refDeviceProcessId =
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

GraphNode *AnalysisEngine::getLastLeave(uint64_t timestamp, uint64_t procId) const
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

GraphNode* AnalysisEngine::newGraphNode(uint64_t time, uint64_t processId,
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
        int nodeType, Edge::ParadigmEdgeMap *resultEdges)
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
        uint64_t remoteProcId, uint32_t remoteNodeId, Paradigm paradigm,
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

size_t AnalysisEngine::getNumAllDeviceProcesses()
{
    return allocation.getNumProcesses() - allocation.getNumHostProcesses();
}

double AnalysisEngine::getRealTime(uint64_t t)
{
    return (double) t / (double) getTimerResolution();
}

static bool processSort(Process* p1, Process* p2)
{
    if (p1->isDeviceProcess() && p2->isHostProcess())
        return false;
    
    if (p2->isDeviceProcess() && p1->isHostProcess())
        return true;
    
    return p1->getId() <= p2->getId();
}

void AnalysisEngine::saveParallelAllocationToFile(std::string filename,
        std::string origFilename, bool enableWaitStates, bool verbose)
{
    Allocation::ProcessList allProcs;
    getProcesses(allProcs);
    
    std::sort(allProcs.begin(), allProcs.end(), processSort);

    IParallelTraceWriter *writer = new OTF1ParallelTraceWriter(
            VT_CUPTI_CUDA_STREAMREF_KEY,
            VT_CUPTI_CUDA_EVENTREF_KEY,
            VT_CUPTI_CUDA_CURESULT_KEY,
            mpiAnalysis.getMPIRank(),
            mpiAnalysis.getMPISize(),
            origFilename.c_str());
    writer->open(filename.c_str(), 100, allProcs.size(), this->ticksPerSecond);

    CounterTable::CtrIdSet ctrIdSet = this->ctrTable.getAllCounterIDs();
    uint32_t cpCtrId = this->ctrTable.getCtrId(CTR_CRITICALPATH);
    for (CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin();
            ctrIter != ctrIdSet.end(); ++ctrIter)
    {
        CtrTableEntry *entry = this->ctrTable.getCounter(*ctrIter);
        if (!entry->isInternal)
            writer->writeDefCounter(*ctrIter, entry->name, entry->otfMode);
    }

    for (Allocation::ProcessList::const_iterator pIter = allProcs.begin();
            pIter != allProcs.end(); ++pIter)
    {
        Process *p = *pIter;

        if (p->isRemoteProcess())
            continue;

        uint64_t pId = p->getId();
        if (verbose)
        {
            printf("[%u] def process %lu (%s)\n",
                    mpiAnalysis.getMPIRank(),
                    pId, p->getName());
        }

        writer->writeDefProcess(pId, p->getParentId(), p->getName(),
                this->processTypeToGroup(p->getProcessType()));

        Process::SortedNodeList &nodes = p->getNodes();
        GraphNode *pLastGraphNode = p->getLastGraphNode();

        for (Process::SortedNodeList::const_iterator iter = nodes.begin();
                iter != nodes.end(); ++iter)
        {
            Node *node = *iter;

            // find next connected node on critical path
            GraphNode *futureCPNode = NULL;
            Graph::EdgeList outEdges = this->getOutEdges((GraphNode*)node);
            Graph::EdgeList::const_iterator edgeIter = outEdges.begin();
            uint64_t timeNextCPNode = 0;

            while(edgeIter != outEdges.end())
            {
                GraphNode *edgeEndNode = (*edgeIter)->getEndNode();
                
                if((edgeEndNode->getCounter(cpCtrId, NULL) == 1) && 
                        ((timeNextCPNode > edgeEndNode->getTime()) || timeNextCPNode == 0))
                {
                    futureCPNode = edgeEndNode;
                    timeNextCPNode = futureCPNode->getTime();
                }
                ++edgeIter;
            }
            
            if (node->isEnter() || node->isLeave())
            {
                if ((!node->isPureWaitstate()) || enableWaitStates)
                {
                    if (verbose)
                    {
                        printf("[%u] [%12lu:%12.8fs] %60s in %8lu (FID %lu)\n",
                                mpiAnalysis.getMPIRank(),
                                node->getTime(),
                                (double) (node->getTime()) / (double) this->ticksPerSecond,
                                node->getUniqueName().c_str(),
                                node->getProcessId(),
                                node->getFunctionId());
                    }

                    writer->writeNode(node, this->ctrTable,
                            node == pLastGraphNode, futureCPNode);
                }
            }

        }
    }

    writer->close();
    delete writer;
}

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

void AnalysisEngine::pushOnOMPBackTraceStack(GraphNode* node, uint64_t processId)
{
    ompBackTraceStackMap[processId].push(node);
}

GraphNode* AnalysisEngine::ompBackTraceStackTop(uint64_t processId)
{
    if(ompBackTraceStackMap[processId].empty())
        return NULL;
    return ompBackTraceStackMap[processId].top();
}

GraphNode* AnalysisEngine::ompBackTraceStackPop(uint64_t processId)
{
    GraphNode* node = ompBackTraceStackMap[processId].top();
    ompBackTraceStackMap[processId].pop();
    return node;
}

bool AnalysisEngine::ompBackTraceStackIsEmpty(uint64_t processId)
{
    return ompBackTraceStackMap[processId].empty();
}

GraphNode* AnalysisEngine::getLastOmpNode(uint64_t processId)
{
    return lastOmpEventMap[processId];
}

void AnalysisEngine::setLastOmpNode(GraphNode* node, uint64_t processId)
{
    lastOmpEventMap[processId] = node;
}

GraphNode* AnalysisEngine::getPendingParallelRegion()
{
    return pendingParallelRegion;
}

void AnalysisEngine::setPendingParallelRegion(GraphNode* node)
{
    pendingParallelRegion = node;
}

GraphNode* AnalysisEngine::getOmpCompute(uint64_t processId)
{
    return ompComputeTrackMap[processId];
}

void AnalysisEngine::setOmpCompute(GraphNode* node, uint64_t processId)
{
    ompComputeTrackMap[processId] = node;
}

const GraphNode::GraphNodeList& AnalysisEngine::getBarrierEventList()
{
    return ompBarrierList;
}

void AnalysisEngine::clearBarrierEventList()
{
    ompBarrierList.clear();
}

void AnalysisEngine::addBarrierEventToList(GraphNode* node)
{
    ompBarrierList.push_back(node);
}
