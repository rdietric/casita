#include "MPIAnalysis.hpp"
#include "AnalysisEngine.hpp"
#include "common.hpp"

using namespace cdm;

MPIAnalysis::MPIAnalysis(uint32_t mpiRank, uint32_t mpiSize) :
mpiRank(mpiRank),
mpiSize(mpiSize)
{

}

MPIAnalysis::~MPIAnalysis()
{

}

uint32_t MPIAnalysis::getMPIRank() const
{
    return mpiRank;
}

uint32_t MPIAnalysis::getMPISize() const
{
    return mpiSize;
}

uint32_t MPIAnalysis::getMPIRank(uint32_t processId) const
{
    TokenTokenMap::const_iterator iter = processRankMap.find(processId);
    if (iter != processRankMap.end())
        return iter->second;
    else
        throw RTException("Request for mpi rank with invalid process ID %u",
            processId);
}

void MPIAnalysis::setMPIRank(uint32_t processId, uint32_t rank)
{
    processRankMap[processId] = rank;
}

void MPIAnalysis::setMPICommGroupMap(uint32_t group, uint32_t numProcs,
        const uint32_t *procs)
{
    for (uint32_t i = 0; i < numProcs; ++i)
    {
        mpiCommGroupMap[group].procs.insert(procs[i]);
    }
}

void MPIAnalysis::createMPICommunicatorsFromMap()
{
    for (MPICommGroupMap::iterator iter = mpiCommGroupMap.begin();
            iter != mpiCommGroupMap.end(); ++iter)
    {
        MPICommGroup &group = iter->second;

        int ranks[group.procs.size()];
        size_t i = 0;
        for (std::set<uint32_t>::const_iterator iter = group.procs.begin();
                iter != group.procs.end(); ++iter)
        {
            ranks[i] = getMPIRank(*iter);
            ++i;
        }

        MPI_Group worldGroup, commGroup;
        MPI_CHECK(MPI_Comm_group(MPI_COMM_WORLD, &worldGroup));
        MPI_CHECK(MPI_Group_incl(worldGroup, group.procs.size(), ranks, &commGroup));

        MPI_CHECK(MPI_Comm_create(MPI_COMM_WORLD, commGroup, &(group.comm)));
    }
}

const MPIAnalysis::MPICommGroup& MPIAnalysis::getMPICommGroup(uint32_t group) const
{
    MPICommGroupMap::const_iterator iter = mpiCommGroupMap.find(group);
    if (iter != mpiCommGroupMap.end())
        return iter->second;

    throw RTException("Request for unknown MPI comm group %u", group);
}

#ifdef MPI_CP_MERGE

void MPIAnalysis::addMPIEdge(GraphNode *localSrcNode,
        uint32_t remoteDstNodeID, uint32_t remoteProcessID)
{
    mpiEdgeMap[localSrcNode].remoteNodeID = remoteDstNodeID;
    mpiEdgeMap[localSrcNode].remoteProcessID = remoteProcessID;
}

GraphNode* MPIAnalysis::getMPIEdgeLocalNode(uint32_t dstNodeId, uint32_t dstProcessId)
{
    for (MPIEdgeMap::const_iterator iter = mpiEdgeMap.begin();
            iter != mpiEdgeMap.end(); ++iter)
    {
        if (iter->second.remoteNodeID == dstNodeId &&
                iter->second.remoteProcessID == dstProcessId)
            return iter->first;
    }

    throw RTException("[%u] No entry in mpiEdgeMap for remote node %u on process %u",
            mpiRank, dstNodeId, dstProcessId);
}

#endif

void MPIAnalysis::addRemoteMPIEdge(GraphNode *localDstNode,
        uint32_t remoteSrcNodeID, uint32_t remoteProcessID)
{
    remoteMpiEdgeMap[remoteProcessID][remoteSrcNodeID] = localDstNode;
}

GraphNode* MPIAnalysis::getRemoteMPIEdgeLocalNode(uint32_t srcNodeId, uint32_t srcProcessId)
{
    MPIProcessNodeMap::const_iterator pIter = remoteMpiEdgeMap.find(srcProcessId);
    if (pIter != remoteMpiEdgeMap.end())
    {
        MPIIdNodeMap::const_iterator nIter = pIter->second.find(srcNodeId);
        if (nIter != pIter->second.end())
        {
            return nIter->second;
        }
    }

    return NULL;
}

MPIAnalysis::ProcessNodePair MPIAnalysis::getRemoteNodeInfo(GraphNode *localNode)
{
    ReverseRemoteNodeMap::const_iterator iter = reverseRemoteNodeMap.find(localNode);
    if (iter != reverseRemoteNodeMap.end())
        return iter->second;
    else
        throw RTException("[%u] No entry in reverse remote node map for local node %s",
            mpiRank, localNode->getUniqueName().c_str());
}

#ifdef MPI_CP_MERGE

void MPIAnalysis::processMPIExchangeData(const MPIExchangeData &data,
        RemoteNodeMap &remoteNodeMap, AnalysisEngine *analysis, MPIExchangeType type)
{
    if (data.type != type)
        return;

    if (data.type == MPI_TYPE_NODE)
    {
        // add new node and new remote process, is necessary

        Process *process = analysis->getProcess(data.processIDSource);
        if (!process)
        {
            process = analysis->newProcess(data.processIDSource, 0,
                    "RemoteProcess", Process::PT_HOST, GRAPH_MPI, true);
        }

        int nodeType = (data.mpiNodeInfo & MPI_NINFO_ENTER)
                ? NT_RT_ENTER : NT_RT_LEAVE;
        int functionType = TraceData::getFunctionType(
                analysis->getFunctionName(data.functionID), process);

        Edge *newEdge = NULL;
        GraphNode *newNode = analysis->addNewGraphNode(data.time, process,
                analysis->getFunctionName(data.functionID),
                nodeType | functionType, NULL, &newEdge);
        if (newNode->isLeave() && (data.mpiNodeInfo & MPI_NINFO_WAITSTATE))
            newEdge->makeBlocking();

        // update the remoteNodeMap
        remoteNodeMap[data.processIDSource][data.nodeIDSource] = newNode;
        reverseRemoteNodeMap[newNode].nodeID = data.nodeIDSource;
        reverseRemoteNodeMap[newNode].processID = data.processIDSource;
    } else
    {
        // find nodes and add new edge

        GraphNode *node1 = NULL, *node2 = NULL;
        RemoteNodeMap::const_iterator pIter;
        IdNodeMap::const_iterator nIter;

        // source node
        pIter = remoteNodeMap.find(data.processIDSource);
        if (pIter == remoteNodeMap.end())
            throw RTException("Did not find source process %u (node %u)",
                data.processIDSource, data.nodeIDSource);

        nIter = pIter->second.find(data.nodeIDSource);
        if (nIter == pIter->second.end())
            throw RTException("Did not find source node %u", data.nodeIDSource);

        node1 = nIter->second;

        // target node
        pIter = remoteNodeMap.find(data.processIDTarget);
        if (pIter == remoteNodeMap.end())
            throw RTException("Did not find target process %u (node %u)",
                data.processIDTarget, data.nodeIDTarget);

        nIter = pIter->second.find(data.nodeIDTarget);
        if (nIter == pIter->second.end())
            throw RTException("Did not find target node %u", data.nodeIDTarget);

        node2 = nIter->second;

        analysis->newEdge(node1, node2, (data.isBlocking > 0) ? true : false);
    }
}

void MPIAnalysis::mergeMPIGraphs(AnalysisEngine *analysis)
{
    // merge of all per-process MPI graphs
    RemoteNodeMap remoteNodeMap;
    MPIExchangeData *sendBuffer = NULL;
    Process::SortedNodeList localNodes;
    analysis->getAllNodes(localNodes);

    /* prepare send buffers */
    if (mpiRank != 0)
        sendBuffer = new MPIExchangeData[localNodes.size() + mpiEdgeMap.size()];
    else
        sendBuffer = new MPIExchangeData[mpiEdgeMap.size()];

    int numEntries = 0;
    for (Process::SortedNodeList::const_iterator iter = localNodes.begin();
            iter != localNodes.end(); ++iter)
    {
        if (!(*iter)->isGraphNode() || (*iter)->isProcess() || !(*iter)->isMPI())
            continue;

        GraphNode *node = (GraphNode*) (*iter);

        if (mpiRank != 0)
        {
            // insert node in send buffer
            MPIExchangeData *entry = &(sendBuffer[numEntries]);

            entry->type = MPI_TYPE_NODE;
            entry->nodeIDSource = node->getId();
            entry->processIDSource = node->getProcessId();
            entry->time = node->getTime();
            entry->functionID = node->getFunctionId();
            entry->mpiNodeInfo = MPI_NINFO_NONE;
            if (node->isEnter())
                entry->mpiNodeInfo = MPI_NINFO_ENTER;

            if (node->isLeave() &&
                    analysis->getEdge(node->getGraphPair().first, node)->isBlocking())
            {
                entry->mpiNodeInfo = MPI_NINFO_WAITSTATE;
            }
            numEntries++;
        } else
        {
            // insert nodes directly
            remoteNodeMap[node->getProcessId()][node->getId()] = node;
            reverseRemoteNodeMap[node].nodeID = node->getId();
            reverseRemoteNodeMap[node].processID = node->getProcessId();
        }
    }

    for (MPIEdgeMap::const_iterator iter = mpiEdgeMap.begin();
            iter != mpiEdgeMap.end(); ++iter)
    {
        // insert edge in send buffer
        MPIExchangeData *entry = &(sendBuffer[numEntries]);

        entry->type = MPI_TYPE_EDGE;
        entry->nodeIDSource = iter->first->getId();
        entry->processIDSource = iter->first->getProcessId();
        entry->nodeIDTarget = iter->second.remoteNodeID;
        entry->processIDTarget = iter->second.remoteProcessID;
        entry->isBlocking = iter->first->isLeave();
        entry->reserved = 0;
        numEntries++;
    }

    /* collect number of nodes + edges at root node */

    int numEntriesAllProcesses[mpiSize];
    MPI_CHECK(MPI_Gather(&numEntries, 1, MPI_INTEGER4,
            numEntriesAllProcesses, 1, MPI_INTEGER4, 0, MPI_COMM_WORLD));

    /* send/receive */

    if (mpiRank == 0)
    {
        int bytesPerProcess[mpiSize];
        int bytesOffsets[mpiSize];

        uint32_t totalEntries = 0;
        for (int i = mpiSize - 1; i >= 0; --i)
        {
            bytesPerProcess[i] = (int) (sizeof (MPIExchangeData) * numEntriesAllProcesses[i]);
            bytesOffsets[i] = (int) (sizeof (MPIExchangeData) * totalEntries);
            totalEntries += numEntriesAllProcesses[i];
        }

        MPIExchangeData *recvBuffer = new MPIExchangeData[totalEntries];
        MPI_CHECK(MPI_Gatherv(sendBuffer, numEntries * sizeof (MPIExchangeData),
                MPI_INTEGER1, recvBuffer, bytesPerProcess,
                bytesOffsets, MPI_INTEGER1, 0, MPI_COMM_WORLD));

        /* create new nodes and edges from received data */
        /* first nodes (to create all necessary remote processes) */
        for (uint32_t i = 0; i < totalEntries; ++i)
            processMPIExchangeData(recvBuffer[i], remoteNodeMap, analysis, MPI_TYPE_NODE);
        for (uint32_t i = 0; i < totalEntries; ++i)
            processMPIExchangeData(recvBuffer[i], remoteNodeMap, analysis, MPI_TYPE_EDGE);

        GraphNode *lastNode = analysis->getLastGraphNode();
        GraphNode *exitMPINode = analysis->addNewGraphNode(lastNode->getTime(),
                analysis->getProcess(lastNode->getProcessId()), "MPI_EXIT",
                NT_RT_ATOMIC | NT_RT_MPI_EXIT, NULL, NULL);

        const Allocation::ProcessList& hostProcs = analysis->getHostProcesses();
        for (Allocation::ProcessList::const_iterator pIter = hostProcs.begin();
                pIter != hostProcs.end(); ++pIter)
        {
            Process *p = *pIter;
            GraphNode *lastPGraphNode = p->getLastGraphNode(GRAPH_MPI);

            if ((p->getId() != exitMPINode->getProcessId()) && lastPGraphNode)
            {
                analysis->newEdge(lastPGraphNode, exitMPINode, false);
            }
        }

        delete[] recvBuffer;
    } else
    {
        MPI_CHECK(MPI_Gatherv(sendBuffer, numEntries * sizeof (MPIExchangeData),
                MPI_INTEGER1, NULL, NULL, NULL, MPI_INTEGER1, 0, MPI_COMM_WORLD));
    }

    delete[] sendBuffer;
}

#endif

void MPIAnalysis::reset()
{
    reverseRemoteNodeMap.clear();
}

std::set<uint32_t> MPIAnalysis::getMpiPartners(GraphNode *node)
{
    std::set<uint32_t> partners;

    if (!node->isMPI())
        return partners;

    if (node->isEnter())
        node = node->getGraphPair().second;

    if (node->isMPIRecv())
        partners.insert(node->getReferencedProcessId());

    if (node->isMPISend())
        partners.insert(*((uint32_t*) (node->getData())));

    if (node->isMPICollective())
    {
        uint32_t mpiGroupId = node->getReferencedProcessId();
        const MPICommGroup& mpiCommGroup = getMPICommGroup(mpiGroupId);
        partners.insert(mpiCommGroup.procs.begin(), mpiCommGroup.procs.end());
    }

    if (node->isMPISendRecv())
    {
        partners.insert(node->getReferencedProcessId());
        partners.insert(*((uint32_t*) (node->getData())));
    }

    return partners;
}
