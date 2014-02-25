/* 
 * File:   Generator.cpp
 * Author: felix
 * 
 * Created on May 15, 2013, 2:16 PM
 */

#include <../4.8.1/bits/algorithmfwd.h>

#include "Generator.hpp"
#include "IPattern.hpp"

using namespace cdm;

Generator::Generator(size_t numHostProcs, size_t numDeviceProcs, bool hasNullStream) :
numHostProcs(numHostProcs),
numDeviceProcs(numDeviceProcs)
{
    numNullStream = hasNullStream ? 1 : 0;
    setTimerResolution(TICKS_PER_SECOND); // 1GHz
}

Generator::~Generator()
{
    for (std::vector<IPattern*>::iterator iter = patterns.begin();
            iter != patterns.end(); ++iter)
    {
        delete (*iter);
    }
}

void Generator::addPattern(IPattern* pattern)
{
    patterns.push_back(pattern);
}

size_t Generator::generate(size_t numBlocks)
{
    if (patterns.size() == 0)
        return 0;

    generateAllocation(numHostProcs, numDeviceProcs, numNullStream);
    functionTable.generateFunctions(numHostProcs * 1000, numDeviceProcs * 100);
    return generateBlocks(numBlocks);
}

void Generator::insertCPUNodes(Process::SortedNodeList& nodes, uint64_t pId, uint64_t start, uint64_t end)
{
    uint64_t currentTime = start;
    while (currentTime < end)
    {
        uint64_t functionDuration = IPattern::getTimeOffset(1000);
        int64_t overlap = (currentTime + functionDuration) - end;
        if (overlap > 0)
            functionDuration -= overlap;

        uint64_t fId = functionTable.getRandomFunction(NT_FT_CPU);
        //printf("adding cpu function %4u (%5lu-%5lu)\n", fId,
        //      currentTime, currentTime + functionDuration);
        Node *nodeEnter = newNode(currentTime, pId,
                functionTable.getName(fId), NT_RT_ENTER | NT_FT_CPU);
        Node *nodeLeave = newNode(currentTime + functionDuration, pId,
                functionTable.getName(fId), NT_RT_LEAVE | NT_FT_CPU);

        nodeEnter->setFunctionId(fId);
        nodeLeave->setFunctionId(fId);

        nodes.push_back(nodeEnter);
        nodes.push_back(nodeLeave);
        
        std::sort(nodes.begin(), nodes.end(), Node::compareLess);

        currentTime += functionDuration;
    }
}

void Generator::fillCPUNodes()
{
    const Allocation::ProcessList& pList = allocation.getHostProcesses();
    for (Allocation::ProcessList::const_iterator pIter = pList.begin();
            pIter != pList.end(); ++pIter)
    {
        Process *p = *pIter;
        Process::SortedNodeList& nodes = p->getNodes();
        Process::SortedNodeList::iterator nodeIter = nodes.begin();

        Process::SortedNodeList tmpNodes;
        Node *node = NULL;
        uint64_t lastGPUTime = 0;

        if (nodes.size() == 0)
            continue;

        // find first enter node, if any
        while (nodeIter != nodes.end() && !((*nodeIter)->getType() & NT_RT_ENTER))
        {
            ++nodeIter;
        }
        if (nodeIter == nodes.end())
            continue;

        // insert between 0 and first enter node
        node = *nodeIter;
        if (node->getTime() > 0)
        {
            //printf("inserting CPU before first enter\n");
            uint64_t timeOffset = 0;
            if (p->getId() > 1)
                timeOffset = IPattern::getTimeOffset(node->getTime());
            insertCPUNodes(tmpNodes, p->getId(), timeOffset, node->getTime());
        }

        // insert between other nodes (leave-enter)
        for (; nodeIter != nodes.end();)
        {
            node = *nodeIter;
            if (node->getTime() > lastGPUTime)
                lastGPUTime = node->getTime();

            if (!node->isLeave())
            {
                ++nodeIter;
                continue;
            }

            // find next enter node
            Process::SortedNodeList::iterator nextGPUIter = ++nodeIter;
            while ((nextGPUIter != nodes.end()) &&
                    !(*nextGPUIter)->isEnter())
            {
                ++nextGPUIter;
                if ((nextGPUIter != nodes.end()) && (*nextGPUIter)->getTime() > lastGPUTime)
                    lastGPUTime = (*nextGPUIter)->getTime();
            }

            if (nextGPUIter == nodes.end())
                break;

            //printf("inserting CPU between (%lu-%lu)\n", node->getTime(), (*nextGPUIter)->getTime());
            insertCPUNodes(tmpNodes, p->getId(), node->getTime(),
                    (*nextGPUIter)->getTime());

            nodeIter = nextGPUIter;
        }

        // insert nodes after last gpu node
        Node *lastNode = getLastNode();
        //printf("inserting CPU after last gpu node (%lu-%lu)\n",
        //      lastGPUTime, lastNode->getTime());
        insertCPUNodes(tmpNodes, p->getId(), lastGPUTime,
                lastNode->getTime() + (rand() % 1000));

        // insert all new tmpNodes to process' nodes
        nodes.insert(nodes.end(), tmpNodes.begin(), tmpNodes.end());
        std::sort(nodes.begin(), nodes.end(), Node::compareLess);
    }
}

void Generator::generateAllocation(size_t numHostProcs, size_t numDeviceProcs, size_t numNullStream)
{
    uint64_t pId = 1;
    uint64_t parentId = 0;
    
    for (size_t i = 0; i < numHostProcs; ++i)
    {
        std::stringstream stream;
        stream << "CPU:" << i;

        newProcess(pId, parentId, stream.str().c_str(), Process::PT_HOST,
                GRAPH_CUDA, false);
        if (i == 0)
        {
            parentId = pId;
            pId += 1000;
        }
        pId += rand() % 10 + 1;
    }

    for (size_t i = 0; i < numNullStream; ++i)
    {
        std::stringstream stream;
        stream << "CUDA (null):" << i;

        newProcess(pId, parentId, stream.str().c_str(), Process::PT_DEVICE_NULL,
                GRAPH_CUDA, false);
        pId += rand() % 10 + 1;
    }

    for (size_t i = numNullStream; i < numDeviceProcs + numNullStream; ++i)
    {
        std::stringstream stream;
        stream << "CUDA:" << i;

        newProcess(pId, parentId, stream.str(), Process::PT_DEVICE,
                GRAPH_CUDA, false);
        pId += rand() % 10 + 1;
    }
}

size_t Generator::generateBlocks(size_t numBlocks)
{
    size_t numBlocksAdded = 0;

    for (size_t block = 0; block < numBlocks; ++block)
    {
        size_t pHostProcs = 0, pDeviceProcs = 0;
        bool validSplit = false, nullStream = false;

        IPattern *pattern = patterns[rand() % patterns.size()];
        if (pattern->getAllocationSize(&pHostProcs, &pDeviceProcs, &nullStream))
        {
            Allocation::Splittings splittings;
            splittings.push_back(Split(pHostProcs, pDeviceProcs, nullStream));

            Allocation::AllocationList allocList = allocation.split(splittings, &validSplit);

            if (validSplit)
            {
                pattern->fill(*this, *(allocList[0]), functionTable);
                numBlocksAdded++;
            }
            
            for (Allocation::AllocationList::iterator iter = allocList.begin();
                    iter != allocList.end(); ++iter)
            {
                delete (*iter);
            }
        }
    }

    return numBlocksAdded;
}

void Generator::mapProcessList(const Allocation::ProcessList& processes)
{
    for (Allocation::ProcessList::const_iterator pIter = processes.begin();
            pIter != processes.end(); ++pIter)
    {
        Process *p = *pIter;
        Process::SortedNodeList nodes = p->getNodes();
        uint64_t lastFuncId = 0;

        for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                nIter != nodes.end(); ++nIter)
        {
            Node *node = *nIter;
            uint64_t funcId = lastFuncId;
            if (node->isAtomic())
                continue;

            if (node->isEnter())
            {
                funcId = functionTable.getRandomFunction(node->getType());
                lastFuncId = funcId;
            }

            node->setFunctionId(funcId);
            node->setName(functionTable.getName(funcId));
        }
    }
}

void Generator::mapFunctions()
{
    mapProcessList(allocation.getHostProcesses());
    mapProcessList(allocation.getDeviceProcesses());
    if (allocation.getNullStream())
    {
        Allocation::ProcessList nullStreamList;
        nullStreamList.push_back(allocation.getNullStream());
        mapProcessList(nullStreamList);
    }
}

GraphNode *Generator::addNewGraphNode(uint64_t time, Process *process,
        int nodeType, Edge **resultEdge)
{
    return TraceData::addNewGraphNode(time, process,
            Node::typeToStr(nodeType).c_str(), nodeType, resultEdge, NULL);
}

EventNode *Generator::addNewEventNode(uint64_t time, uint32_t eventId,
        EventNode::FunctionResultType fResult, Process *process,
        int nodeType, Edge **resultEdge)
{
    return TraceData::addNewEventNode(time, eventId, fResult, process,
            Node::typeToStr(nodeType).c_str(), nodeType, resultEdge, NULL);
}
