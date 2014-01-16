/* 
 * File:   Generator.hpp
 * Author: felix
 *
 * Created on May 15, 2013, 2:16 PM
 */

#ifndef GENERATOR_HPP
#define	GENERATOR_HPP

#include "TraceData.hpp"
#include "FunctionTable.hpp"

#define TICKS_PER_SECOND 1000000000
#define TICKS_MIN_INC 500

namespace cdm
{

    class IPattern;

    class Generator : public TraceData
    {
    public:
        Generator(size_t numHostProcs, size_t numDeviceProcs, bool hasNullStream);
        virtual ~Generator();

        void addPattern(IPattern *pattern);
        size_t generate(size_t numBlocks);
        void fillCPUNodes();
        void mapFunctions();

        GraphNode *addNewGraphNode(uint64_t time, Process *process,
                int nodeType, Edge **resultEdge = NULL);
        EventNode *addNewEventNode(uint64_t time, uint32_t eventId,
                EventNode::FunctionResultType fResult, Process *process,
                int nodeType, Edge **resultEdge = NULL);

    private:
        std::vector<IPattern*> patterns;
        FunctionTable functionTable;
        size_t numHostProcs, numDeviceProcs, numNullStream;

        void generateAllocation(size_t numHostProcs, size_t numDeviceProcs,
                size_t numNullStream);
        size_t generateBlocks(size_t numBlocks);
        void mapProcessList(const Allocation::ProcessList& processes);

        void insertCPUNodes(Process::SortedNodeList& nodes, uint32_t pId,
                uint64_t start, uint64_t end);
    };

}

#endif	/* GENERATOR_HPP */

