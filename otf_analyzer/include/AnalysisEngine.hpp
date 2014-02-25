/* 
 * File:   AnalysisEngine.hpp
 * Author: felix
 *
 * Created on May 15, 2013, 2:14 PM
 */

#ifndef ANALYSISENGINE_HPP
#define	ANALYSISENGINE_HPP

#include <map>
#include <set>
#include <stack>
#include <mpi.h>

#include "TraceData.hpp"
#include "AbstractRule.hpp"
#include "MPIAnalysis.hpp"
#include "graph/EventNode.hpp"
#include "graph/RemoteGraphNode.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/IParallelTraceWriter.hpp"

namespace cdm
{

    class AnalysisEngine : public TraceData
    {
    public:

        typedef struct
        {
            EventNode* node;
            std::set<uint32_t> tags;
        } StreamWaitTagged;

        typedef std::map<uint64_t, uint64_t> IdIdMap;
        typedef std::map<uint64_t, EventNode*> IdEventNodeMap;
        typedef std::map<uint64_t, EventNode::EventNodeList> IdEventsListMap;
        typedef std::list<StreamWaitTagged*> NullStreamWaitList;
        typedef std::map<uint64_t, GraphNode::GraphNodeList > KernelLaunchListMap;
        typedef std::stack<GraphNode*> OmpNodeStack;
        typedef std::map<uint64_t, OmpNodeStack > pendingOMPKernelStackMap;
        typedef std::map<uint64_t, GraphNode*> OmpEventMap;

        AnalysisEngine(uint32_t mpiRank, uint32_t mpiSize);
        virtual ~AnalysisEngine();

        MPIAnalysis &getMPIAnalysis();
        uint32_t getMPIRank() const;

        void addFunction(uint64_t funcId, const char *name);
        uint64_t getNewFunctionId();
        void setWaitStateFunctionId(uint64_t id);
        const char *getFunctionName(uint64_t id);

        GraphNode* newGraphNode(uint64_t time, uint64_t processId,
                const std::string name, Paradigm paradigm,
                NodeRecordType recordType, int nodeType);
        GraphNode* addNewGraphNode(uint64_t time, Process *process,
                const char *name, Paradigm paradigm, NodeRecordType recordType,
                int nodeType, Edge::ParadigmEdgeMap *resultEdges);
        RemoteGraphNode *addNewRemoteNode(uint64_t time, uint64_t remoteProcId,
                uint32_t remoteNodeId, Paradigm paradigm, NodeRecordType recordType,
                int nodeType, uint32_t mpiRank);

        void addRule(AbstractRule *rule);
        void removeRules();
        bool applyRules(Node *node, bool verbose);
        Process *getNullStream() const;

        void setLastEventLaunch(EventNode *eventLaunchLeave);
        EventNode *consumeLastEventLaunchLeave(uint32_t eventId);
        EventNode *getLastEventLaunchLeave(uint32_t eventId) const;

        void setEventProcessId(uint32_t eventId, uint64_t processId);
        uint64_t getEventProcessId(uint32_t eventId) const;

        void addPendingKernelLaunch(GraphNode* launch);
        GraphNode* consumePendingKernelLaunch(uint64_t kernelProcessId);

        void addStreamWaitEvent(uint64_t deviceProcId, EventNode *streamWaitLeave);
        EventNode *getFirstStreamWaitEvent(uint64_t deviceProcId);
        EventNode *consumeFirstStreamWaitEvent(uint64_t deviceProcId);

        void linkEventQuery(EventNode *eventQueryLeave);
        void removeEventQuery(uint32_t eventId);

        GraphNode *getLastLaunchLeave(uint64_t timestamp, uint64_t deviceProcId) const;
        GraphNode *getLastLeave(uint64_t timestamp, uint64_t procId) const;

        void reset();

        void saveParallelAllocationToFile(const char* filename,
                const char* origFilename,
                bool enableWaitStates, bool verbose);

        double getRealTime(uint64_t t);

        void pushOnOMPBackTraceStack(GraphNode* node, uint64_t processId);
        
        GraphNode* ompBackTraceStackTop(uint64_t processId);
        GraphNode* ompBackTraceStackPop(uint64_t processId);
        bool ompBackTraceStackIsEmpty(uint64_t processId);
        
        GraphNode* getLastOmpNode(uint64_t processId);
        void setLastOmpNode(GraphNode* node, uint64_t processId);
        
        GraphNode* getPendingParallelRegion();
        void setPendingParallelRegion(GraphNode* node);
        
        GraphNode* getOmpCompute(uint64_t processId);
        void setOmpCompute(GraphNode* node, uint64_t processId);
        
        const GraphNode::GraphNodeList& getBarrierEventList();
        void addBarrierEventToList(GraphNode* node);
        void clearBarrierEventList();

    private:
       
        MPIAnalysis mpiAnalysis;

        std::vector<AbstractRule*> rules;
        IdEventNodeMap eventLaunchMap; // maps event ID to (cuEventRecord) leave node
        IdEventNodeMap eventQueryMap; // maps event ID to (cuEventQuery) leave node
        IdEventsListMap streamWaitMap; // maps (device) process ID to list of (cuStreamWaitEvent) leave nodes
        IdIdMap eventProcessMap; // maps event ID to (device) process ID
        NullStreamWaitList nullStreamWaits;
        KernelLaunchListMap pendingKernelLaunchMap;

        pendingOMPKernelStackMap ompBackTraceStackMap; // log the OMP enter events, needed to resolve nested function calls
        OmpEventMap lastOmpEventMap; // remember last omp event per process -> needed to resolve nested function calls
        GraphNode* pendingParallelRegion; // remember opened parallel region TODO: implement that as stack for nested parallelism
        OmpEventMap ompComputeTrackMap; // keep track of omp kernels between parallel regions
        GraphNode::GraphNodeList ompBarrierList; // collect barriers from different processes

        std::map<uint64_t, std::string> functionMap;
        uint64_t maxFunctionId;
        uint64_t waitStateFuncId;

        static bool rulePriorityCompare(AbstractRule *r1, AbstractRule *r2);

        size_t getNumAllDeviceProcesses();
        
    };

}

#endif	/* ANALYSISENGINE_HPP */

