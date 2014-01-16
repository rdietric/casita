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
#include <mpi.h>

#include "TraceData.hpp"
#include "AbstractRule.hpp"
#include "MPIAnalysis.hpp"
#include "graph/EventNode.hpp"
#include "graph/RemoteGraphNode.hpp"

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

        typedef std::map<uint32_t, uint32_t> IdIdMap;
        typedef std::map<uint32_t, EventNode*> IdEventNodeMap;
        typedef std::map<uint32_t, EventNode::EventNodeList> IdEventsListMap;
        typedef std::list<StreamWaitTagged*> NullStreamWaitList;
        typedef std::map<uint32_t, GraphNode::GraphNodeList > KernelLaunchListMap;

        AnalysisEngine(uint32_t mpiRank, uint32_t mpiSize);
        virtual ~AnalysisEngine();

        MPIAnalysis &getMPIAnalysis();
        uint32_t getMPIRank() const;
#ifdef MPI_CP_MERGE
        void mergeMPIGraphs();
#endif

        void addFunction(uint32_t funcId, const char *name);
        uint32_t getNewFunctionId();
        void setWaitStateFunctionId(uint32_t id);
        const char *getFunctionName(uint32_t id);

        GraphNode* newGraphNode(uint64_t time, uint32_t processId,
                const std::string name, int nodeType);
        GraphNode* addNewGraphNode(uint64_t time, Process *process,
                const char *name, int nodeType, Edge **resultEdgeCUDA,
                Edge **resultEdgeMPI);
        RemoteGraphNode *addNewRemoteNode(uint64_t time, uint32_t remoteProcId,
                uint32_t remoteNodeId, int nodeType, uint32_t mpiRank);

        void addRule(AbstractRule *rule);
        void removeRules();
        bool applyRules(Node *node, bool verbose);
        Process *getNullStream() const;

        void setLastEventLaunch(EventNode *eventLaunchLeave);
        EventNode *consumeLastEventLaunchLeave(uint32_t eventId);
        EventNode *getLastEventLaunchLeave(uint32_t eventId) const;

        void setEventProcessId(uint32_t eventId, uint32_t processId);
        uint32_t getEventProcessId(uint32_t eventId) const;

        void addPendingKernelLaunch(GraphNode* launch);
        GraphNode* consumePendingKernelLaunch(uint32_t kernelProcessId);

        void addStreamWaitEvent(uint32_t deviceProcId, EventNode *streamWaitLeave);
        EventNode *getFirstStreamWaitEvent(uint32_t deviceProcId);
        EventNode *consumeFirstStreamWaitEvent(uint32_t deviceProcId);

        void linkEventQuery(EventNode *eventQueryLeave);
        void removeEventQuery(uint32_t eventId);

        GraphNode *getLastLaunchLeave(uint64_t timestamp, uint32_t deviceProcId) const;
        GraphNode *getLastLeave(uint64_t timestamp, uint32_t procId) const;

        void reset();
        void optimizeKernel(std::map<uint32_t, double> optimizationMap, bool verbose);

        void saveParallelAllocationToFile(const char* filename,
                const char* origFilename,
                bool enableWaitStates, bool verbose);

        double getRealTime(uint64_t t);

    private:
        MPIAnalysis mpiAnalysis;

        std::vector<AbstractRule*> rules;
        IdEventNodeMap eventLaunchMap; // maps event ID to (cuEventRecord) leave node
        IdEventNodeMap eventQueryMap; // maps event ID to (cuEventQuery) leave node
        IdEventsListMap streamWaitMap; // maps (device) process ID to list of (cuStreamWaitEvent) leave nodes
        IdIdMap eventProcessMap; // maps event ID to (device) process ID
        NullStreamWaitList nullStreamWaits;
        KernelLaunchListMap pendingKernelLaunchMap;

        std::map<uint32_t, std::string> functionMap;
        uint32_t maxFunctionId;
        uint32_t waitStateFuncId;

        static bool rulePriorityCompare(AbstractRule *r1, AbstractRule *r2);
        static uint64_t getNodeSlack(const std::map<GraphNode*, uint64_t> slackMap,
                GraphNode *node);

        uint64_t updateInEdges(const Graph::EdgeList& inEdges, bool verbose);
        uint64_t updateGraphNode(GraphNode *node, uint64_t delta,
                uint64_t fixedSlack, bool verbose);

        size_t getNumAllDeviceProcesses();
    };

}

#endif	/* ANALYSISENGINE_HPP */

