/* 
 * File:   TraceData.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 11:26 AM
 */

#ifndef TRACEDATA_HPP
#define	TRACEDATA_HPP

#include <map>
#include <vector>
#include <string>
#include "ErrorUtils.hpp"
#include "graph/Graph.hpp"
#include "graph/Node.hpp"
#include "graph/EventNode.hpp"
#include "graph/Edge.hpp"
#include "otf/ITraceWriter.hpp"
#include "Kernel.hpp"
#include "Process.hpp"
#include "Allocation.hpp"
#include "CounterTable.hpp"
#include "FunctionTable.hpp"

// shared with VampirTrace
#define VT_CUPTI_CUDA_STREAMREF_KEY     "CUDA_STREAM_REF_KEY"
#define VT_CUPTI_CUDA_EVENTREF_KEY      "CUDA_EVENT_REF_KEY"
#define VT_CUPTI_CUDA_CURESULT_KEY      "CUDA_DRV_API_RESULT_KEY"

#define SCOREP_CUPTI_CUDA_STREAMREF_KEY     "CUDA_STREAM_REF"
#define SCOREP_CUPTI_CUDA_EVENTREF_KEY      "CUDA_EVENT_REF"
#define SCOREP_CUPTI_CUDA_CURESULT_KEY      "CUDA_DRV_API_RESULT"

#define SYNC_DELTA 500 // in us

namespace cdm
{

    class TraceData
    {
    private:

        Graph::EdgeList emptyEdgeList;

    public:
        typedef std::map<uint32_t, Process*> ProcessMap;
        typedef std::list<Activity*> ActivityList;
        typedef std::stack<GraphNode*> GraphNodeStack;
        typedef std::map<uint32_t, GraphNodeStack > GraphNodeStackMap;

        TraceData();
        virtual ~TraceData();

        static bool getFunctionType(uint32_t id, const char *name, Process *process, FunctionDescriptor *descr);

        Graph& getGraph();
        Graph* getGraph(Paradigm paradigm);

        CounterTable &getCtrTable();
        virtual void reset();
        void resetCounters();

        // processes
        Process* getProcess(uint32_t id) const;
        void getProcesses(Allocation::ProcessList& procs) const;
        void getLocalProcesses(Allocation::ProcessList& procs) const;
        void getProcesses(Allocation::ProcessList& procs, Paradigm paradigm) const;
        const Allocation::ProcessList& getHostProcesses() const;
        const Allocation::ProcessList& getDeviceProcesses() const;
        void getAllDeviceProcesses(Allocation::ProcessList& deviceProcs) const;

        // allocators
        Process* newProcess(uint32_t id, uint32_t parentId, const std::string name,
                Process::ProcessType processType, Paradigm paradigm, bool remoteProcess = false);
        Node* newNode(uint64_t time, uint32_t processId, const std::string name,
                Paradigm paradigm, NodeRecordType recordType, int nodeType);
        Edge* newEdge(GraphNode* n1, GraphNode *n2, int properties = EDGE_NONE,
                Paradigm *edgeType = NULL);

        GraphNode* newGraphNode(uint64_t time, uint32_t processId,
                const std::string name, Paradigm paradigm, NodeRecordType recordType,
                int nodeType);
        EventNode* newEventNode(uint64_t time, uint32_t processId, uint32_t eventId,
                EventNode::FunctionResultType fResult, const std::string name,
                Paradigm paradigm, NodeRecordType recordType, int nodeType);

        GraphNode *addNewGraphNode(uint64_t time, Process *process,
                const char *name, Paradigm paradigm, NodeRecordType recordType,
                int nodeType, Edge::ParadigmEdgeMap *resultEdges);
        EventNode *addNewEventNode(uint64_t time, uint32_t eventId,
                EventNode::FunctionResultType fResult, Process *process,
                const char *name, Paradigm paradigm, NodeRecordType recordType,
                int nodeType, Edge::ParadigmEdgeMap *resultEdges);

        Edge* getEdge(GraphNode *source, GraphNode *target);
        void removeEdge(Edge *e);

        // query timeline objects
        Node* getLastNode() const;
        GraphNode *getSourceNode() const;
        GraphNode *getLastGraphNode() const;
        GraphNode *getFirstTimedGraphNode(Paradigm paradigm) const;
        GraphNode *getLastGraphNode(Paradigm paradigm) const;
        void getAllNodes(Process::SortedNodeList& allNodes) const;
        ActivityList &getActivities();

        // timings
        void setTimerResolution(uint64_t ticksPerSecond);
        uint64_t getTimerResolution();
        uint64_t getDeltaTicks();

        void getCriticalPath(GraphNode *sourceNode, GraphNode * lastNode,
                GraphNode::GraphNodeList *cpath, Paradigm paradigm);

        void runSanityCheck(uint32_t mpiRank);

        void saveAllocationToFile(const char *filename,
                bool enableWaitStates, bool verbose);
    protected:
        Allocation allocation;
        uint64_t ticksPerSecond;

        Graph graph;
        GraphNode * globalSourceNode;
        GraphNodeStackMap pendingGraphNodeStackMap;

        ProcessMap processMap;
        ActivityList activities;

        CounterTable ctrTable;

        // query graph objects
        bool hasInEdges(GraphNode *n);
        bool hasOutEdges(GraphNode *n);
        const Graph::EdgeList& getInEdges(GraphNode *n) const;
        const Graph::EdgeList& getOutEdges(GraphNode *n) const;
        
        GraphNode* topGraphNodeStack(uint32_t processId);
        void popGraphNodeStack(uint32_t processId);
        void pushGraphNodeStack(GraphNode* node, uint32_t processId);
        

        void sanityCheckEdge(Edge *edge, uint32_t mpiRank);
        void addNewGraphNodeInternal(GraphNode *node, Process *process,
                Edge::ParadigmEdgeMap *resultEdges);

        static io::ITraceWriter::ProcessGroup processTypeToGroup(Process::ProcessType pt);
    };

}

#endif	/* ANALYSISENGINE_HPP */

