/* 
 * File:   CDMRunner.hpp
 * Author: felix
 *
 * Created on 13. August 2013, 10:40
 */

#ifndef CDMRUNNER_HPP
#define	CDMRUNNER_HPP

#include <string>
#include <stdio.h>
#include <stdint.h>
#include <cuda.h>
#include <mpi.h>
#include <map>

#include "common.hpp"
#include "AnalysisEngine.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/IKeyValueList.hpp"

#define GRAPH_LOCAL_FILENAME "g_cuda.dot"
#define GRAPH_MPI_FILENAME "g_mpi.dot"
#define GRAPH_GLOBAL_FILENAME "graph.dot"
#define GRAPH_CUDA_OPT_FILENAME "g_cuda_optimized.dot"

namespace cdm
{

    class CDMRunner
    {
    private:
        typedef std::vector<MPIAnalysis::CriticalPathSection > SectionsList;
        typedef std::vector<MPIAnalysis::ProcessNodePair > MPINodeList;
        
    public:

        typedef struct
        {
            bool createOTF;
            bool createGraphs;
            bool printCriticalPath;
            bool mergeActivities;
            bool noErrors;
            int verbose;
            int maxEvents, eventsProcessed;
            int memLimit;
            char *outOtfFile;
            char *filename;
        } ProgramOptions;

        typedef struct
        {
            uint64_t functionId;
            uint32_t numInstances;
            uint32_t numUnifyProcesses;
            uint64_t totalDuration;
            uint64_t totalDurationOnCP;
            uint64_t totalBlame;
            double fractionCP;
            double fractionBlame;
        } ActivityGroup;

        typedef struct
        {

            bool operator()(const ActivityGroup &g1, const ActivityGroup &g2) const
            {
                double rating1 = g1.fractionBlame + g1.fractionCP;
                double rating2 = g2.fractionBlame + g2.fractionCP;

                if (rating1 == rating2)
                    return g1.functionId > g2.functionId;
                else
                    return rating1 > rating2;
            }

        } ActivityGroupCompare;

        CDMRunner(int mpiRank, int mpiSize, ProgramOptions options);
        virtual ~CDMRunner();

        void readOTF();
        void runAnalysis(Paradigm paradigm, Process::SortedNodeList &allNodes);

        void getCriticalPath(Process::SortedGraphNodeList &gpuNodes,
                Process::SortedGraphNodeList &mpiNodes);

        ProgramOptions &getOptions();
        AnalysisEngine &getAnalysis();

        // OTF misc
        static void applyStreamRefsEnter(io::ITraceReader *reader, Node *node,
                io::IKeyValueList *list);
        static void applyStreamRefsLeave(io::ITraceReader *reader, Node *node,
                Node *oldNode, io::IKeyValueList *list);
        static uint32_t readKeyVal(io::ITraceReader *reader, const char * keyName,
                io::IKeyValueList *list);
        
        static int getCurrentResources();
    private:
        int mpiRank;
        int mpiSize;
        AnalysisEngine analysis;
        ProgramOptions options;

        void printNode(GraphNode *node, Process *process);
        void printAllActivities(uint64_t globalCPLength);
        void mergeActivityGroups(std::map<uint64_t, ActivityGroup> &activityGroupMap,
            bool cpKernelsOnly);

        // optimization
        void getOptFactors(char *optKernels, std::map<uint64_t, double>& optFactors);

        // critical path
        void getCriticalPathIntern(GraphNode *start, GraphNode *end,
                Process::SortedGraphNodeList& cpNodes, Graph& subGraph);
        void getCriticalLocalSections(MPIAnalysis::CriticalPathSection *sections,
                uint32_t numSections, Process::SortedGraphNodeList& gpuNodes,
                MPIAnalysis::CriticalSectionsMap& sectionsMap);
        void createSection(SectionsList *sections,
                GraphNode* start, GraphNode* end, uint64_t prevProcessId,
                uint64_t currentProcessId, uint64_t nextProcessId);

        void findLastMpiNode(GraphNode **node);
        void reverseReplayMPICriticalPath(MPIAnalysis::CriticalSectionsList& sectionsList);

        // handlers
        static void handleProcessMPIMapping(io::ITraceReader *reader,
                uint64_t processId, uint32_t mpiRank);
        static void handleDefProcess(io::ITraceReader *reader, uint32_t stream,
                uint64_t processId, uint64_t parentId, const char *name,
                io::IKeyValueList * list, bool isCUDA, bool isCUDANull);
        static void handleDefFunction(io::ITraceReader *reader, uint32_t streamId,
                uint64_t functionId, const char *name, uint32_t functionGroupId);
        static void handleEnter(io::ITraceReader *reader, uint64_t time, uint64_t functionId,
                uint64_t processId, io::IKeyValueList *list);
        static void handleAdditionalEnter(io::ITraceReader *reader, uint64_t time, uint64_t functionId,
                uint64_t processId, io::IKeyValueList *list);
        static void handleLeave(io::ITraceReader *reader, uint64_t time,
                uint64_t functionId, uint64_t processId, io::IKeyValueList *list);
        static void handleAdditionalLeave(io::ITraceReader *reader, uint64_t time,
                uint64_t functionId, uint64_t processId, io::IKeyValueList *list);
        static void handleMPIComm(io::ITraceReader *reader, io::MPIType mpiType,
                uint64_t processId, uint64_t partnerId, uint32_t root, uint32_t tag);
        static void handleMPICommGroup(io::ITraceReader *reader, uint32_t group,
                uint32_t numProcs, const uint64_t *procs);
    };

}

#endif	/* CDMRUNNER_HPP */
