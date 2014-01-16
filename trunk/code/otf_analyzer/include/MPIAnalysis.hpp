/* 
 * File:   MPIAnalysis.hpp
 * Author: felix
 *
 * Created on 17. Juli 2013, 10:50
 */

#ifndef MPIANALYSIS_HPP
#define	MPIANALYSIS_HPP

#include <map>
#include <set>
#include <vector>
#include <stdint.h>
#include <mpi.h>

#include "common.hpp"
#include "graph/GraphNode.hpp"

#define MPI_CHECK(cmd) \
        { \
                int mpi_result = cmd; \
                if (mpi_result != MPI_SUCCESS) \
                        throw RTException("MPI error %d in call %s", mpi_result, #cmd); \
        }

namespace cdm
{
    class AnalysisEngine;

    class MPIAnalysis
    {
    public:

        typedef struct
        {
            uint32_t processID;
            uint32_t nodeID;
        } ProcessNodePair;

    private:

        enum MPIExchangeType
        {
            MPI_TYPE_NODE, MPI_TYPE_EDGE
        };

        enum MPINodeInfo
        {
            MPI_NINFO_NONE = 0,
            MPI_NINFO_ENTER = 1 << 1,
            MPI_NINFO_WAITSTATE = 1 << 2,
        };

        typedef struct
        {
            MPIExchangeType type;
            uint32_t nodeIDSource;
            uint32_t processIDSource;

            union
            {

                struct
                {
                    uint64_t time;
                    uint32_t functionID;
                    uint8_t mpiNodeInfo;
                };

                struct
                {
                    uint32_t nodeIDTarget;
                    uint32_t processIDTarget;
                    uint32_t isBlocking;
                    uint8_t reserved;
                };
            };
        } MPIExchangeData;

        typedef std::map<uint32_t, uint32_t> TokenTokenMap;
        typedef std::map<uint32_t, GraphNode*> IdNodeMap;
        typedef std::map<uint32_t, IdNodeMap > RemoteNodeMap;
        typedef std::map<GraphNode*, ProcessNodePair> ReverseRemoteNodeMap;
    public:

        typedef struct
        {
            MPI_Comm comm;
            std::set<uint32_t> procs;
        } MPICommGroup;

        typedef struct
        {
            uint32_t remoteNodeID; // remote node ID
            uint32_t remoteProcessID; // remote process ID
        } MPIEdge;

        typedef struct
        {
            //uint32_t prevProcessID;
            uint32_t processID;
            //uint32_t nextProcessID;
            uint32_t nodeStartID;
            uint32_t nodeEndID;
        } CriticalPathSection;

        typedef std::map<GraphNode*, CriticalPathSection> CriticalSectionsMap;
        typedef std::vector<CriticalPathSection> CriticalSectionsList;

        typedef std::map<GraphNode*, MPIEdge> MPIEdgeMap;
        typedef std::map<uint32_t, GraphNode*> MPIIdNodeMap;
        typedef std::map<uint32_t, MPIIdNodeMap> MPIProcessNodeMap;
        typedef std::map<uint32_t, MPICommGroup > MPICommGroupMap;

        MPIAnalysis(uint32_t mpiRank, uint32_t mpiSize);
        virtual ~MPIAnalysis();

        uint32_t getMPIRank() const;
        uint32_t getMPISize() const;
        uint32_t getMPIRank(uint32_t processId) const;
        void setMPIRank(uint32_t processId, uint32_t rank);
        void setMPICommGroupMap(uint32_t group, uint32_t numProcs, const uint32_t *procs);
        void createMPICommunicatorsFromMap();
        const MPICommGroup& getMPICommGroup(uint32_t group) const;

#ifdef MPI_CP_MERGE
        void addMPIEdge(GraphNode *localNode, uint32_t remoteDstNodeID, uint32_t remoteProcessID);
        GraphNode* getMPIEdgeLocalNode(uint32_t dstNodeId, uint32_t dstProcessId);
        void mergeMPIGraphs(AnalysisEngine *analysis);
#endif

        void addRemoteMPIEdge(GraphNode *localDstNode, uint32_t remoteSrcNodeID, uint32_t remoteProcessID);
        GraphNode* getRemoteMPIEdgeLocalNode(uint32_t srcNodeId, uint32_t srcProcessId);

        ProcessNodePair getRemoteNodeInfo(GraphNode *localNode);
        std::set<uint32_t> getMpiPartners(GraphNode *node);

        void reset();

    private:
        uint32_t mpiRank;
        uint32_t mpiSize;
        TokenTokenMap processRankMap;
#ifdef MPI_CP_MERGE
        MPIEdgeMap mpiEdgeMap;
#endif
        MPIProcessNodeMap remoteMpiEdgeMap;
        MPICommGroupMap mpiCommGroupMap;
        ReverseRemoteNodeMap reverseRemoteNodeMap;

#ifdef MPI_CP_MERGE
        void processMPIExchangeData(const MPIExchangeData &data,
                RemoteNodeMap &remoteNodeMap, AnalysisEngine *analysis,
                MPIExchangeType type);
#endif
    };
}

#endif	/* MPIANALYSIS_HPP */

