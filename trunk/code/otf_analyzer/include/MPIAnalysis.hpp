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
            uint64_t processID;
            uint32_t nodeID;
        } ProcessNodePair;

    private:
        typedef std::map<uint32_t, uint32_t> TokenTokenMap;
        typedef std::map<uint64_t, GraphNode*> IdNodeMap;
        typedef std::map<uint64_t, IdNodeMap > RemoteNodeMap;
        typedef std::map<GraphNode*, ProcessNodePair> ReverseRemoteNodeMap;
    public:

        typedef struct
        {
            MPI_Comm comm;
            std::set<uint64_t> procs;
        } MPICommGroup;
        
        enum MPIEdgeDirection
        {
            MPI_EDGE_LOCAL_REMOTE,
            MPI_EDGE_REMOTE_LOCAL
        };

        typedef struct
        {
            MPIEdgeDirection direction;
            GraphNode *localNode;
            uint64_t remoteNodeID; // remote node ID
            uint64_t remoteProcessID; // remote process ID
        } MPIEdge;

        typedef struct
        {
            //uint64_t prevProcessID;
            uint64_t processID;
            //uint64_t nextProcessID;
            uint32_t nodeStartID;
            uint32_t nodeEndID;
        } CriticalPathSection;

        typedef std::map<GraphNode*, CriticalPathSection> CriticalSectionsMap;
        typedef std::vector<CriticalPathSection> CriticalSectionsList;

        typedef std::map<uint64_t, MPIEdge> MPIIdEdgeMap;
        typedef std::map<uint64_t, MPIIdEdgeMap> MPIRemoteEdgeMap;
        typedef std::map<uint64_t, MPICommGroup > MPICommGroupMap;

        MPIAnalysis(uint32_t mpiRank, uint32_t mpiSize);
        virtual ~MPIAnalysis();

        uint32_t getMPIRank() const;
        uint32_t getMPISize() const;
        uint32_t getMPIRank(uint64_t processId) const;
        uint32_t getMPIRank(uint64_t processId, const MPICommGroup &commGroup) const;
        void setMPIRank(uint64_t processId, uint32_t rank);
        void setMPICommGroupMap(uint32_t group, uint32_t numProcs, const uint64_t *procs);
        void createMPICommunicatorsFromMap();
        const MPICommGroup& getMPICommGroup(uint32_t group) const;

        void addRemoteMPIEdge(GraphNode *localNode, uint32_t remoteNodeID,
            uint64_t remoteProcessID, MPIEdgeDirection direction);
        bool getRemoteMPIEdge(uint32_t remoteNodeId, uint64_t remoteProcessId,
            MPIEdge &edge);

        ProcessNodePair getRemoteNodeInfo(GraphNode *localNode, bool *valid);
        std::set<uint32_t> getMpiPartnersRank(GraphNode *node);

        void reset();

    private:
        uint32_t mpiRank;
        uint32_t mpiSize;
        TokenTokenMap processRankMap;
        MPICommGroupMap mpiCommGroupMap;
        MPIRemoteEdgeMap remoteMpiEdgeMap;
        ReverseRemoteNodeMap reverseRemoteNodeMap;
    };
}

#endif	/* MPIANALYSIS_HPP */

