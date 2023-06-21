/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <map>
#include <set>
#include <vector>
#include <stdint.h>
#include <mpi.h>

#include "common.hpp"
#include "graph/GraphNode.hpp"

namespace casita
{
 class AnalysisEngine;

 class MPIAnalysis
 {
   public:
     
     // counts the number of global MPI collectives
     uint32_t globalCollectiveCounter;

     typedef struct
     {
       uint64_t streamID;
       uint64_t nodeID;
     } RemoteNode;

   private:
     typedef std::map< uint32_t, uint32_t > TokenTokenMap;
     typedef std::map< uint64_t, GraphNode* > IdNodeMap;
     typedef std::map< GraphNode*, RemoteNode > RemoteNodeMap;
   public:

     typedef struct
     {
       MPI_Comm                comm;
       std::vector< uint32_t > procs;
     } MPICommGroup;

     enum MPIEdgeDirection
     {
       MPI_EDGE_LOCAL_REMOTE,
       MPI_EDGE_REMOTE_LOCAL
     };

     typedef struct
     {
       MPIEdgeDirection direction;
       GraphNode*       localNode;
       uint64_t         remoteNodeID; /* remote node ID */
       uint64_t         remoteStreamID; /* remote stream ID */
     } MPIEdge;

     typedef struct
     {
       GraphNode* startNode;
       GraphNode* endNode;
     } CriticalPathSection;

     typedef std::vector< CriticalPathSection > CriticalSectionsList;

     typedef std::map< uint64_t, MPIEdge > MPIIdEdgeMap;
     typedef std::map< uint64_t, MPIIdEdgeMap > MPIRemoteEdgeMap;
     typedef std::map< uint32_t, MPICommGroup > MPICommGroupMap;

     MPIAnalysis( uint32_t mpiRank, uint32_t mpiSize );
     virtual
     ~MPIAnalysis();

     uint32_t
     getMPISize() const;
     
     uint32_t
     getMPIRank() const;

     uint32_t
     getMPIRank( uint64_t streamId ) const;
     
     uint32_t
     getMPIRank( uint64_t streamId, const MPICommGroup& commGroup ) const;

     void
     setMPIRank( uint64_t streamId, uint32_t rank );
     
     uint64_t
     getStreamId( int rank, uint32_t comRef );

     void
     addMPICommGroup( uint32_t        group,
                      uint32_t        numProcs,
                      const uint32_t* procs );

     void
     createMPICommunicatorsFromMap();

     const MPICommGroup&
     getMPICommGroup( uint32_t group ) const;

     void
     addRemoteMPIEdge( GraphNode* localNode, uint64_t remoteNodeID,
                       uint64_t remoteProcessID );

     RemoteNode
     getRemoteNodeInfo( GraphNode* localNode, bool* valid );
     
     void
     removeRemoteNode( GraphNode* localNode );

     std::set< uint32_t >
     getMpiPartnersRanks( GraphNode* node );
     
     uint32_t
     getMpiPartnersRank( GraphNode* node );

     void
     reset();

   private:
     uint32_t             mpiRank;
     uint32_t             mpiSize;
     TokenTokenMap        streamIdRankMap;
     MPICommGroupMap      mpiCommGroupMap;
     MPIRemoteEdgeMap     remoteMpiEdgeMap;
     
     //<! Map MPI nodes to remote nodes (stream ID, node ID), which represents an edge
     RemoteNodeMap remoteNodeMap;  
 };
}
