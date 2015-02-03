/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014,
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

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) { throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
  }

namespace casita
{
 class AnalysisEngine;

 class MPIAnalysis
 {
   public:

     typedef struct
     {
       uint64_t streamID;
       uint32_t nodeID;
     } ProcessNodePair;

   private:
     typedef std::map< uint32_t, uint32_t > TokenTokenMap;
     typedef std::map< uint64_t, GraphNode* > IdNodeMap;
     typedef std::map< uint64_t, IdNodeMap > RemoteNodeMap;
     typedef std::map< GraphNode*, ProcessNodePair > ReverseRemoteNodeMap;
   public:

     typedef struct
     {
       MPI_Comm             comm;
       std::set< uint64_t > procs;
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
       /* uint64_t prevStreamID; */
       uint64_t streamID;
       /* uint64_t nextStreamID; */
       uint32_t nodeStartID;
       uint32_t nodeEndID;
     } CriticalPathSection;

     typedef std::map< GraphNode*, CriticalPathSection > CriticalSectionsMap;
     typedef std::vector< CriticalPathSection > CriticalSectionsList;

     typedef std::map< uint64_t, MPIEdge > MPIIdEdgeMap;
     typedef std::map< uint64_t, MPIIdEdgeMap > MPIRemoteEdgeMap;
     typedef std::map< uint64_t, MPICommGroup > MPICommGroupMap;

     MPIAnalysis( uint32_t mpiRank, uint32_t mpiSize );
     virtual
     ~MPIAnalysis( );

     uint32_t
     getMPIRank( ) const;

     uint32_t
     getMPISize( ) const;

     uint32_t
     getMPIRank( uint64_t streamId ) const;

     uint32_t
     getMPIRank( uint64_t streamId, const MPICommGroup& commGroup ) const;

     void
     setMPIRank( uint64_t streamId, uint32_t rank );

     void
     setMPICommGroupMap( uint32_t        group,
                         uint32_t        numProcs,
                         const uint64_t* procs );

     void
     createMPICommunicatorsFromMap( );

     const MPICommGroup&
     getMPICommGroup( uint32_t group ) const;

     void
     addRemoteMPIEdge( GraphNode* localNode, uint32_t remoteNodeID,
                       uint64_t remoteProcessID, MPIEdgeDirection direction );

     bool
     getRemoteMPIEdge( uint32_t remoteNodeId, uint64_t remoteProcessId,
                       MPIEdge& edge );

     ProcessNodePair
     getRemoteNodeInfo( GraphNode* localNode, bool* valid );

     std::set< uint32_t >
     getMpiPartnersRank( GraphNode* node );

     void
     reset( );

   private:
     uint32_t             mpiRank;
     uint32_t             mpiSize;
     TokenTokenMap        processRankMap;
     MPICommGroupMap      mpiCommGroupMap;
     MPIRemoteEdgeMap     remoteMpiEdgeMap;
     ReverseRemoteNodeMap reverseRemoteNodeMap;
 };
}
