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

#include <mpi.h>
#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace casita
{
 namespace mpi
 {

  class RecvRule :
    public AbstractRule
  {
    public:

      RecvRule( int priority ) :
        AbstractRule( "RecvRule", priority )
      {

      }

      bool
      apply( AnalysisEngine* analysis, GraphNode* node )
      {
        /* applied at MPI_Recv leave */
        if ( !node->isMPIRecv( ) || !node->isLeave( ) )
        {
          return false;
        }

        uint64_t partnerProcessId = node->getReferencedStreamId( );
        GraphNode::GraphNodePair& recv = node->getGraphPair( );

        const int BUFFER_SIZE = 8;
        uint64_t buffer[BUFFER_SIZE];
        /* uint64_t *bfr64 = (uint64_t*) buffer; */

        /* receive */
        uint32_t partnerMPIRank = analysis->getMPIAnalysis( ).getMPIRank(
          partnerProcessId );
        MPI_Status status;
        MPI_CHECK( MPI_Recv( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                             partnerMPIRank, 0,
                             MPI_COMM_WORLD, &status ) );
        uint64_t sendStartTime = buffer[0];         /* bfr64[0]; */
        /* uint64_t sendEndTime = bfr64[1]; */
        /* int partnerType = buffer[BUFFER_SIZE - 1]; */

        uint64_t recvStartTime = recv.first->getTime( );
        uint64_t recvEndTime = recv.second->getTime( );

        /* compute wait states and edges */
        if ( recvStartTime < sendStartTime )
        {
          Edge* recvRecordEdge = analysis->getEdge( recv.first, recv.second );
          recvRecordEdge->makeBlocking( );
          recv.first->setCounter( analysis->getCtrTable( ).getCtrId(
                                    CTR_WAITSTATE ), sendStartTime -
                                  recvStartTime );

#ifdef MPI_CP_MERGE
          analysis->getMPIAnalysis( ).addMPIEdge( recv.first,
                                                  buffer[4],
                                                  partnerProcessId );
#endif
        }

        if ( recvStartTime > sendStartTime )
        {
          distributeBlame( analysis,
                           recv.first,
                           recvStartTime - sendStartTime,
                           streamWalkCallback );
        }

        analysis->getMPIAnalysis( ).addRemoteMPIEdge(
          recv.second,
          (uint32_t)buffer[2],
          partnerProcessId,
          MPIAnalysis::
          MPI_EDGE_REMOTE_LOCAL );

        /* send */
        /* memcpy(bfr64 + 0, &recvStartTime, sizeof (uint64_t)); */
        /* memcpy(bfr64 + 1, &recvEndTime, sizeof (uint64_t)); */
        buffer[0] = recvStartTime;
        buffer[1] = recvEndTime;
        buffer[2] = recv.first->getId( );
        buffer[3] = recv.second->getId( );
        buffer[BUFFER_SIZE - 1] = recv.second->getType( );
        MPI_CHECK( MPI_Send( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                             partnerMPIRank,
                             0, MPI_COMM_WORLD ) );

        return true;
      }
  };
 }
}