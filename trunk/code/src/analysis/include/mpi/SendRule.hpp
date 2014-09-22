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

  class SendRule :
    public AbstractRule
  {
    public:

      SendRule( int priority ) :
        AbstractRule( "SendRule", priority )
      {

      }

      bool
      apply( AnalysisEngine* analysis, GraphNode* node )
      {
        /* applied at MPI_Send leave */
        if ( !node->isMPISend( ) || !node->isLeave( ) )
        {
          return false;
        }

        /* get the complete execution */
        GraphNode::GraphNodePair send = node->getGraphPair( );
        uint64_t* data = (uint64_t*)( send.second->getData( ) );
        uint64_t  partnerProcessId    = *data;

        const int BUFFER_SIZE         = 8;
        uint64_t  buffer[BUFFER_SIZE];
        /* uint64_t *bfr64 = (uint64_t*) buffer; */

        /* send */
        uint64_t  sendStartTime       = send.first->getTime( );
        uint64_t  sendEndTime         = send.second->getTime( );

        uint32_t  partnerMPIRank      = analysis->getMPIAnalysis( ).getMPIRank(
          partnerProcessId );
        /* memcpy(bfr64 + 0, &sendStartTime, sizeof (uint64_t)); */
        /* memcpy(bfr64 + 1, &sendEndTime, sizeof (uint64_t)); */

        buffer[0] = sendStartTime;
        buffer[1] = sendEndTime;
        buffer[2] = send.first->getId( );
        buffer[3] = send.second->getId( );
        buffer[BUFFER_SIZE - 1] = send.second->getType( );
        MPI_CHECK( MPI_Send( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                             partnerMPIRank,
                             0, MPI_COMM_WORLD ) );

        /* receive */
        MPI_Status status;
        uint64_t   recvStartTime = 0;
        /* uint64_t recvEndTime = 0; */
        MPI_CHECK( MPI_Recv( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                             partnerMPIRank,
                             0, MPI_COMM_WORLD, &status ) );
        recvStartTime = buffer[0];         /* bfr64[0]; */
        /* recvEndTime = bfr64[1]; */
        /* int partnerType = buffer[BUFFER_SIZE - 1]; */

        /* compute wait states */
        if ( ( sendStartTime <= recvStartTime ) )
        {
          if ( sendStartTime < recvStartTime )
          {
            Edge* sendRecordEdge = analysis->getEdge( send.first, send.second );
            sendRecordEdge->makeBlocking( );
            send.first->setCounter( analysis->getCtrTable( ).getCtrId(
                                      CTR_WAITSTATE ),
                                    recvStartTime - sendStartTime );
          }
        }
        else
        {
          distributeBlame( analysis, send.first,
                           sendStartTime - recvStartTime, streamWalkCallback );
        }

        analysis->getMPIAnalysis( ).addRemoteMPIEdge(
          send.first,
          (uint32_t)buffer[3],
          partnerProcessId,
          MPIAnalysis::
          MPI_EDGE_LOCAL_REMOTE );

        return true;
      }
  };
 }
}
