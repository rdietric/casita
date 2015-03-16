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

#include "IMPIRule.hpp"
#include "AnalysisParadigmMPI.hpp"

namespace casita
{
 namespace mpi
 {

  class SendRule :
    public IMPIRule
  {
    public:

      SendRule( int priority ) :
        IMPIRule( "SendRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI_Send leave */
        if ( !node->isMPISend( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair send  = node->getGraphPair( );
        uint64_t* data = (uint64_t*)( send.second->getData( ) );
        uint64_t  partnerProcessId     = *data;

        const int BUFFER_SIZE          = 5;
        uint64_t  buffer[BUFFER_SIZE];

        /* send */
        uint64_t  sendStartTime        = send.first->getTime( );
        uint64_t  sendEndTime          = send.second->getTime( );

        uint32_t  partnerMPIRank       =
          commonAnalysis->getMPIAnalysis( ).getMPIRank(
            partnerProcessId );

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
        MPI_CHECK( MPI_Recv( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                             partnerMPIRank,
                             0, MPI_COMM_WORLD, &status ) );
        recvStartTime = buffer[0];

        if ( buffer[BUFFER_SIZE - 1] == MPI_IRECV )
        {
          std::cout << "[" << node->getStreamId( ) << "] SEND " << node->getUniqueName( ) << " DONE " << std::endl;
          return true;
        }
        /* compute wait states */
        if ( ( sendStartTime <= recvStartTime ) )
        {
          if ( sendStartTime < recvStartTime )
          {
            Edge* sendRecordEdge = commonAnalysis->getEdge( send.first,
                                                            send.second );
            sendRecordEdge->makeBlocking( );
            send.second->setCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                       CTR_WAITSTATE ),
                                     recvStartTime - sendStartTime );
          }
        }
        else
        {
          distributeBlame( commonAnalysis, send.first,
                           sendStartTime - recvStartTime, streamWalkCallback );
        }

        commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
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
