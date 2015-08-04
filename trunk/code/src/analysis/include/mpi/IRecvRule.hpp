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

  class IRecvRule :
    public IMPIRule
  {
    public:

      IRecvRule( int priority ) :
        IMPIRule( "IRecvRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI_IRecv leave */
        if ( !node->isMPIIRecv( ) || !node->isLeave( ) )
        {
          return false;
        }

        /* What this rule does:
         * 1) Recv type of Send
         * 2) If type is ISend, do nothing
         * 3) If type is Send, send own type: IRecv
         */

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        uint64_t partnerProcessId      = node->getReferencedStreamId( );
        GraphNode::GraphNodePair& recv = node->getGraphPair( );
        EventStream::MPIIcommRecord* record = 
                (EventStream::MPIIcommRecord* )node->getData( );

        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        MPI_CHECK( MPI_Irecv( record->recvBuffer, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, 0, MPI_COMM_WORLD, 
                              &(record->requests[0]) ) );
        /*std::cerr << "[" << node->getStreamId( ) << "] IRecvRule: MPI_Irecv <- Rank " 
                  << partnerMPIRank << " (request: " << recvRequest << ") "
                  << node->getUniqueName( ) << std::endl;*/
        
        uint64_t *buffer_send = record->sendBuffer;
        buffer_send[0] = recv.first->getTime( );
        buffer_send[CASITA_MPI_P2P_BUF_SIZE - 1] = recv.second->getType( );

        /* Send indicator that this is an MPI_Irecv */
        MPI_CHECK( MPI_Isend( buffer_send, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank,
                              0, MPI_COMM_WORLD, &(record->requests[1]) ) );
        
        /*std::cerr << "[" << node->getStreamId( ) << "] IRecvRule: MPI_Isend -> Rank " 
                  << partnerMPIRank << " (request: " << sendRequest << ") "
                  << node->getUniqueName( ) << " ISend start: " << buffer_send[0] 
                  << std::endl;*/
        
        // collect pending non-blocking MPI operations
        int finished = 0;
        MPI_Status status;

        MPI_Test(&(record->requests[0]), &finished, &status);
        if(finished)
        {
          // TODO: check if necessary!!!
          std::cerr << "[" << node->getStreamId( ) << "] record->requests[0] = "
                    << record->requests[0] << " MPI_REQUEST_NULL=" << MPI_REQUEST_NULL << std::endl;
          record->requests[0] = MPI_REQUEST_NULL;
        }

        finished = 0;
        MPI_Test(&(record->requests[1]), &finished, &status);
        if(finished)
        {
          std::cerr << "[" << node->getStreamId( ) << "] record->requests[0] = "
                    << record->requests[0] << " MPI_REQUEST_NULL=" << MPI_REQUEST_NULL << std::endl;
          record->requests[1] = MPI_REQUEST_NULL;
        }

        return true;
      }
  };
 }
}
