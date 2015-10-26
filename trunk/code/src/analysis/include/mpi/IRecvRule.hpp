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

  /* What this rule does:
   * 1) Replay MPI_Irecv
   */
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
        
        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        uint64_t partnerProcessId      = node->getReferencedStreamId( );
        GraphNode::GraphNodePair& recv = node->getGraphPair( );
        EventStream::MPIIcommRecord* record = 
                (EventStream::MPIIcommRecord* )node->getData( );
        
        // check if the record has been invalidated/deleted
        if( NULL == record )
        {
          std::cerr << "[" << node->getStreamId( ) << "] Irecv rule: Invalid record data." 
                    << std::endl;
          
          return false;
        }

        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        // replay MPI_Irecv (receive buffer is never read as data are first valid in MPI_Wait[all])
        MPI_CHECK( MPI_Irecv( record->recvBuffer, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, 
                              CASITA_MPI_REPLAY_TAG, 
                              MPI_COMM_WORLD, 
                              &(record->requests[0]) ) );
        
        // send information to communication partner
        // the blocking MPI_Recv can evaluate them and e.g. stop wait state analysis
        uint64_t *buffer_send = record->sendBuffer;
        buffer_send[0] = recv.first->getTime( );
        buffer_send[3] = recv.second->getId( );
        buffer_send[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_IRECV; //recv.second->getType( );

        // Send indicator that this is an MPI_Irecv
        // use another tag to not mix up with replayed communication
        MPI_CHECK( MPI_Isend( buffer_send, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank,
                              CASITA_MPI_REVERS_REPLAY_TAG, 
                              MPI_COMM_WORLD, &(record->requests[1]) ) );

        // collect pending non-blocking MPI operations
        int finished = 0;

        MPI_Test(&(record->requests[0]), &finished, MPI_STATUS_IGNORE);
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[0] = MPI_REQUEST_NULL;
        }

        finished = 0;
        MPI_Test(&(record->requests[1]), &finished, MPI_STATUS_IGNORE);
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[1] = MPI_REQUEST_NULL;
        }

        return true;
      }
  };
 }
}
