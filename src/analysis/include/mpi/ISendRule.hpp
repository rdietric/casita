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

  /** What this rule does:
   *  1) Send indicator that this is an ISEND
   *  ...
   */
  class ISendRule :
    public IMPIRule
  {
    public:

      ISendRule( int priority ) :
        IMPIRule( "ISendRule", priority )
      {

      }
        
      typedef std::vector< MPI_Request > MPIRequestList;

    private:
        
      MPIRequestList pendingMPIRequests;

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        // applied at MPI_ISend leave
        if ( !node->isMPIISend( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode::GraphNodePair send  = node->getGraphPair( );
        EventStream::MPIIcommRecord* record = 
                (EventStream::MPIIcommRecord* ) node->getData( );
        
        uint64_t *buffer = record->sendBuffer;
        
        buffer[0] = 0; // start time is not relevant
        buffer[1] = 0; // leave time is not relevant
        buffer[2] = send.first->getId( );  // send start node
        buffer[3] = send.second->getId( ); // send leave node
        buffer[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_ISEND; //send.second->getType( );
        
        uint64_t partnerProcessId  = node->getReferencedStreamId();
        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        // replay the MPI_Isend and provide the receiver with local information
        // a blocking MPI_Recv can distribute blame then
        MPI_CHECK( MPI_Isend( buffer, CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, 0, MPI_COMM_WORLD, &(record->requests[0]) ) );
        
        // MPI_Isend does not need to receive information!
        // But the MPI_Irecv does not know if the partner is an MPI_Isend or MPI_Send.

        // MPI_Irecv to have a matching partner for MPI_[I]Send rule
        MPI_CHECK( MPI_Irecv( record->recvBuffer, CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, 42, MPI_COMM_WORLD, 
                              &(record->requests[1]) ) );
        /*
        std::cerr << "[" << node->getStreamId( ) << "] ISendRule - record data after Icomm:" 
                  << " node adress: "     << record->leaveNode
                  << " MPI_Request[0]: "  << record->requests[0]
                  << " MPI_Request[1]: "  << record->requests[1]
                  << " OTF2 request ID: " << record->requestId
                  << std::endl;
        */
        // collect pending MPI operations
        int finished = 0;
        MPI_Status status;

        MPI_CHECK( MPI_Test(&(record->requests[0]), &finished, &status) );
        if(finished)
        {
          record->requests[0] = MPI_REQUEST_NULL;
        }

        finished = 0;
        MPI_CHECK( MPI_Test(&(record->requests[1]), &finished, &status) );
        if(finished)
        {
          record->requests[0] = MPI_REQUEST_NULL;
        }

        return true;
      }
  };
 }
}
