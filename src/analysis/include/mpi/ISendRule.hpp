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
        /* applied at MPI_ISend leave */
        if ( !node->isMPIISend( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair send  = node->getGraphPair( );
        //uint64_t*    data = (uint64_t*)( send.second->getData( ) );
        uint64_t     partnerProcessId  = node->getReferencedStreamId();
        
        
        /* Buffer size 5 necessary because original Send/Recv-Rules need that size. */
        const int    BUFFER_SIZE       = 5;
        //uint64_t     buffer[BUFFER_SIZE];
        uint64_t *buffer = (uint64_t *) malloc(sizeof(uint64_t) * BUFFER_SIZE);

        /* send */
        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        buffer[0] = 0; // start time is not relevant
        buffer[1] = 0; // leave time is not relevant
        buffer[2] = send.first->getId( );  // send start node
        buffer[3] = send.second->getId( ); // send leave node
        buffer[BUFFER_SIZE - 1] = send.second->getType( );

        // replay the MPI_Isend
        MPI_Request sendRequest;
        MPI_CHECK( MPI_Isend( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, 
                              partnerMPIRank, 0, MPI_COMM_WORLD, &sendRequest ) );
        /*std::cerr << "[" << node->getStreamId( ) << "] ISendRule: MPI_Isend -> Rank " 
                  << partnerMPIRank << " (request: " << sendRequest << ") "
                  << node->getUniqueName( ) << std::endl;*/
        
        // MPI_Isend does not need to receive information!
        // But the MPI_Irecv does not now if the partner is an MPI_Isend or MPI_Send.
        
        uint64_t *buffer_recv = (uint64_t *) malloc(sizeof(uint64_t) * BUFFER_SIZE);

        // MPI_Irecv to have a matching partner for MPI_R[I]ecv rule
        MPI_Request recvRequest;
        MPI_CHECK( MPI_Irecv( buffer_recv, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, 
                              partnerMPIRank, 0, MPI_COMM_WORLD, &recvRequest ) );
        /*std::cerr << "[" << node->getStreamId( ) << "] ISendRule: MPI_Irecv <- Rank "
                  << partnerMPIRank << " (request: " << recvRequest << ") "
                  << node->getUniqueName( ) << std::endl;*/
        
        // collect pending MPI operations
        uint64_t* requestID = (uint64_t* )( node->getData( ) );
        //std::cerr << "[" << node->getStreamId( ) << "] ISendRule: requestID=" << *requestID << std::endl;
        int finished = 0;
        MPI_Status status;

        MPI_CHECK( MPI_Test(&sendRequest, &finished, &status) );
        if(!finished)
        {
          analysis->addPendingMPIRequest( *requestID, sendRequest );
        }

        finished = 0;
        MPI_CHECK( MPI_Test(&recvRequest, &finished, &status) );
        if(!finished)
        {
          analysis->addPendingMPIRequest( *requestID + 42, recvRequest );
        }

        return true;
      }
  };
 }
}
