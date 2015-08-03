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

        uint64_t   partnerProcessId    = node->getReferencedStreamId( );
        GraphNode::GraphNodePair& recv = node->getGraphPair( );

        /* Buffer size 5 necessary because original Send/Recv-Rules need that size. */
        const int BUFFER_SIZE = 5;
        uint64_t *buffer = (uint64_t *) malloc(sizeof(uint64_t) * BUFFER_SIZE); 
        // TODO: free buffer

        /* send */
        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        MPI_Request recvRequest;
        MPI_CHECK( MPI_Irecv( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, 
                              partnerMPIRank, 0, MPI_COMM_WORLD, &recvRequest ) );
        /*std::cerr << "[" << node->getStreamId( ) << "] IRecvRule: MPI_Irecv <- Rank " 
                  << partnerMPIRank << " (request: " << recvRequest << ") "
                  << node->getUniqueName( ) << std::endl;*/
        
        uint64_t *buffer_send = (uint64_t *) malloc(sizeof(uint64_t) * BUFFER_SIZE);
        //TODO: free buffer!!!
        buffer_send[0] = recv.first->getTime( );
        buffer_send[BUFFER_SIZE - 1] = recv.second->getType( );

        /* Send indicator that this is an MPI_Irecv */
        MPI_Request sendRequest;
        MPI_CHECK( MPI_Isend( buffer_send, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, partnerMPIRank,
                              0, MPI_COMM_WORLD, &sendRequest ) );
        
        /*std::cerr << "[" << node->getStreamId( ) << "] IRecvRule: MPI_Isend -> Rank " 
                  << partnerMPIRank << " (request: " << sendRequest << ") "
                  << node->getUniqueName( ) << " ISend start: " << buffer_send[0] 
                  << std::endl;*/
        
        // collect pending non-blocking MPI operations
        uint64_t* requestID = (uint64_t* )( node->getData( ) );
        std::cerr << "[" << node->getStreamId( ) << "] IRecvRule: requestID=" << *requestID << std::endl;
        
        // we are adding *requestID twice to the map -> fix it!
        int finished = 0;
        MPI_Status status;
        
        MPI_Test(&recvRequest, &finished, &status);
        if(!finished)
        {
          analysis->addPendingMPIRequest( *requestID, recvRequest );
        }

        finished = 0;
        MPI_Test(&sendRequest, &finished, &status);
        if(!finished)
        {
          analysis->addPendingMPIRequest( *requestID + 42, sendRequest );
        }

        return true;
      }
  };
 }
}
