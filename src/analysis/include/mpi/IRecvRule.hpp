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

        std::cout << "[" << node->getStreamId( ) << "] IRECV " << node->getUniqueName( ) << " START" << std::endl;

        /* What this rule does:
         * 1) Recv type of Send
         * 2) If type is ISend, do nothing
         * 3) If type is Send, send own type: IRecv
         */

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair send  = node->getGraphPair( );
        uint64_t*    data = (uint64_t*)( send.second->getData( ) );
        uint64_t     partnerProcessId  = *data;

        /* Buffer size 5 necessary because original Send/Recv-Rules need that size. */
        const int    BUFFER_SIZE       = 5;
        uint64_t     buffer[BUFFER_SIZE];

        /* send */
        uint32_t     partnerMPIRank    =
          commonAnalysis->getMPIAnalysis( ).getMPIRank(
            partnerProcessId );
        uint32_t     myMpiRank         = commonAnalysis->getMPIAnalysis( ).getMPIRank( );

        MPI_Request* recvRequest       = new MPI_Request;
        MPI_CHECK( MPI_Irecv( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, partnerMPIRank,
                              0, MPI_COMM_WORLD, recvRequest ) );

        /* If partner is MPI_ISEND just do nothign */
        if ( buffer[BUFFER_SIZE - 1] = MPI_ISEND )
        {
          return true;
        }

        buffer[BUFFER_SIZE - 1] = send.second->getType( );

        /* Send indicator that this is a IRECV */
        MPI_Request* sendRequest = new MPI_Request;
        MPI_CHECK( MPI_Isend( buffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, partnerMPIRank,
                              0, MPI_COMM_WORLD, sendRequest ) );

        /*        commonAnalysis->addPendingMPIRequest( sendRequest ); */
        /*        commonAnalysis->addPendingMPIRequest( recvRequest ); */

        return true;
      }
  };
 }
}
