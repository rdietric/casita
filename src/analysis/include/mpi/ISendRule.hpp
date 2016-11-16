/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
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
   *  1) Forward replay: MPI_Isend
   *  2) Backward replay: MPI_Irecv
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
      apply( AnalysisParadigmMPI* analysis, GraphNode* isendLeave )
      {
        // applied at MPI_ISend leave
        if ( !isendLeave->isMPI_Isend() || isendLeave->isEnter() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();

        EventStream::MPIIcommRecord* record = 
                (EventStream::MPIIcommRecord* ) isendLeave->getData( );
        
        // check if the record has been invalidated/deleted
        if( NULL == record )
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE, 
                     "[%" PRIu64 "] MPI_Isend rule: Invalid record data.",
                     isendLeave->getStreamId());
          
          return false;
        }
        
        GraphNode* isendEnter = isendLeave->getGraphPair().first;
        
        uint64_t *buffer = record->sendBuffer;
        
        buffer[0] = isendEnter->getTime(); // isend start time
        buffer[1] = isendLeave->getTime(); // isend leave time
        buffer[2] = isendEnter->getId();  // send start node
        buffer[3] = isendLeave->getId(); // send leave node
        buffer[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_ISEND; //send.second->getType( );
        
        uint64_t partnerProcessId = isendLeave->getReferencedStreamId();
        uint32_t partnerMPIRank =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        // replay the MPI_Isend and provide the receiver with local information
        // a blocking MPI_Recv can distribute blame then
        MPI_CHECK( MPI_Isend( buffer, CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, CASITA_MPI_REPLAY_TAG, 
                              MPI_COMM_WORLD, &(record->requests[1]) ) );
        
        // MPI_Isend would like to know if partner is an MPI_Irecv or MPI_Recv
        // for the latter we need the dependency edge
        // MPI_Irecv does not know if the partner is an MPI_Isend or MPI_Send.

        // MPI_Irecv to have a matching partner for MPI_[I]Send rule
        MPI_CHECK( MPI_Irecv( record->recvBuffer, CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerMPIRank, CASITA_MPI_REVERS_REPLAY_TAG, 
                              MPI_COMM_WORLD, 
                              &(record->requests[0]) ) );
        
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
        MPI_CHECK( MPI_Test(&(record->requests[1]), &finished, MPI_STATUS_IGNORE ) );
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[1] = MPI_REQUEST_NULL; 
        }

        finished = 0;
        MPI_CHECK( MPI_Test(&(record->requests[0]), &finished, MPI_STATUS_IGNORE ) ); 
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[0] = MPI_REQUEST_NULL; 
          
          /* if receive finished, we can use the buffer
          
          if(record->recvBuffer[CASITA_MPI_P2P_BUF_SIZE - 1] & MPI_RECV )
          {
            commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
              send.first,
              (uint32_t)record->recvBuffer[3],
              partnerProcessId,
              MPIAnalysis::MPI_EDGE_LOCAL_REMOTE );
          }*/
        }
        
        return true;
      }
  };
 }
}
