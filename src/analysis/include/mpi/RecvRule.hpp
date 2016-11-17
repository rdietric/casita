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

  class RecvRule :
    public IMPIRule
  {
    public:

      RecvRule( int priority ) :
        IMPIRule( "RecvRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* recvLeave )
      {
        // applied only at MPI_Recv leave
        if ( !recvLeave->isMPIRecv() || !recvLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();

        uint64_t partnerProcessId = recvLeave->getReferencedStreamId();
        uint32_t partnerMPIRank   =
          commonAnalysis->getMPIAnalysis().getMPIRank( partnerProcessId );
        
        // replay receive and retrieve information from communication partner
        uint64_t   buffer[CASITA_MPI_P2P_BUF_SIZE];
        MPI_CHECK( MPI_Recv( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerMPIRank, 
                             CASITA_MPI_REPLAY_TAG, 
                             MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );
        
        GraphNode* recvEnter     = recvLeave->getGraphPair().first;
        uint64_t   sendStartTime = buffer[0];
        uint64_t   sendEndTime   = buffer[1];
        uint64_t   recvStartTime = recvEnter->getTime();
        uint64_t   recvEndTime   = recvLeave->getTime();

        // if send starts after receive, we found a late sender
        if ( recvStartTime < sendStartTime )
        {
          Edge* recvRecordEdge = commonAnalysis->getEdge(recvEnter, recvLeave);

          if ( recvRecordEdge )
          {
            recvRecordEdge->makeBlocking();

            commonAnalysis->getMPIAnalysis().addRemoteMPIEdge(
              recvLeave,
              buffer[2], // remote node ID (send enter)
              partnerProcessId );
          }
          else
          {
            UTILS_MSG( true, "["PRIu64"] RecvRule: Record edge not found."
                             "Critical path analysis might fail!", 
                             recvLeave->getStreamId() );
          }

          recvLeave->setCounter( WAITING_TIME, sendStartTime - recvStartTime );

          // if receive starts after send AND send and recv are overlapping, 
          // we found a later receiver
          if ( recvStartTime > sendStartTime && sendEndTime > recvStartTime )
          {
            distributeBlame( commonAnalysis,
                             recvEnter,
                             recvStartTime - sendStartTime,
                             streamWalkCallback );
          }
        }

        // send local information to communication partner to compute wait states
        // use another tag to not mix up with replayed communication
        buffer[0] = recvStartTime;
        buffer[1] = recvEndTime;
        buffer[2] = recvEnter->getId();
        buffer[3] = recvLeave->getId();
        buffer[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_RECV;

        MPI_CHECK( MPI_Send( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerMPIRank,
                             CASITA_MPI_REVERS_REPLAY_TAG, 
                             MPI_COMM_WORLD ) );

        return true;
      }
  };
 }
}
