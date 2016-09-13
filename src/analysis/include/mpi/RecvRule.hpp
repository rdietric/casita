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
        if ( !recvLeave->isMPIRecv( ) || !recvLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        uint64_t partnerProcessId = recvLeave->getReferencedStreamId( );
        uint32_t partnerMPIRank   =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );
        
        // replay receive and retrieve information from communication partner
        uint64_t   buffer[CASITA_MPI_P2P_BUF_SIZE];
        MPI_Status status;
        MPI_CHECK( MPI_Recv( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerMPIRank, 
                             CASITA_MPI_REPLAY_TAG, 
                             MPI_COMM_WORLD, &status ) );
        
        GraphNode* recvEnter     = recvLeave->getGraphPair( ).first;
        uint64_t   sendStartTime = buffer[0];  
        uint64_t   recvStartTime = recvEnter->getTime( );
        uint64_t   recvEndTime   = recvLeave->getTime( );

        
        // TODO: MPI_Recv should be always blocking
        // check for MPI_ISEND as partner and skip analysis
        if ( buffer[CASITA_MPI_P2P_BUF_SIZE - 1] != MPI_ISEND )
        {
          
          // compute wait states and edges
          if ( recvStartTime < sendStartTime )
          {
            Edge* recvRecordEdge = commonAnalysis->getEdge( recvEnter,
                                                            recvLeave );
            
            if ( recvRecordEdge )
            {
              recvRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << recvLeave->getStreamId( ) 
                        << "] RecvRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            recvLeave->setCounter( WAITING_TIME, sendStartTime - recvStartTime );

#ifdef MPI_CP_MERGE
            analysis->getMPIAnalysis( ).addMPIEdge( recvEnter,
                                                    buffer[4],
                                                    partnerProcessId );
#endif
          }

          if ( recvStartTime > sendStartTime )
          {
            distributeBlame( commonAnalysis,
                             recvEnter,
                             recvStartTime - sendStartTime,
                             streamWalkCallback );
          }

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            recvLeave,
            (uint32_t)buffer[2],
            partnerProcessId,
            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL );
        }

        // send local information to communication partner to compute wait states
        // use another tag to not mix up with replayed communication
        buffer[0] = recvStartTime;
        buffer[1] = recvEndTime;
        buffer[2] = recvEnter->getId( );
        buffer[3] = recvLeave->getId( );
        buffer[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_RECV; //recv.second->getType( );

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
