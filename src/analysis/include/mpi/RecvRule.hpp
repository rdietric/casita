/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
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
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        // applied only at MPI_Recv leave
        if ( !node->isMPIRecv( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        uint64_t partnerProcessId = node->getReferencedStreamId( );
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
        
        GraphNode::GraphNodePair& recv = node->getGraphPair( );
        uint64_t   sendStartTime       = buffer[0];  
        uint64_t   recvStartTime       = recv.first->getTime( );
        uint64_t   recvEndTime         = recv.second->getTime( );

        
        // TODO: MPI_Recv should be always blocking
        // check for MPI_ISEND as partner and skip analysis
        if ( buffer[CASITA_MPI_P2P_BUF_SIZE - 1] != MPI_ISEND )
        {
          
          // compute wait states and edges
          if ( recvStartTime < sendStartTime )
          {
            Edge* recvRecordEdge = commonAnalysis->getEdge( recv.first,
                                                            recv.second );
            
            if ( recvRecordEdge )
            {
              recvRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << node->getStreamId( ) 
                        << "] RecvRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            recv.second->setCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                       CTR_WAITSTATE ), sendStartTime -
                                     recvStartTime );

#ifdef MPI_CP_MERGE
            analysis->getMPIAnalysis( ).addMPIEdge( recv.first,
                                                    buffer[4],
                                                    partnerProcessId );
#endif
          }

          if ( recvStartTime > sendStartTime )
          {
            distributeBlame( commonAnalysis,
                             recv.first,
                             recvStartTime - sendStartTime,
                             streamWalkCallback );
          }

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            recv.second,
            (uint32_t)buffer[2],
            partnerProcessId,
            MPIAnalysis::
            MPI_EDGE_REMOTE_LOCAL );
        }

        // send local information to communication partner to compute wait states
        // use another tag to not mix up with replayed communication
        buffer[0] = recvStartTime;
        buffer[1] = recvEndTime;
        buffer[2] = recv.first->getId( );
        buffer[3] = recv.second->getId( );
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
