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

  class SendRule :
    public IMPIRule
  {
    public:

      SendRule( int priority ) :
        IMPIRule( "SendRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI_Send leave */
        if ( !node->isMPISend( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair send  = node->getGraphPair( );
        
        // send
        uint64_t  sendStartTime = send.first->getTime( );
        uint64_t  sendEndTime   = send.second->getTime( );

        // allocated in class AnalysisParadigmMPI, still used in CPA later
        uint64_t* data = (uint64_t*)( node->getData( ) );
        uint64_t partnerProcessId = *data;
        
        // set referenced stream field and delete allocated memory of data field
        node->setReferencedStreamId( partnerProcessId );
        delete data;
        
        node->setReferencedStreamId( partnerProcessId ); // for debugging in CP analysis
        uint32_t  partnerMPIRank  =
          commonAnalysis->getMPIAnalysis( ).getMPIRank( partnerProcessId );

        // replay MPI_Send
        uint64_t buffer[CASITA_MPI_P2P_BUF_SIZE];
        
        buffer[0] = sendStartTime;
        buffer[1] = sendEndTime;
        buffer[2] = send.first->getId( );
        buffer[3] = send.second->getId( );
        buffer[CASITA_MPI_P2P_BUF_SIZE - 1] = MPI_SEND;//send.second->getType( );
        MPI_CHECK( MPI_Send( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerMPIRank,
                             CASITA_MPI_REPLAY_TAG, MPI_COMM_WORLD ) );
        
        // receive the communication partner start time to compute wait states
        // use another tag to not mix up with replayed communication
        MPI_Status status;
        uint64_t   recvStartTime = 0;
        MPI_CHECK( MPI_Recv( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerMPIRank,
                             CASITA_MPI_REVERS_REPLAY_TAG, MPI_COMM_WORLD, &status ) );
        recvStartTime = buffer[0];
        
        
        // if the communication partner is an MPI_Irecv we can stop here
        // as no wait states can be found
        if(buffer[CASITA_MPI_P2P_BUF_SIZE - 1] & MPI_IRECV )
        {
          return true;
        }
        else if ( buffer[CASITA_MPI_P2P_BUF_SIZE - 1] & MPI_SEND || 
                  buffer[CASITA_MPI_P2P_BUF_SIZE - 1] & MPI_ISEND )
        {
          // the communication partner should be a receive!!!
          std::cerr << "[" << node->getStreamId( ) 
                    << "] SendRule: Partner rank " << partnerMPIRank 
                    << " is MPI_[I]SEND "
                    << buffer[CASITA_MPI_P2P_BUF_SIZE - 1] << std::endl;
        }
        
        // compute wait states
        if ( ( sendStartTime <= recvStartTime ) )
        {
          if ( sendStartTime < recvStartTime )
          {
            Edge* sendRecordEdge = commonAnalysis->getEdge( send.first,
                                                            send.second );
            if ( sendRecordEdge )
            {
              sendRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << node->getStreamId( ) 
                        << "] SendRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            //\todo: write counter to enter node
            send.second->setCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                       CTR_WAITSTATE ),
                                     recvStartTime - sendStartTime );
          }
        }
        else
        {
          distributeBlame( commonAnalysis, send.first,
                           sendStartTime - recvStartTime, streamWalkCallback );
        }

        commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
          send.first,
          (uint32_t)buffer[3],
          partnerProcessId,
          MPIAnalysis::
          MPI_EDGE_LOCAL_REMOTE );

        return true;
      }
  };
 }
}
