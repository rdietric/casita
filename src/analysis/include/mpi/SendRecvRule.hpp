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

  class SendRecvRule :
    public IMPIRule
  {
    public:

      SendRecvRule( int priority ) :
        IMPIRule( "SendRecvRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* sendRecvLeave )
      {
        // applied at MPI send recv leave
        if ( !sendRecvLeave->isMPISendRecv( ) || !sendRecvLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode* sendRecvEnter = sendRecvLeave->getGraphPair( ).first;

        uint64_t* data =
          (uint64_t*)( sendRecvLeave->getData( ) );
        UTILS_ASSERT( data, "No data found for %s",
                      sendRecvLeave->getUniqueName( ).c_str( ) );

        uint64_t  partnerProcessIdRecv = sendRecvLeave->getReferencedStreamId( );
        uint64_t  partnerProcessIdSend = *data;

        const int BUFFER_SIZE = 8;
        uint64_t  sendBuffer[BUFFER_SIZE], recvBuffer[BUFFER_SIZE];

        uint64_t  myStartTime = sendRecvEnter->getTime( );
        uint64_t  myEndTime   = sendRecvLeave->getTime( );

        // prepare send buffer
        sendBuffer[0] = myStartTime;
        sendBuffer[1] = myEndTime;
        sendBuffer[2] = sendRecvEnter->getId( );
        sendBuffer[3] = sendRecvLeave->getId( );

        /* send + recv */
        uint32_t   partnerMPIRankRecv =
          commonAnalysis->getMPIAnalysis( ).getMPIRank(
            partnerProcessIdRecv );
        uint32_t   partnerMPIRankSend =
          commonAnalysis->getMPIAnalysis( ).getMPIRank(
            partnerProcessIdSend );
        MPI_Status status;

        /* round 1: send same direction. myself == send */

        MPI_CHECK( MPI_Sendrecv( sendBuffer, BUFFER_SIZE,
                                 MPI_UINT64_T, partnerMPIRankSend, 0,
                                 recvBuffer, BUFFER_SIZE,
                                 MPI_UINT64_T, partnerMPIRankRecv, 0,
                                 MPI_COMM_WORLD, &status ) );

        /* evaluate receive buffer */
        uint64_t otherStartTime = recvBuffer[0]; 
        uint64_t otherEnterId   = recvBuffer[2];
        uint64_t otherLeaveId   = recvBuffer[3];

        /* compute wait states */
        if ( ( myStartTime <= otherStartTime ) )
        {
          if ( myStartTime < otherStartTime )
          {
            Edge* sendRecordEdge = commonAnalysis->getEdge( sendRecvEnter,
                                                            sendRecvLeave );
            
            if ( sendRecordEdge )
            {
              sendRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << sendRecvLeave->getStreamId( ) 
                        << "] SendRecvRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }

            sendRecvLeave->incCounter( WAITING_TIME,
                                         otherStartTime - myStartTime );
          }
        }
        else
        {
          distributeBlame( commonAnalysis, sendRecvEnter,
                           myStartTime - otherStartTime, streamWalkCallback );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            sendRecvEnter,
            otherLeaveId,
            partnerProcessIdRecv,
            MPIAnalysis::MPI_EDGE_LOCAL_REMOTE );
        }

        // round 2: send reverse direction. myself == recv
        MPI_CHECK( MPI_Sendrecv( sendBuffer, BUFFER_SIZE,
                                 MPI_UINT64_T, partnerMPIRankRecv, 0,
                                 recvBuffer, BUFFER_SIZE,
                                 MPI_UINT64_T, partnerMPIRankSend, 0,
                                 MPI_COMM_WORLD, &status ) );

        otherStartTime = recvBuffer[0]; 
        otherEnterId   = recvBuffer[2];
        otherLeaveId   = recvBuffer[3];

        // compute wait states and edges
        if ( myStartTime < otherStartTime )
        {
          Edge* recvRecordEdge = commonAnalysis->getEdge( sendRecvEnter,
                                                          sendRecvLeave );
          if ( recvRecordEdge )
          {
            recvRecordEdge->makeBlocking( );
          }
          else
          {
            std::cerr << "[" << sendRecvLeave->getStreamId( ) 
                      << "] SendRecvRule: Record edge not found. CPA might fail!" 
                      << std::endl;
          }
          
          sendRecvLeave->incCounter( WAITING_TIME,
                                       otherStartTime - myStartTime );
        }

        if ( myStartTime > otherStartTime )
        {
          distributeBlame( commonAnalysis, sendRecvEnter,
                           myStartTime - otherStartTime, streamWalkCallback );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            sendRecvLeave,
            otherEnterId,
            partnerProcessIdSend,
            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL );
        }

        return true;
      }
  };

 }
}
