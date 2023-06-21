/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
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

      /**\todo: simplify this rule. To a send and a receive operation? */
      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* sendRecvLeave )
      {
        // applied at MPI_SendRecv leave
        if ( !sendRecvLeave->isMPISendRecv() || !sendRecvLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getAnalysisEngine();
        
        // count occurrence
        commonAnalysis->getStatistics().countActivity( STAT_MPI_P2P );
        commonAnalysis->getStatistics().countActivity( STAT_MPI_P2P );
        
        MPIAnalysis& mpiAnalysis = commonAnalysis->getMPIAnalysis();

        GraphNode* sendRecvEnter = sendRecvLeave->getGraphPair().first;

        uint32_t* data = (uint32_t*)( sendRecvLeave->getData() );
        UTILS_ASSERT( data, "No data found for %s",
                      sendRecvLeave->getUniqueName().c_str() );

        int sendRank     = (int) data[ 0 ];
        uint32_t sendTag = data[ 1 ];
        int recvRank     = (int) data[ 2 ];
        uint32_t recvTag = data[ 3 ];
        
        delete[] data;
        
        const uint32_t comRef = (uint32_t) sendRecvLeave->getReferencedStreamId();
        MPI_Comm communicator = mpiAnalysis.getMPICommGroup( comRef ).comm;

        uint64_t sendBuffer[ CASITA_MPI_P2P_BUF_SIZE ];
        uint64_t recvBuffer[ CASITA_MPI_P2P_BUF_SIZE ];

        const uint64_t myStartTime = sendRecvEnter->getTime();
        const uint64_t myEndTime   = sendRecvLeave->getTime();

        // prepare send buffer
        sendBuffer[0] = myStartTime;
        sendBuffer[1] = myEndTime;
        sendBuffer[2] = sendRecvEnter->getId();
        sendBuffer[3] = sendRecvLeave->getId();
        sendBuffer[CASITA_MPI_P2P_BUF_LAST] = MPI_SEND | MPI_RECV;

        // replay: get information from receive rank
        MPI_CHECK( MPI_Sendrecv( sendBuffer, CASITA_MPI_P2P_BUF_SIZE, MPI_UINT64_T, 
                                 sendRank, sendTag, //CASITA_MPI_REPLAY_TAG,
                                 recvBuffer, CASITA_MPI_P2P_BUF_SIZE, MPI_UINT64_T, 
                                 recvRank, recvTag, //CASITA_MPI_REPLAY_TAG,
                                 communicator, MPI_STATUS_IGNORE ) );

        // evaluate receive buffer
        const uint64_t recvRankStartTime = recvBuffer[0];
        const uint64_t recvRankEndTime   = recvBuffer[1];
        const uint64_t recvRankEnterId   = recvBuffer[2];
        //uint64_t otherLeaveId   = recvBuffer[3];

        // if send and receive rank are the same we do not need the revers replay
        // as there is only one remote partner which also has sendRank == recvRank
        if( sendRank == recvRank )
        {
          // handle similar to MPI_Recv
          // if this operation is early, it is blocking and not on the CP
          if ( myStartTime < recvRankStartTime )
          {
            Edge* myEdge = 
              commonAnalysis->getEdge( sendRecvEnter, sendRecvLeave );

            if ( myEdge )
            {
              myEdge->makeBlocking();

              uint64_t partnerStreamId = 
                mpiAnalysis.getStreamId( recvRank, comRef );
              
              // point to potential continuation (node ID, stream ID)
              commonAnalysis->getMPIAnalysis().addRemoteMPIEdge(
                sendRecvLeave, recvRankEnterId, partnerStreamId );
            }
            else
            {
              UTILS_OUT( "[%" PRIu64 "] SendRecvRule: Edge not found. "
                         "Critical path analysis might fail!",
                         sendRecvLeave->getStreamId() );
            }
            
            // add waiting time to statistics
            commonAnalysis->getStatistics().addStatWithCount( 
              MPI_STAT_SENDRECV, recvRankStartTime - myStartTime );

            sendRecvLeave->incCounter( WAITING_TIME, recvRankStartTime - myStartTime );
          }
          // if this operation starts after the other AND they are overlapping, 
          else 
          if ( myStartTime != recvRankStartTime && myStartTime < recvRankEndTime )
          {
            distributeBlame( commonAnalysis, sendRecvEnter,
                             myStartTime - recvRankStartTime, 
                             mpiStreamWalkCallback, REASON_MPI_LATE_SENDRECV );
          }
          
          return true;
        }

        // send and receive rank are distinct
        // reverse replay: get information from send rank
        MPI_CHECK( MPI_Sendrecv( sendBuffer, CASITA_MPI_P2P_BUF_SIZE,
                                 MPI_UINT64_T, recvRank, 
                                 recvTag + CASITA_MPI_REVERS_REPLAY_TAG,
                                 recvBuffer, CASITA_MPI_P2P_BUF_SIZE,
                                 MPI_UINT64_T, sendRank, 
                                 sendTag + CASITA_MPI_REVERS_REPLAY_TAG,
                                 communicator, MPI_STATUS_IGNORE ) );

        const uint64_t sendRankStartTime = recvBuffer[0];
        const uint64_t sendRankEnterId   = recvBuffer[2];
        
        // set myself as last entering stream
        uint64_t lastStreamId = sendRecvLeave->getStreamId();
        uint64_t lastNodeId   = sendRecvLeave->getId();
        uint64_t lastTime     = myStartTime;
        
        // set my operation edge to NULL 
        // (if it gets set, this operation is not the last)
        Edge* myEdge = NULL;

        // handle similar to MPI_Recv, as this operation cannot end before the
        // sender started (no overlap check needed)
        if ( myStartTime < recvRankStartTime )
        {
          // this operation started early, hence it is blocking
          myEdge = commonAnalysis->getEdge( sendRecvEnter, sendRecvLeave );

          if ( myEdge )
          {
            myEdge->makeBlocking();
          }
          else
          {
            UTILS_WARNING( "[%" PRIu64 "] SendrecvRule: Activity edge not found.", 
                           sendRecvLeave->getStreamId() );
          }
          
          lastStreamId = mpiAnalysis.getStreamId( recvRank, comRef );
          lastNodeId   = recvRankEnterId;
          lastTime     = recvRankStartTime;
        }
        
        // handle similar to MPI_Send (which is often buffered)
        // early sender/late receiver AND operations are overlapping
        if ( myStartTime < sendRankStartTime && myEndTime > sendRankStartTime )
        {
          // this operation started before the receive rank operation
          // blocking edge is already set
          if( myEdge )
          {
            // get the latest starter
            if( sendRankStartTime < recvRankStartTime )
            {
              // receive rank is the latest
              lastStreamId = mpiAnalysis.getStreamId( recvRank, comRef );
              lastNodeId   = recvRankEnterId;
              lastTime     = recvRankStartTime;
            }
            else
            {
              // send rank is the latest
              lastStreamId = mpiAnalysis.getStreamId( sendRank, comRef );
              lastNodeId   = sendRankEnterId;
              lastTime     = sendRankStartTime;
            }
          }
          else// create blocking edge due to send rank operation
          {
            myEdge = commonAnalysis->getEdge( sendRecvEnter, sendRecvLeave );

            if ( myEdge )
            {
              myEdge->makeBlocking();
            }
            else
            {
              UTILS_WARNING( "[%" PRIu64 "] SendrecvRule: Activity edge not found.", 
                             sendRecvLeave->getStreamId() );
            }

            lastStreamId = mpiAnalysis.getStreamId( sendRank, comRef );
            lastNodeId   = sendRankEnterId;
            lastTime     = sendRankStartTime;
          }
        }
        
        // if this operation is blocking, we need a continuation edge for CPA
        if( myEdge )
        {
          uint64_t waitEnd = std::min( lastTime, myEndTime );
          if( myStartTime < waitEnd )
          {
            commonAnalysis->getStatistics().addStatWithCount( 
              MPI_STAT_SENDRECV, waitEnd - myStartTime );
            
            sendRecvLeave->incCounter( WAITING_TIME, waitEnd - myStartTime );
          }
          else
          {
            UTILS_WARNING( "[%" PRIu64 "] SendrecvRule: Error in determining "
                           "waiting time." );
          }
          
          // point to potential continuation (node ID, stream ID)
          commonAnalysis->getMPIAnalysis().addRemoteMPIEdge(
                sendRecvLeave, lastNodeId, lastStreamId );
        }
        else // this operation was last (all other enter times are smaller)
        {
          // distribute blame from both
          uint64_t blame = 0;
          if ( myStartTime != recvRankStartTime && myStartTime < recvRankEndTime )
          {
            blame = myStartTime - recvRankStartTime;
          }
          
          // late sender
          if( myStartTime > sendRankStartTime )
          {
            blame += myStartTime - sendRankStartTime;
          }
          
          if( blame > 0 )
          {
            distributeBlame( commonAnalysis, sendRecvEnter,
                             blame, mpiStreamWalkCallback, 
                             REASON_MPI_LATE_SENDRECV );
          }
        }

        return true;
      }
  };

 }
}
