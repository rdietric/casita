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

#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace casita
{
 namespace mpi
 {

  class SendRecvRule :
    public AbstractRule
  {
    public:

      SendRecvRule( int priority ) :
        AbstractRule( "SendRecvRule", priority )
      {

      }

      bool
      apply( AnalysisEngine* analysis, GraphNode* node )
      {
        /* applied at MPI send recv leave */
        if ( !node->isMPISendRecv( ) || !node->isLeave( ) )
        {
          return false;
        }

        /* get the complete execution */
        GraphNode::GraphNodePair sendRecv = node->getGraphPair( );

        uint64_t* data = (uint64_t*)( sendRecv.second->getData( ) );

        uint64_t partnerProcessIdRecv = node->getReferencedStreamId( );
        uint64_t partnerProcessIdSend = *data;

        const int BUFFER_SIZE = 8;
        uint64_t sendBuffer[BUFFER_SIZE], recvBuffer[BUFFER_SIZE];
        /* uint64_t *sendBfr64 = (uint64_t*) sendBuffer; */
        /* uint64_t *recvBfr64 = (uint64_t*) recvBuffer; */

        uint64_t myStartTime = sendRecv.first->getTime( );
        uint64_t myEndTime = sendRecv.second->getTime( );

        /* prepare send buffer */
        /* memcpy(sendBfr64 + 0, &myStartTime, sizeof (uint64_t)); */
        /* memcpy(sendBfr64 + 1, &myEndTime, sizeof (uint64_t)); */
        sendBuffer[0] = myStartTime;
        sendBuffer[1] = myEndTime;
        sendBuffer[2] = sendRecv.first->getId( );
        sendBuffer[3] = sendRecv.second->getId( );

        /* send + recv */
        uint32_t partnerMPIRankRecv = analysis->getMPIAnalysis( ).getMPIRank(
          partnerProcessIdRecv );
        uint32_t partnerMPIRankSend = analysis->getMPIAnalysis( ).getMPIRank(
          partnerProcessIdSend );
        MPI_Status status;

        /* round 1: send same direction. myself == send */

        MPI_CHECK( MPI_Sendrecv( sendBuffer, BUFFER_SIZE,
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankSend, 0,
                                 recvBuffer, BUFFER_SIZE,
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankRecv, 0,
                                 MPI_COMM_WORLD, &status ) );

        /* evaluate receive buffer */
        uint64_t otherStartTime = recvBuffer[0];         /*
                                                          *recvBfr64[0];
                                                          **/
        uint64_t otherEnterId = recvBuffer[2];
        uint64_t otherLeaveId = recvBuffer[3];

        /* compute wait states */
        if ( ( myStartTime <= otherStartTime ) )
        {
          if ( myStartTime < otherStartTime )
          {
            Edge* sendRecordEdge = analysis->getEdge( sendRecv.first,
                                                      sendRecv.second );
            sendRecordEdge->makeBlocking( );
            sendRecv.first->incCounter( analysis->getCtrTable( ).getCtrId(
                                          CTR_WAITSTATE ),
                                        otherStartTime - myStartTime );
          }
        }
        else
        {
          distributeBlame( analysis, sendRecv.first,
                           myStartTime - otherStartTime, streamWalkCallback );

          analysis->getMPIAnalysis( ).addRemoteMPIEdge(
            sendRecv.first,
            otherLeaveId,
            partnerProcessIdRecv,
            MPIAnalysis::
            MPI_EDGE_LOCAL_REMOTE );
        }

        /* round 2: send reverse direction. myself == recv */

        MPI_CHECK( MPI_Sendrecv( sendBuffer, BUFFER_SIZE,
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankRecv, 0,
                                 recvBuffer, BUFFER_SIZE,
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankSend, 0,
                                 MPI_COMM_WORLD, &status ) );

        otherStartTime = recvBuffer[0];         /* recvBfr64[0]; */
        otherEnterId = recvBuffer[2];
        otherLeaveId = recvBuffer[3];

        /* compute wait states and edges */
        if ( myStartTime < otherStartTime )
        {
          Edge* recvRecordEdge = analysis->getEdge( sendRecv.first,
                                                    sendRecv.second );
          recvRecordEdge->makeBlocking( );
          sendRecv.first->incCounter( analysis->getCtrTable( ).getCtrId(
                                        CTR_WAITSTATE ),
                                      otherStartTime - myStartTime );
        }

        if ( myStartTime > otherStartTime )
        {
          distributeBlame( analysis, sendRecv.first,
                           myStartTime - otherStartTime, streamWalkCallback );

          analysis->getMPIAnalysis( ).addRemoteMPIEdge(
            sendRecv.second,
            otherEnterId,
            partnerProcessIdSend,
            MPIAnalysis::
            MPI_EDGE_REMOTE_LOCAL );
        }

        return true;
      }
  };

 }
}
