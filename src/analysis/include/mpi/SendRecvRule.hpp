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
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI send recv leave */
        if ( !node->isMPISendRecv( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis    = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair sendRecv = node->getGraphPair( );

        uint64_t* data =
          (uint64_t*)( node->getData( ) );
        UTILS_ASSERT( data, "No data found for %s",
                      node->getUniqueName( ).c_str( ) );

        uint64_t  partnerProcessIdRecv    = node->getReferencedStreamId( );
        uint64_t  partnerProcessIdSend    = *data;

        const int BUFFER_SIZE = 8;
        uint64_t  sendBuffer[BUFFER_SIZE], recvBuffer[BUFFER_SIZE];

        uint64_t  myStartTime = sendRecv.first->getTime( );
        uint64_t  myEndTime   = sendRecv.second->getTime( );

        /* prepare send buffer */
        sendBuffer[0] = myStartTime;
        sendBuffer[1] = myEndTime;
        sendBuffer[2] = sendRecv.first->getId( );
        sendBuffer[3] = sendRecv.second->getId( );

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
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankSend, 0,
                                 recvBuffer, BUFFER_SIZE,
                                 MPI_UNSIGNED_LONG_LONG, partnerMPIRankRecv, 0,
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
            Edge* sendRecordEdge = commonAnalysis->getEdge( sendRecv.first,
                                                            sendRecv.second );
            
            if ( sendRecordEdge )
            {
              sendRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << node->getStreamId( ) 
                        << "] SendRecvRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }

            //\todo: write counter to enter node
            sendRecv.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                           CTR_WAITSTATE ),
                                         otherStartTime - myStartTime );
          }
        }
        else
        {
          distributeBlame( commonAnalysis, sendRecv.first,
                           myStartTime - otherStartTime, streamWalkCallback );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
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

        otherStartTime = recvBuffer[0]; 
        otherEnterId   = recvBuffer[2];
        otherLeaveId   = recvBuffer[3];

        /* compute wait states and edges */
        if ( myStartTime < otherStartTime )
        {
          Edge* recvRecordEdge = commonAnalysis->getEdge( sendRecv.first,
                                                          sendRecv.second );
          if ( recvRecordEdge )
          {
            recvRecordEdge->makeBlocking( );
          }
          else
          {
            std::cerr << "[" << node->getStreamId( ) 
                      << "] SendRecvRule: Record edge not found. CPA might fail!" 
                      << std::endl;
          }
          
          //\todo: write counter to enter node
          sendRecv.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                         CTR_WAITSTATE ),
                                       otherStartTime - myStartTime );
        }

        if ( myStartTime > otherStartTime )
        {
          distributeBlame( commonAnalysis, sendRecv.first,
                           myStartTime - otherStartTime, streamWalkCallback );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
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
