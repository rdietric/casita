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

  class CollectiveRule :
    public IMPIRule
  {
    public:

      CollectiveRule( int priority ) :
        IMPIRule( "CollectiveRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI collective leave */
        if ( !node->isMPICollective( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair coll  = node->getGraphPair( );
        uint32_t mpiGroupId        =
          node->getReferencedStreamId( );
        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          commonAnalysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );
        uint32_t myMpiRank         =
          commonAnalysis->getMPIRank( );

        if ( mpiCommGroup.comm == MPI_COMM_SELF )
        {
          return false;
        }

        const uint32_t BUFFER_SIZE = 7;
        uint32_t recvBufferSize    = mpiCommGroup.procs.size( ) * BUFFER_SIZE;
        uint64_t sendBuffer[BUFFER_SIZE];
        uint64_t*      recvBuffer  = new uint64_t[recvBufferSize];
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t collStartTime     = coll.first->getTime( );
        uint64_t collEndTime       = coll.second->getTime( );

        /* memcpy(sendBuffer, &collStartTime, sizeof (uint64_t)); */
        /* memcpy(sendBuffer + 2, &collEndTime, sizeof (uint64_t)); */
        sendBuffer[0] = collStartTime;
        sendBuffer[1] = collEndTime;
        sendBuffer[2] = coll.first->getId( );
        sendBuffer[3] = coll.second->getId( );
        sendBuffer[4] = node->getStreamId( );

        MPI_CHECK( MPI_Allgather( sendBuffer, BUFFER_SIZE,
                                  MPI_UNSIGNED_LONG_LONG,
                                  recvBuffer, BUFFER_SIZE,
                                  MPI_UNSIGNED_LONG_LONG, mpiCommGroup.comm ) );

        /* get last enter event for collective */
        uint64_t lastEnterTime         = 0, lastLeaveTime = 0;
        uint64_t lastEnterProcessId    = 0;
        /* uint64_t lastLeaveProcessId = 0; */
        uint64_t lastEnterRemoteNodeId = 0;
        /* uint64_t lastLeaveRemoteNodeId = 0; */

        for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
        {
          uint64_t enterTime = recvBuffer[i];
          uint64_t leaveTime = recvBuffer[i + 1];

          if ( enterTime > lastEnterTime )
          {
            lastEnterTime         = enterTime;
            lastEnterRemoteNodeId = recvBuffer[i + 2];
            lastEnterProcessId    = recvBuffer[i + 4];
          }

          if ( leaveTime > lastLeaveTime )
          {
            lastLeaveTime = leaveTime;
          }

        }

        /* I'm not latest collective -> blocking + remoteEdge to
         * lastEnter */
        if ( collStartTime < lastEnterTime )
        {
          /** These nodes/edges are needed for dependency correctness but are
           *  omitted since they are currently not used anywhere.
           * analysis->getMPIAnalysis().addRemoteMPIEdge(coll.second, lastEnterRemoteNodeId, lastEnterProcessId);
           *
           * GraphNode *remoteNode = analysis->addNewRemoteNode(lastEnterTime, lastEnterProcessId,
           *         lastEnterRemoteNodeId, PARADIGM_MPI, RECORD_ENTER, MPI_COLL,
           *         analysis->getMPIAnalysis().getMPIRank(lastEnterProcessId));
           *
           * analysis->newEdge(remoteNode, coll.second);
           **/
          Edge* collRecordEdge = commonAnalysis->getEdge( coll.first,
                                                          coll.second );
          collRecordEdge->makeBlocking( );
          coll.second->setCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                   CTR_WAITSTATE ),
                                   lastEnterTime - collStartTime );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            coll.second,
            lastEnterRemoteNodeId,
            lastEnterProcessId,
            MPIAnalysis::
            MPI_EDGE_REMOTE_LOCAL );
        }
        else           /* I'm latest collective */
        {
          /* aggregate blame from all other streams */
          uint64_t total_blame = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];
            total_blame += lastEnterTime - enterTime;

            if ( i != myMpiRank )
            {
              commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
                coll.first,
                recvBuffer[i + 3],
                recvBuffer[i + 4],
                MPIAnalysis::
                MPI_EDGE_LOCAL_REMOTE );
            }

            /** These nodes/edges are needed for dependency correctness but are
             *  omitted since they are currently not used anywhere.
             * uint64_t leaveTime = recvBuffer[i + 1];
             * uint64_t remoteLeaveNodeId = recvBuffer[i + 3];
             * uint64_t remoteProcessId = recvBuffer[i + 4];
             *
             * GraphNode *remoteNode = analysis->addNewRemoteNode(leaveTime, remoteProcessId,
                    remoteLeaveNodeId, PARADIGM_MPI, RECORD_LEAVE, MPI_COLL,
                    analysis->getMPIAnalysis().getMPIRank(remoteProcessId));

             * analysis->newEdge(coll.first, remoteNode);*/

          }

          distributeBlame( commonAnalysis,
                           coll.first,
                           total_blame,
                           streamWalkCallback );
        }

        delete[]recvBuffer;

        return true;
      }
  };
 }
}