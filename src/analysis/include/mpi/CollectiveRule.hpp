/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016, 2018
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

        /**
         * This rule can be applied for all kind of MPI collectives, e.g. Barrier,
         * Allreduce, Allgather, etc.. It can also be used instead of oneToAll and
         * allToOne rules.
         *
         * @param priority
         */
        CollectiveRule( int priority ) :
          IMPIRule( "CollectiveRule", priority )
        {

        }

      private:

        bool
        apply( AnalysisParadigmMPI* mAnalysis, GraphNode* colLeave )
        {
          /* applied at MPI collective leave */
          if ( !colLeave->isMPICollective( ) || !colLeave->isLeave( ) )
          {
            return false;
          }

          AnalysisEngine* analysis = mAnalysis->getAnalysisEngine( );

          /* count occurrences */
          analysis->getStatistics( ).countActivity( STAT_MPI_COLLECTIVE );

          /* test for pending non-blocking MPI communication (to close open requests) */
          if ( !colLeave->isMPIInit( ) )
          {
            analysis->getStreamGroup( ).getMpiStream( colLeave->getStreamId( ) )->testAllPendingMPIRequests( );
          }

          GraphNode* colEnter        = colLeave->getGraphPair( ).first;

          uint32_t   mpiGroupId      = colLeave->getReferencedStreamId( );
          const MPIAnalysis::MPICommGroup& mpiCommGroup =
              analysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );

          if ( mpiCommGroup.comm == MPI_COMM_SELF )
          {
            return false;
          }

          /* Data about each collective is exchanged with everyone */
          const uint32_t BUFFER_SIZE = 4;
          uint64_t  sendBuffer[BUFFER_SIZE];

          /* receive buffer has to be dynamically allocated as it depends on the */
          /* number of processes in this group */
          uint32_t  recvBufferSize   = mpiCommGroup.procs.size( ) * BUFFER_SIZE;
          uint64_t* recvBuffer       = new uint64_t[recvBufferSize];

          UTILS_ASSERT( recvBuffer != NULL, "Could not allocate uint64_t[]!\n" );

          memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

          uint64_t collStartTime     = colEnter->getTime( );

          /* prepare send buffer */
          sendBuffer[0] = collStartTime;
          sendBuffer[1] = colEnter->getId( );
          sendBuffer[2] = colLeave->getId( );
          sendBuffer[3] = colLeave->getStreamId( );

          /*UTILS_WARNING( "[%" PRIu64 "] MPI collective rule for %s (group %u)",
                         colLeave->getStreamId(),
                         colLeave->getUniqueName().c_str(),
                         mpiGroupId );*/

          /* \todo: replace with a custom MPI_Allreduce */
          MPI_CHECK( MPI_Allgather( sendBuffer, BUFFER_SIZE, MPI_UINT64_T,
              recvBuffer, BUFFER_SIZE, MPI_UINT64_T,
              mpiCommGroup.comm ) );

          /* get last enter event for collective */
          uint64_t lastEnterTime         = 0;
          uint64_t lastEnterProcessId    = 0;
          /* uint64_t lastLeaveRemoteNodeId = 0; */
          uint64_t lastEnterRemoteNodeId = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];

            if ( enterTime > lastEnterTime )
            {
              lastEnterTime         = enterTime;
              lastEnterRemoteNodeId = recvBuffer[i + 1];
              /* lastLeaveRemoteNodeId = recvBuffer[ i + 2 ]; */
              lastEnterProcessId    = recvBuffer[i + 3];
            }
          }

          /* this is not the last -> blocking + remoteEdge to lastEnter */
          if ( lastEnterProcessId != colLeave->getStreamId( ) ) /* collStartTime < lastEnterTime ) */
          {
            Edge* collRecordEdge = analysis->getEdge( colEnter, colLeave );

            if ( collRecordEdge )
            {
              collRecordEdge->makeBlocking( );

              /* add remote edge as this activity is blocking (needed in CPA) */
              analysis->getMPIAnalysis( ).addRemoteMPIEdge(
                colLeave, /* local leave node */
                lastEnterRemoteNodeId, /* remote leave node ID */
                lastEnterProcessId );
            }
            else
            {
              UTILS_WARNING( "[%" PRIu64 "] MPI collective rule: Record edge not "
                                         "found. CPA might fail!", colLeave->getStreamId( ) );
            }

            analysis->getStatistics( ).addStatWithCount(
              MPI_STAT_COLLECTIVE, lastEnterTime - collStartTime );

            /* set the wait state counter for this blocking region */
            /* waiting time = last enter time - this ranks enter time */
            colLeave->setCounter( WAITING_TIME, lastEnterTime - collStartTime );
          }
          else /* this stream is entering the collective last */
          {
            /* aggregate blame from all other streams */
            uint64_t total_blame = 0;
            for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
            {
              /* blame += last enter time - this enter time */
              total_blame += lastEnterTime - recvBuffer[i];
            }

            distributeBlame( analysis,
                colEnter,
                total_blame,
                mpiStreamWalkCallback,
                REASON_MPI_COLLECTIVE );
          }

          delete[]recvBuffer;

          return true;
        }
    };
  }
}
