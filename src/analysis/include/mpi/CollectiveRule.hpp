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
      apply( AnalysisParadigmMPI* analysis, GraphNode* colLeave )
      {
        // applied at MPI collective leave
        if ( !colLeave->isMPICollective( ) || !colLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );
        
        // wait or test for pending non-blocking MPI communication
        if ( colLeave->isMPIFinalize() )
        {
          analysis->getCommon()->getStream( colLeave->getStreamId() )->waitForAllPendingMPIRequests();
        }
        else if ( !colLeave->isMPIInit() )
        {
          analysis->getCommon()->getStream( colLeave->getStreamId() )->testAllPendingMPIRequests();
        }
        /*
        UTILS_MSG( 0 == strcmp( colLeave->getName(), "MPI_Reduce" ), 
                   "[%u] MPI collective: %s", colLeave->getStreamId(), colLeave->getUniqueName().c_str());
        */
        GraphNode* colEnter = colLeave->getGraphPair( ).first;
        
        uint32_t mpiGroupId = colLeave->getReferencedStreamId( );
        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          commonAnalysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId ); 

        if ( mpiCommGroup.comm == MPI_COMM_SELF )
        {
          return false;
        }

        // Data about each collective is exchanged with everyone
        const uint32_t BUFFER_SIZE = 4;
        uint64_t sendBuffer[BUFFER_SIZE];

        // receive buffer has to be dynamically allocated as it depends on the
        // number of processes in this group
        uint32_t recvBufferSize = mpiCommGroup.procs.size( ) * BUFFER_SIZE;
        uint64_t *recvBuffer    = new uint64_t[recvBufferSize];
        
        UTILS_ASSERT( recvBuffer != NULL, "Could not allocate uint64_t[]!\n");
        
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t collStartTime = colEnter->getTime( );
        
        // prepare send buffer
        sendBuffer[0] = collStartTime;
        sendBuffer[1] = colEnter->getId( );
        sendBuffer[2] = colLeave->getId( );
        sendBuffer[3] = colLeave->getStreamId( );

        MPI_CHECK( MPI_Allgather( sendBuffer, BUFFER_SIZE, MPI_UINT64_T,
                                  recvBuffer, BUFFER_SIZE, MPI_UINT64_T, 
                                  mpiCommGroup.comm ) );

        // get last enter event for collective
        uint64_t lastEnterTime         = 0;
        uint64_t lastEnterProcessId    = 0;
        uint64_t lastEnterRemoteNodeId = 0;
        for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
        {
          uint64_t enterTime = recvBuffer[i];

          if ( enterTime > lastEnterTime )
          {
            lastEnterTime         = enterTime;
            lastEnterRemoteNodeId = recvBuffer[i + 1];
            lastEnterProcessId    = recvBuffer[i + 3];
          }
        }

        // this is not the last -> blocking + remoteEdge to lastEnter
        if ( lastEnterProcessId != colLeave->getStreamId( ) ) // collStartTime < lastEnterTime )
        {
          // These nodes/edges are needed for dependency correctness but are
          // omitted since they are currently not used anywhere.
//           analysis->getMPIAnalysis().addRemoteMPIEdge(coll.second, lastEnterRemoteNodeId, lastEnterProcessId);
//           
//           GraphNode *remoteNode = analysis->addNewRemoteNode(lastEnterTime, lastEnterProcessId,
//                   lastEnterRemoteNodeId, PARADIGM_MPI, RECORD_ENTER, MPI_COLL,
//                   analysis->getMPIAnalysis().getMPIRank(lastEnterProcessId));
//           
//           analysis->newEdge(remoteNode, coll.second);
           
          Edge* collRecordEdge = commonAnalysis->getEdge( colEnter, colLeave );
          
          if ( collRecordEdge )
          {
            collRecordEdge->makeBlocking( );
            /*UTILS_MSG( 0 == strcmp( colLeave->getName(), "MPI_Reduce" ), 
                       "[%u] make blocking", colLeave->getStreamId() );*/
          }
          else
          {
            std::cerr << "[" << colLeave->getStreamId( ) 
                      << "] CollectiveRule: Record edge not found. CPA might fail!" 
                      << std::endl;
          }
          
          // set the wait state counter for this blocking region
          // waiting time = last enter time - this ranks enter time
          colLeave->setCounter( WAITING_TIME, lastEnterTime - collStartTime );

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            colLeave, // local leave node
            lastEnterRemoteNodeId, // remote enter node
            lastEnterProcessId,
            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL );
        }
        else // this stream is entering the collective last
        {
          // aggregate blame from all other streams
          uint64_t total_blame = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];
            total_blame += lastEnterTime - enterTime;

            uint32_t myMpiRank = commonAnalysis->getMPIRank( );
            if ( recvBuffer[i + 3] != myMpiRank )
            {
              commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
                colEnter, // local enter node
                recvBuffer[i + 2], // remote leave node ID
                recvBuffer[i + 3], // remote process ID
                MPIAnalysis::MPI_EDGE_LOCAL_REMOTE );
            }

            // These nodes/edges are needed for dependency correctness but are
            // omitted since they are currently not used anywhere.
//             uint64_t leaveTime = recvBuffer[i + 1];
//             uint64_t remoteLeaveNodeId = recvBuffer[i + 3];
//             uint64_t remoteProcessId = recvBuffer[i + 4];
//             
//             GraphNode *remoteNode = analysis->addNewRemoteNode(leaveTime, remoteProcessId,
//                    remoteLeaveNodeId, PARADIGM_MPI, RECORD_LEAVE, MPI_COLL,
//                    analysis->getMPIAnalysis().getMPIRank(remoteProcessId));
//
//             analysis->newEdge(coll.first, remoteNode);

          }

          distributeBlame( commonAnalysis,
                           colEnter,
                           total_blame,
                           streamWalkCallback );
        }

        delete[] recvBuffer;

        return true;
      }
  };
 }
}
