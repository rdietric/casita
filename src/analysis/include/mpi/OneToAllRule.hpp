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

  class OneToAllRule :
    public IMPIRule
  {
    public:

      OneToAllRule( int priority ) :
        IMPIRule( "OneToAllRule", priority )
      {

      }

    private:

      /**
       * This rule might be broken as well in concerns of critical path detection
       * and blame distribution. (Use collective rule instead.)
       * @param analysis
       * @param oneToAllLeave
       * @return 
       */
      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* oneToAllLeave )
      {
        // applied at MPI OneToAll leave
        if ( !oneToAllLeave->isMPIOneToAll( ) || !oneToAllLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        // get the complete execution
        GraphNode* oneToAllEnter = oneToAllLeave->getGraphPair().first;
        uint32_t  mpiGroupId = oneToAllLeave->getReferencedStreamId();
        uint64_t* root = (uint64_t*)( oneToAllLeave->getData() );
        if ( !root )
        {
          ErrorUtils::getInstance().throwFatalError(
            "Root must be known for MPI OneToAll" );
        }

        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          commonAnalysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );

        if ( mpiCommGroup.comm == MPI_COMM_SELF )
        {
          return false;
        }

        uint64_t rootStreamId = *root;
        
        // \TODO: used in CPA?
        // delete root;
        
        // this translation seems wrong
        uint32_t rootMPIRank = commonAnalysis->getMPIAnalysis().getMPIRank(
                                               rootStreamId );
        
        bool isRoot = false;
        // we compare OTF2 location IDs / references
        if ( oneToAllLeave->getStreamId() == rootStreamId )
        {
          isRoot = true;
        }

        const uint32_t BUFFER_SIZE = 4;
        uint32_t recvBufferSize    = BUFFER_SIZE;

        uint64_t  sendBuffer[BUFFER_SIZE];
        
        // allocate receive buffer depending on the communication group size
        uint64_t* recvBuffer = NULL;
        if( isRoot )
        {
          recvBufferSize = mpiCommGroup.procs.size() * BUFFER_SIZE;
          
          recvBuffer = new uint64_t[recvBufferSize];
          
          // zero the receive buffer
          memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );
        }
 
        uint64_t oneToAllStartTime = oneToAllEnter->getTime();
        sendBuffer[0] = oneToAllStartTime;
        sendBuffer[1] = oneToAllEnter->getId();
        sendBuffer[2] = oneToAllLeave->getId();
        sendBuffer[3] = oneToAllLeave->getStreamId();

        MPI_CHECK( MPI_Gather( sendBuffer, BUFFER_SIZE, MPI_UINT64_T,
                               recvBuffer, BUFFER_SIZE, MPI_UINT64_T,
                               rootMPIRank, mpiCommGroup.comm ) );

        // the root has gathered all information now
        if ( isRoot )
        {
          // root computes its blame
          uint64_t total_blame = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];

            if ( enterTime < oneToAllStartTime )
            {
              total_blame += oneToAllStartTime - enterTime;
            }

            commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
              oneToAllEnter,
              recvBuffer[i + 2],
              recvBuffer[i + 3]/*,
              MPIAnalysis::MPI_EDGE_LOCAL_REMOTE*/ );
          }

          distributeBlame( commonAnalysis,
                           oneToAllEnter,
                           total_blame,
                           streamWalkCallback,
                           REASON_MPI_COLLECTIVE );
          
          delete[] recvBuffer;
        }
        
        // use the fixed-size send buffer for each rank
        MPI_CHECK( MPI_Bcast( sendBuffer, BUFFER_SIZE, MPI_UINT64_T,
                              rootMPIRank, mpiCommGroup.comm ) );

        // non-root ranks compute their wait states and create dependency edges
        if ( !isRoot )
        {
          uint64_t rootEnterTime = sendBuffer[0];

          if ( rootEnterTime > oneToAllStartTime )
          {
            Edge* oneToAllRecordEdge = commonAnalysis->getEdge(
              oneToAllEnter, oneToAllLeave );
          
            if ( oneToAllRecordEdge )
            {
              oneToAllRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << oneToAllLeave->getStreamId( ) 
                        << "] OneToAllRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            oneToAllLeave->setCounter( WAITING_TIME,
                                       rootEnterTime - oneToAllStartTime );
          }

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            oneToAllLeave,
            sendBuffer[1],
            sendBuffer[3]/*,
            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL*/ );
        }

        return true;
      }
  };
 }
}
