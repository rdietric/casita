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

  class AllToOneRule :
    public IMPIRule
  {
    public:

      // This rule seems to be broken!
      AllToOneRule( int priority ) :
        IMPIRule( "AllToOneRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* allToOneLeave )
      {
        // applied at MPI AllToOne leave
        if ( !allToOneLeave->isMPIAllToOne( ) || !allToOneLeave->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        // get the enter event
        GraphNode* allToOneEnter = allToOneLeave->getGraphPair( ).first;
        
        // get MPI communicator
        uint32_t  mpiGroupId = (uint32_t)allToOneLeave->getReferencedStreamId( );
        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          commonAnalysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );

        // determine root rank
        uint64_t* root = (uint64_t*)( allToOneLeave->getData( ) );
        if ( !root )
        {
          ErrorUtils::getInstance( ).throwFatalError(
            "Root must be known for MPI AllToOne" );
        }
        uint64_t rootStreamId = *root;
        uint32_t rootMPIRank  = commonAnalysis->getMPIAnalysis( ).getMPIRank(
                                               rootStreamId/*, mpiCommGroup*/ );
        bool isRoot = false;
        // we compare OTF2 location IDs / references
        if ( allToOneLeave->getStreamId( ) == rootStreamId )
        {
          isRoot = true;
        }

        // Exchange data for all matching activities with everyone
        const uint32_t BUFFER_SIZE = 4;
        uint32_t recvBufferSize    = mpiCommGroup.procs.size( ) * BUFFER_SIZE;

        uint64_t sendBuffer[BUFFER_SIZE];
        uint64_t* recvBuffer = new uint64_t[recvBufferSize];
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t allToOneStartTime = allToOneEnter->getTime( );

        sendBuffer[0] = allToOneStartTime;
        sendBuffer[1] = allToOneEnter->getId( );
        sendBuffer[2] = allToOneLeave->getId( );
        sendBuffer[3] = allToOneLeave->getStreamId( );

        MPI_CHECK( MPI_Allgather( sendBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                  recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, 
                                  mpiCommGroup.comm ) );

        // get last enter event for collective
        uint64_t lastEnterTime      = 0;
        uint64_t lastEnterProcessId = 0;
        for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
        {
          uint64_t enterTime = recvBuffer[i];
          if ( enterTime > lastEnterTime )
          {
            lastEnterTime      = enterTime;
            lastEnterProcessId = recvBuffer[i + 3];
          }
        }

        // root computes total waiting time and creates dependency edges
        if ( isRoot )
        {
          uint64_t total_waiting_time = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];

            if ( enterTime > allToOneStartTime )
            {
              total_waiting_time += enterTime - allToOneStartTime;
            }

            // last entering process is not waiting
            if ( lastEnterProcessId == recvBuffer[i + 3] )
            {
              commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
                allToOneLeave,
                recvBuffer[i + 1],
                recvBuffer[i + 3],
                MPIAnalysis::MPI_EDGE_REMOTE_LOCAL );
            }
          }

          // make edge blocking if root is a wait state
          if ( total_waiting_time )
          {
            UTILS_MSG( true, "[%u] AllToOne %s is blocking", 
                       allToOneLeave->getStreamId( ), allToOneLeave->getUniqueName().c_str() );
            
            Edge* allToOneRecordEdge = commonAnalysis->getEdge(
                                                 allToOneEnter, allToOneLeave );
            
            if ( allToOneRecordEdge )
            {
              allToOneRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << allToOneLeave->getStreamId( ) 
                        << "] OneToAllRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            allToOneLeave->setCounter( WAITING_TIME, total_waiting_time );
          }
        }
        else // non-root processes compute their blame
        {
          uint64_t rootEnterTime = recvBuffer[rootMPIRank];

          if ( rootEnterTime < allToOneStartTime )
          {
            distributeBlame( commonAnalysis, allToOneEnter,
                             allToOneStartTime - rootEnterTime,
                             streamWalkCallback );
          }

          if ( lastEnterProcessId == allToOneLeave->getStreamId( ) )
          {
            commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
              allToOneEnter,
              recvBuffer[rootMPIRank + 2],
              recvBuffer[rootMPIRank + 3],
              MPIAnalysis::MPI_EDGE_LOCAL_REMOTE );
          }
        }

        delete[]recvBuffer;

        return true;
      }
  };
 }
}
