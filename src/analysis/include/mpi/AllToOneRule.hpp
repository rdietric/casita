/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
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

      AllToOneRule( int priority ) :
        IMPIRule( "AllToOneRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI AllToOne leave */
        if ( !node->isMPIAllToOne( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis    = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair allToOne = node->getGraphPair( );
        /* \todo CHECK THIS!!! */
        uint32_t  mpiGroupId =
          (uint32_t)node->getReferencedStreamId( );
        uint64_t* root       =
          (uint64_t*)( allToOne.second->getData( ) );
        if ( !root )
        {
          ErrorUtils::getInstance( ).throwFatalError(
            "Root must be known for MPI AllToOne" );
        }

        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          commonAnalysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );

        uint64_t rootId            = *root;
        uint32_t rootMPIRank       =
          commonAnalysis->getMPIAnalysis( ).getMPIRank(
            rootId,
            mpiCommGroup );

        /* Exchange data for all matching activities with everyone */
        const uint32_t BUFFER_SIZE = 5;
        uint32_t recvBufferSize    = 0;
        recvBufferSize = mpiCommGroup.procs.size( ) * BUFFER_SIZE;

        uint64_t sendBuffer[BUFFER_SIZE];
        uint64_t*      recvBuffer  = new uint64_t[recvBufferSize];
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t allToOneStartTime = allToOne.first->getTime( );
        uint64_t allToOneEndTime   = allToOne.second->getTime( );

        sendBuffer[0]  = allToOneStartTime;
        sendBuffer[1]  = allToOneEndTime;
        sendBuffer[2]  = allToOne.first->getId( );
        sendBuffer[3]  = allToOne.second->getId( );
        sendBuffer[4]  = node->getStreamId( );

        MPI_CHECK( MPI_Allgather( sendBuffer, BUFFER_SIZE,
                                  MPI_UNSIGNED_LONG_LONG,
                                  recvBuffer, BUFFER_SIZE,
                                  MPI_UNSIGNED_LONG_LONG, mpiCommGroup.comm ) );

        /* get last enter event for collective */
        uint64_t lastEnterTime      = 0;
        uint64_t lastEnterProcessId = 0;
        for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
        {
          uint64_t enterTime = recvBuffer[i];
          if ( enterTime > lastEnterTime )
          {
            lastEnterTime      = enterTime;
            lastEnterProcessId = recvBuffer[i + 4];
          }
        }

        // root computes total waiting time and creates dependency edges
        if ( node->getStreamId( ) == rootId )
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
            if ( lastEnterProcessId == recvBuffer[i + 4] )
            {
              commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
                allToOne.second,
                recvBuffer[i + 2],
                recvBuffer[i + 4],
                MPIAnalysis::
                MPI_EDGE_REMOTE_LOCAL );
            }
          }

          /* make edge blocking if root is a wait state */
          if ( total_waiting_time )
          {
            Edge* allToOneRecordEdge = commonAnalysis->getEdge(
              allToOne.first, allToOne.second );
            
            if ( allToOneRecordEdge )
            {
              allToOneRecordEdge->makeBlocking( );
            }
            else
            {
              std::cerr << "[" << node->getStreamId( ) 
                        << "] OneToAllRule: Record edge not found. CPA might fail!" 
                        << std::endl;
            }
            
            //\todo: write counter to enter event
            allToOne.second->setCounter( WAITING_TIME, total_waiting_time );
          }
        }

        MPI_Barrier( mpiCommGroup.comm );

        if ( node->getStreamId( ) != rootId )
        {
          /* all others compute their blame */
          uint64_t rootEnterTime = recvBuffer[rootMPIRank];

          if ( rootEnterTime < allToOneStartTime )
          {
            distributeBlame( commonAnalysis, allToOne.first,
                             allToOneStartTime - rootEnterTime,
                             streamWalkCallback );
          }

          if ( lastEnterProcessId == node->getStreamId( ) )
          {
            commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
              allToOne.first,
              recvBuffer[rootMPIRank + 3],
              recvBuffer[rootMPIRank + 4],
              MPIAnalysis::
              MPI_EDGE_LOCAL_REMOTE );
          }
        }

        delete[]recvBuffer;

        return true;
      }
  };
 }
}
