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

  class OneToAllRule :
    public AbstractRule
  {
    public:

      OneToAllRule( int priority ) :
        AbstractRule( "OneToAllRule", priority )
      {

      }

      bool
      apply( AnalysisEngine* analysis, GraphNode* node )
      {
        /* applied at MPI OneToAll leave */
        if ( !node->isMPIOneToAll( ) || !node->isLeave( ) )
        {
          return false;
        }

        /* get the complete execution */
        GraphNode::GraphNodePair oneToAll = node->getGraphPair( );
        uint32_t mpiGroupId = node->getReferencedStreamId( );
        uint64_t* root = (uint64_t*)( oneToAll.second->getData( ) );
        if ( !root )
        {
          ErrorUtils::getInstance( ).throwFatalError(
            "Root must be known for MPI OneToAll" );
        }

        const MPIAnalysis::MPICommGroup& mpiCommGroup =
          analysis->getMPIAnalysis( ).getMPICommGroup( mpiGroupId );

        if ( mpiCommGroup.comm == MPI_COMM_SELF )
        {
          return false;
        }

        uint64_t rootId = *root;
        uint32_t rootMPIRank;
        rootMPIRank = analysis->getMPIAnalysis( ).getMPIRank( rootId,
                                                              mpiCommGroup );

        const uint32_t BUFFER_SIZE = 5;
        uint32_t recvBufferSize = 0;
        if ( node->getStreamId( ) == rootId )
        {
          recvBufferSize = mpiCommGroup.procs.size( ) * BUFFER_SIZE;
        }
        else
        {
          recvBufferSize = BUFFER_SIZE;
        }

        uint64_t sendBuffer[BUFFER_SIZE];
        uint64_t* recvBuffer = new uint64_t[recvBufferSize];
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t oneToAllStartTime = oneToAll.first->getTime( );
        uint64_t oneToAllEndTime = oneToAll.second->getTime( );

        sendBuffer[0] = oneToAllStartTime;
        sendBuffer[1] = oneToAllEndTime;
        sendBuffer[2] = oneToAll.first->getId( );
        sendBuffer[3] = oneToAll.second->getId( );
        sendBuffer[4] = node->getStreamId( );

        MPI_CHECK( MPI_Gather( sendBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                               recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                               rootMPIRank, mpiCommGroup.comm ) );

        if ( node->getStreamId( ) == rootId )
        {
          /* root computes its blame */
          uint64_t total_blame = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];

            if ( enterTime < oneToAllStartTime )
            {
              total_blame += oneToAllStartTime - enterTime;
            }

            analysis->getMPIAnalysis( ).addRemoteMPIEdge(
              oneToAll.first,
              recvBuffer[i + 3],
              recvBuffer[i + 4],
              MPIAnalysis::
              MPI_EDGE_LOCAL_REMOTE );
          }

          distributeBlame( analysis,
                           oneToAll.first,
                           total_blame,
                           streamWalkCallback );
        }

        MPI_Barrier( mpiCommGroup.comm );

        memcpy( recvBuffer, sendBuffer, sizeof( uint64_t ) * BUFFER_SIZE );
        MPI_CHECK( MPI_Bcast( recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                              rootMPIRank, mpiCommGroup.comm ) );

        if ( node->getStreamId( ) != rootId )
        {
          /* all others compute their wait states and create
           * dependency edges */
          uint64_t rootEnterTime = recvBuffer[0];

          if ( rootEnterTime > oneToAllStartTime )
          {
            Edge* oneToAllRecordEdge = analysis->getEdge(
              oneToAll.first, oneToAll.second );
            oneToAllRecordEdge->makeBlocking( );
            oneToAll.first->setCounter(
              analysis->getCtrTable( ).getCtrId( CTR_WAITSTATE ),
              rootEnterTime - oneToAllStartTime );
          }

          analysis->getMPIAnalysis( ).addRemoteMPIEdge(
            oneToAll.second,
            recvBuffer[2],
            recvBuffer[4],
            MPIAnalysis::
            MPI_EDGE_REMOTE_LOCAL );
        }

        delete[]recvBuffer;

        return true;
      }
  };
 }
}
