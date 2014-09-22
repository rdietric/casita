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

        const uint32_t BUFFER_SIZE = 7;
        uint32_t recvBufferSize    = 0;
        if ( node->getStreamId( ) == rootId )
        {
          recvBufferSize = mpiCommGroup.procs.size( ) * BUFFER_SIZE;
        }
        else
        {
          recvBufferSize = BUFFER_SIZE;
        }

        uint64_t  sendBuffer[BUFFER_SIZE];
        uint64_t* recvBuffer        = new uint64_t[recvBufferSize];
        memset( recvBuffer, 0, recvBufferSize * sizeof( uint64_t ) );

        uint64_t  allToOneStartTime = allToOne.first->getTime( );
        uint64_t  allToOneEndTime   = allToOne.second->getTime( );

        /* memcpy(sendBuffer, &allToOneStartTime, sizeof (uint64_t));
         **/
        /* memcpy(sendBuffer + 2, &allToOneEndTime, sizeof
         * (uint64_t)); */
        sendBuffer[0] = allToOneStartTime;
        sendBuffer[1] = allToOneEndTime;
        sendBuffer[2] = allToOne.first->getId( );
        sendBuffer[3] = allToOne.second->getId( );
        sendBuffer[4] = node->getStreamId( );

        MPI_CHECK( MPI_Gather( sendBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                               recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                               rootMPIRank, mpiCommGroup.comm ) );

        if ( node->getStreamId( ) == rootId )
        {
          /* root computes its waiting time and creates dependency
           * edges */
          uint64_t total_waiting_time = 0;
          for ( size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE )
          {
            uint64_t enterTime = recvBuffer[i];

            if ( enterTime > allToOneStartTime )
            {
              total_waiting_time += enterTime - allToOneStartTime;
            }

            commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
              allToOne.second,
              recvBuffer[i + 2],
              recvBuffer[i + 4],
              MPIAnalysis::
              MPI_EDGE_REMOTE_LOCAL );
          }

          if ( total_waiting_time )
          {
            Edge* allToOneRecordEdge = commonAnalysis->getEdge(
              allToOne.first, allToOne.second );
            allToOneRecordEdge->makeBlocking( );
            allToOne.first->setCounter(
              commonAnalysis->getCtrTable( ).getCtrId( CTR_WAITSTATE ),
              total_waiting_time );
          }
        }

        MPI_Barrier( mpiCommGroup.comm );

        memcpy( recvBuffer, sendBuffer, sizeof( uint64_t ) * BUFFER_SIZE );
        MPI_CHECK( MPI_Bcast( recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                              rootMPIRank, mpiCommGroup.comm ) );

        if ( node->getStreamId( ) != rootId )
        {
          /* all others compute their blame */
          uint64_t rootEnterTime = recvBuffer[0];

          if ( rootEnterTime < allToOneStartTime )
          {
            distributeBlame( commonAnalysis, allToOne.first,
                             allToOneStartTime - rootEnterTime,
                             streamWalkCallback );
          }

          commonAnalysis->getMPIAnalysis( ).addRemoteMPIEdge(
            allToOne.first,
            recvBuffer[3],
            recvBuffer[4],
            MPIAnalysis::
            MPI_EDGE_LOCAL_REMOTE );
        }

        delete[]recvBuffer;

        return true;
      }
  };
 }
}
