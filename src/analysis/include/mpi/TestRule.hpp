/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2019,
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

    class TestRule :
      public IMPIRule
    {
      public:

        TestRule( int priority ) :
          IMPIRule( "TestRule", priority )
        {

        }

      private:

        bool
        apply( AnalysisParadigmMPI* analysisParadigmMPI, GraphNode* testLeave )
        {
          /* applied at MPI_Test leave */
          if ( !testLeave->isMPI_Test( ) || testLeave->isEnter( ) )
          {
            return false;
          }

          if ( testLeave->getData( ) )
          {
            MpiStream::MPIIcommRecord* record =
                (MpiStream::MPIIcommRecord*)testLeave->getData( );

            AnalysisEngine* analysis          = analysisParadigmMPI->getAnalysisEngine( );

            UTILS_OUT( "[%" PRIu64 "] %s", testLeave->getStreamId( ),
                analysis->getNodeInfo( testLeave ).c_str( ) );

            analysis->getStatistics( ).countActivity( STAT_MPI_TEST );

            uint64_t streamId = testLeave->getStreamId( );

            if ( !record->msgNode )
            {
              return false;
            }

            /* test for MPI_Irecv or MPI_Isend */
            if ( !( record->msgNode->isMPI_Irecv( ) || record->msgNode->isMPI_Isend( ) ) )
            {
              UTILS_OUT( "[%" PRIu64 "] TestRule: Only MPI_Isend and MPI_Irecv"
                                     " are supported! (%s)", streamId,
                  analysis->getNodeInfo( record->msgNode ).c_str( ) );
              return false;
            }

            /* to evaluate the receive buffer, we need to ensure the transfer has finished */
            if ( record->requests[0] != MPI_REQUEST_NULL )
            {
              int success = 0;
              MPI_CHECK( MPI_Test( &( record->requests[0] ), &success, MPI_STATUS_IGNORE ) );

              /* if the operation (send/recv) is not completed, wait for it */
              if ( success )
              {
                record->requests[0] = MPI_REQUEST_NULL;
              }
              else
              {
                MPI_CHECK( MPI_Wait( &( record->requests[0] ), MPI_STATUS_IGNORE ) );
              }
            }

            /* also test for the other MPI_Request associated with the send buffer */
            if ( record->requests[1] != MPI_REQUEST_NULL )
            {
              int success = 0;
              MPI_CHECK( MPI_Test( &( record->requests[1] ), &success, MPI_STATUS_IGNORE ) );

              if ( success )
              {
                record->requests[1] = MPI_REQUEST_NULL;
              }
              else
              {
                MPI_CHECK( MPI_Wait( &( record->requests[1] ), MPI_STATUS_IGNORE ) );
              }
            }

            /* remove the pending MPI request */
            analysis->getStreamGroup( ).getMpiStream( streamId )
            ->removePendingMPIRequest( record->requestId );
          }
          else
          {
            UTILS_MSG( Parser::getVerboseLevel( ) > VERBOSE_BASIC,
                "[%" PRIu64 "] MPI_Test rule: No request to wait for!",
                testLeave->getStreamId( ) );
          }

          return true;
        }
    };
  }
}
