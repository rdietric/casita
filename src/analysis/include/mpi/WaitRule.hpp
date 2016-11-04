/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2015-2016,
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

  class WaitRule :
    public IMPIRule
  {
    public:

      WaitRule( int priority ) :
        IMPIRule( "WaitRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* mpiAnalysis, GraphNode* waitLeave )
      {
        // applied at MPI_Wait leave
        if ( !waitLeave->isMPIWait( ) || waitLeave->isEnter() )
        {
          return false;
        }

        if( waitLeave->getData() )
        {
          EventStream::MPIIcommRecord* record = 
                          (EventStream::MPIIcommRecord* ) waitLeave->getData(); 
          
          AnalysisEngine* analysis = mpiAnalysis->getCommon( );
          
          if( !record->leaveNode )
          {
            return false;
          }
          
          // wait for MPI_Irecv or MPI_Isend
          if( !(record->leaveNode->isMPI_Irecv() || record->leaveNode->isMPI_Isend()) )
          {
            UTILS_MSG( true, "[%"PRIu64"] WaitRule: Neither waiting for receive"
                       " nor send! (%s)", waitLeave->getStreamId(), 
                       record->leaveNode->getUniqueName().c_str() );
            return false;
          }
            
          // to evaluate the receive buffer, we need to ensure the transfer has finished
          if( record->requests[0] != MPI_REQUEST_NULL )
          {
            MPI_CHECK( MPI_Wait( &(record->requests[0]), MPI_STATUS_IGNORE ) );
          }
            
          // get start time of send operation
          uint64_t p2pPartnerStartTime = record->recvBuffer[0];
          GraphNode* waitEnter   = waitLeave->getGraphPair().first;
          uint64_t waitStartTime = waitEnter->getTime();

          // if this wait started before the communication partner operation,
          // we found a late sender or receiver
          if( waitStartTime < p2pPartnerStartTime )
          {
            //UTILS_MSG( true, "[%"PRIu64"] WaitRule: Found late sender/receiver", 
            //           waitLeave->getStreamId() );

            // mark this leave as a wait state
            Edge* waitEdge = analysis->getEdge(waitEnter, waitLeave);

            if ( waitEdge )
            {
              waitEdge->makeBlocking();

              // referenced stream is needed in critical path analysis
              waitLeave->setReferencedStreamId( record->leaveNode->getReferencedStreamId() );

              // add remote edge for critical path analysis
              analysis->getMPIAnalysis().addRemoteMPIEdge(
                waitLeave,
                record->recvBuffer[2], // remote node ID (leave event)
                record->leaveNode->getReferencedStreamId()/*, // remote process ID
                MPIAnalysis::MPI_EDGE_REMOTE_LOCAL*/ );
            }
            else
            {
              UTILS_MSG( true, "[%"PRIu64"] MPI_Wait rule: Activity edge not found.", 
                               waitLeave->getStreamId() );
            }

            waitLeave->setCounter( WAITING_TIME, 
                                  p2pPartnerStartTime - waitEnter->getTime());
          }

          /* timing information on the other wait operation was necessary for 
           * blame distribution
          distributeBlame( analysis,
                           waitEnter,
                           waitStartTime - p2pPartnerStartTime,
                           streamWalkCallback );*/

          // also wait for the other MPI_Request associated with the send buffer
          if( record->requests[1] != MPI_REQUEST_NULL )
          {
            MPI_CHECK( MPI_Wait( &(record->requests[1]), MPI_STATUS_IGNORE ) );
          }
          
          // remove the pending MPI request
          analysis->getStream( waitLeave->getStreamId() )
                  ->removePendingMPIRequest( record->requestId );
        }
        else
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                     "[%" PRIu64 "] MPI_Wait rule: No request to wait for!", 
                     waitLeave->getStreamId() );
        }

        return true;
      }
  };
 }
}
