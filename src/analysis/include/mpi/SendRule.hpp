/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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

  class SendRule :
    public IMPIRule
  {
    public:

      SendRule( int priority ) :
        IMPIRule( "SendRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* sendLeave )
      {
        // applied at MPI_Send leave
        if ( !sendLeave->isMPISend() || !sendLeave->isLeave() )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon();
        MPIAnalysis& mpiAnalysis = commonAnalysis->getMPIAnalysis();

        GraphNode* sendEnter = sendLeave->getGraphPair().first;

        // get communication partner rank in the communicator
        int partnerRank = (int) sendLeave->getReferencedStreamId();
        
        //uint32_t partnerMPIRank  = mpiAnalysis.getMPIRank( partnerStreamId );
        
        uint32_t* data        = (uint32_t*)( sendLeave->getData() );
        uint32_t mpiTag       = data[ 0 ];
        uint32_t comRef       = data[1];
        MPI_Comm communicator = mpiAnalysis.getMPICommGroup( comRef ).comm;
        
        // delete data field allocated in class AnalysisParadigmMPI
        delete[] data;
        
        // send
        uint64_t sendStartTime = sendEnter->getTime();
        uint64_t sendEndTime   = sendLeave->getTime();

        // replay MPI_Send to transfer information on this MPI_Send activity
        uint64_t buffer[CASITA_MPI_P2P_BUF_SIZE];
        
        buffer[0] = sendStartTime;
        buffer[1] = sendEndTime;
        buffer[2] = sendEnter->getId();
        buffer[3] = sendLeave->getId();
        buffer[CASITA_MPI_P2P_BUF_LAST] = MPI_SEND;
        MPI_CHECK( MPI_Send( buffer, 
                             CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE,
                             partnerRank,
                             mpiTag, //CASITA_MPI_REPLAY_TAG, 
                             communicator ) );
        
        // receive the communication partner start time to compute wait states
        // use another tag to not mix up with replayed communication
        MPI_CHECK( MPI_Recv( buffer, CASITA_MPI_P2P_BUF_SIZE, 
                             CASITA_MPI_P2P_ELEMENT_TYPE, partnerRank,
                             mpiTag + CASITA_MPI_REVERS_REPLAY_TAG, 
                             communicator, //MPI_COMM_WORLD, 
                             MPI_STATUS_IGNORE ) );
        
        /*if( ( buffer[CASITA_MPI_P2P_BUF_LAST] & MPI_SEND ) &&
            ( buffer[CASITA_MPI_P2P_BUF_LAST] & MPI_RECV ) )
        {
          UTILS_WARNING( "[%"PRIu64"] SendRule: Partner rank %"PRIu32" is"
                         " MPI_SENDRECV (%"PRIu64") at %s", 
                         sendLeave->getStreamId(),
                         partnerRank, buffer[CASITA_MPI_P2P_BUF_LAST],
                         sendLeave->getUniqueName().c_str() );
        }*/
        
        // the communication partner should be a receive!!!
        if ( !( buffer[CASITA_MPI_P2P_BUF_LAST] & MPI_RECV || 
                buffer[CASITA_MPI_P2P_BUF_LAST] & MPI_IRECV ) )
        {
          UTILS_WARNING( "[%"PRIu64"] MPI_Send rule: Partner rank %"PRIu32" is "
                         "not MPI_[I]RECV (%"PRIu64"), tag: %u", sendLeave->getStreamId(),
                         partnerRank, buffer[CASITA_MPI_P2P_BUF_LAST], mpiTag );
          
          return false;
        }
        
        uint64_t recvStartTime = buffer[0];
        
        // detect wait state or distribute blame
        if ( ( sendStartTime <= recvStartTime ) )
        {
          // early sender/late receiver AND send overlaps with receive
          if ( sendStartTime < recvStartTime && sendEndTime > recvStartTime )
          {
            Edge* sendRecordEdge = commonAnalysis->getEdge( sendEnter,
                                                            sendLeave );
            if ( sendRecordEdge )
            {
              sendRecordEdge->makeBlocking();
              
              uint64_t partnerStreamId = 
                mpiAnalysis.getStreamId( partnerRank, comRef );
              
              // add remote edge for critical path analysis
              commonAnalysis->getMPIAnalysis().addRemoteMPIEdge(
                sendLeave,
                buffer[ 3 ], // remote node ID (receive leave)
                partnerStreamId );
            }
            else
            {
              UTILS_MSG( true, "[%"PRIu64"] SendRule: Activity edge not found.", 
                               sendLeave->getStreamId() );
            }
            
            // add waiting time to statistics
            commonAnalysis->getStatistics().addStatWithCount( 
              MPI_STAT_LATE_RECEIVER, recvStartTime - sendStartTime  );
            
            sendLeave->setCounter( WAITING_TIME, recvStartTime - sendStartTime );
          }
        }
        else // late sender (sendStartTime > recvStartTime)
        {
          uint64_t recvEndTime = buffer[ 1 ];
          
          // late sender AND send overlaps with receive
          if( sendStartTime < recvEndTime )
          {
            distributeBlame( commonAnalysis, sendEnter,
                             sendStartTime - recvStartTime, 
                             streamWalkCallback );
            //UTILS_WARNING( "Send blame: %lf", 
            //               commonAnalysis->getRealTime( sendStartTime - recvStartTime ) );
          }
        }

        return true;
      }
  };
 }
}
