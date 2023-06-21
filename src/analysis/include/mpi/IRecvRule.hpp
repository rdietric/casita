/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016, 2018-2019,
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

  /** What this rule does:
   *  1) Forward replay: MPI_Irecv
   *  2) Backward replay: MPI_Isend
   */
  class IRecvRule :
    public IMPIRule
  {
    public:

      IRecvRule( int priority ) :
        IMPIRule( "IRecvRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* irecvLeave )
      {
        // applied at MPI_Irecv leave
        if ( !irecvLeave->isMPI_Irecv() || irecvLeave->isEnter() )
        {
          return false;
        }
        
        AnalysisEngine* commonAnalysis = analysis->getAnalysisEngine();
        
        //UTILS_OUT( "[%" PRIu64 "] %s", irecvLeave->getStreamId(), commonAnalysis->getNodeInfo(irecvLeave).c_str() );

        MpiStream::MPIIcommRecord* record = 
                          (MpiStream::MPIIcommRecord* )irecvLeave->getData();
        
        // check if the record has been invalidated/deleted
        if( NULL == record )
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_TIME, 
                     "[%" PRIu64 "] MPI_Irecv rule: No record data.",
                     irecvLeave->getStreamId());
          
          return false;
        }
        
        MPIAnalysis& mpiAnalysis = commonAnalysis->getMPIAnalysis();
        MPI_Comm communicator = mpiAnalysis.getMPICommGroup( record->comRef ).comm;

        int partnerRank = (int) irecvLeave->getReferencedStreamId();

        // replay MPI_Irecv (receive buffer is never read as data are first valid in MPI_Wait[all])
        MPI_CHECK( MPI_Irecv( record->recvBuffer, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerRank, 
                              record->msgTag, //CASITA_MPI_REPLAY_TAG, 
                              communicator, //MPI_COMM_WORLD, 
                              &(record->requests[ 0 ]) ) );
        
        GraphNode* irecvEnter = irecvLeave->getGraphPair().first;
        
        // send information to communication partner
        // the blocking MPI_Recv can evaluate them and e.g. stop wait state analysis
        uint64_t *buffer_send = record->sendBuffer;
        
        // use the end time of the associated wait operation as receive end
        if(record->syncNode)
        {
          buffer_send[0] = record->syncNode->getGraphPair().first->getTime();
          buffer_send[1] = record->syncNode->getTime();
          buffer_send[2] = record->syncNode->getId();
          buffer_send[3] = record->syncNode->getId();
        }
        else
        {
          // the start and end time of MPI_Irecv are rather useless for analysis
          UTILS_WARNING("[%" PRIu64 "] MPI_Irecv rule: No wait node available!" );
          buffer_send[0] = irecvEnter->getTime();
          buffer_send[1] = irecvLeave->getTime();
          buffer_send[2] = irecvEnter->getId();
          buffer_send[3] = irecvLeave->getId();
        }
        
        buffer_send[CASITA_MPI_P2P_BUF_LAST] = MPI_IRECV;

        // Send indicator that this is an MPI_Irecv
        // use another tag to not mix up with replayed communication
        MPI_CHECK( MPI_Isend( buffer_send, 
                              CASITA_MPI_P2P_BUF_SIZE, 
                              CASITA_MPI_P2P_ELEMENT_TYPE, 
                              partnerRank,
                              record->msgTag + CASITA_MPI_REVERS_REPLAY_TAG, 
                              communicator, //MPI_COMM_WORLD, 
                              &(record->requests[ 1 ]) ) );

        // try to directly close the MPI request handles
        int finished = 0;
        MPI_Test(&(record->requests[0]), &finished, MPI_STATUS_IGNORE);
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[0] = MPI_REQUEST_NULL;
        }

        finished = 0;
        MPI_Test(&(record->requests[1]), &finished, MPI_STATUS_IGNORE);
        if(finished)
        {
          // TODO: should be done by the MPI implementation
          record->requests[1] = MPI_REQUEST_NULL;
        }

        return true;
      }
  };
 }
}
