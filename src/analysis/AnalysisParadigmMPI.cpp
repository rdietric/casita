/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * Add MPI specific rules.
 *
 */

#include "mpi/AnalysisParadigmMPI.hpp"
#include "AnalysisEngine.hpp"

#include "mpi/RecvRule.hpp"
#include "mpi/SendRule.hpp"
#include "mpi/CollectiveRule.hpp"
#include "mpi/SendRecvRule.hpp"
#include "mpi/OneToAllRule.hpp"
#include "mpi/AllToOneRule.hpp"
#include "mpi/IRecvRule.hpp"
#include "mpi/ISendRule.hpp"
#include "mpi/WaitAllRule.hpp"
#include "mpi/WaitRule.hpp"

using namespace casita;
using namespace casita::mpi;

AnalysisParadigmMPI::AnalysisParadigmMPI( AnalysisEngine* analysisEngine,
                                          uint32_t        mpiRank,
                                          uint32_t        mpiSize ) :
  IAnalysisParadigm( analysisEngine ),
  //pendingMPIRequests( NULL ),
  mpiRank( mpiRank ),
  mpiSize( mpiSize )
{
  addRule( new RecvRule( 1 ) );
  addRule( new SendRule( 1 ) );
  addRule( new CollectiveRule( 1 ) );
  addRule( new SendRecvRule( 1 ) );
  addRule( new OneToAllRule( 1 ) );
  addRule( new AllToOneRule( 1 ) );
  addRule( new IRecvRule( 1 ) );
  addRule( new ISendRule( 1 ) );
  addRule( new WaitAllRule( 1 ) );
  addRule( new WaitRule( 1 ) );
}

AnalysisParadigmMPI::~AnalysisParadigmMPI( )
{

}

Paradigm
AnalysisParadigmMPI::getParadigm( )
{
  return PARADIGM_MPI;
}

//void
//AnalysisParadigmMPI::handlePostEnter( GraphNode* node )
//{
//  
//}

void
AnalysisParadigmMPI::handlePostLeave( GraphNode* node )
{
  EventStream* stream = commonAnalysis->getStream( node->getStreamId( ) );
  
  // handle non-blocking MPI communication enter/leave events
  
  if( node->isMPIISend() )
  {
    stream->setMPIIsendNodeData( node );
    
    return;
  }
  
  if( node->isMPIIRecv() )
  {
    stream->addPendingMPIIrecvNode( node );
    
    return;
  }
  
  if( node->isMPIWait() )
  {
    stream->setMPIWaitNodeData( node );
    
    return;
  }
  
  // MPI_Waitall waits for an arbitrary number of requests
//  if( node->isMPIWaitall() )
//  {
//    stream->setMPIWaitallNodeData( node );
//    
//    return;
//  }

  // handle blocking MPI communication events
  EventStream::MPICommRecordList mpiCommRecords = stream->getPendingMPIRecords( );
  for ( EventStream::MPICommRecordList::const_iterator iter =
          mpiCommRecords.begin( );
        iter != mpiCommRecords.end( ); ++iter )
  {
    uint64_t* tmpId = NULL;

    switch ( iter->mpiType )
    {
      case EventStream::MPI_RECV:
        node->setReferencedStreamId( iter->partnerId );
        break;

      case EventStream::MPI_COLLECTIVE:
        node->setReferencedStreamId( iter->partnerId );
        break;

      case EventStream::MPI_ONEANDALL:
        node->setReferencedStreamId( iter->partnerId );
        tmpId  = new uint64_t; // TODO: free
        *tmpId = iter->rootId;
        node->setData( tmpId ); 
        break;

      case EventStream::MPI_SEND:
        tmpId  = new uint64_t; // TODO: free
        *tmpId = iter->partnerId;
        node->setData( tmpId );
        break;
        
      // MPI_Sendrecv collects the MPI_Send and MPI_Recv partner IDs from the
      // respective pending records

      default: throw RTException( "Not a valid MPICommRecord type here" );
    }
  }
}
