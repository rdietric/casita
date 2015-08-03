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

void
AnalysisParadigmMPI::handlePostLeave( GraphNode* node )
{
  EventStream* stream = commonAnalysis->getStream(
    node->getStreamId( ) );
  
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

  // handle blocking MPI communication enter/leave events
  EventStream::MPICommRecordList mpiCommRecords =
    stream->getPendingMPIRecords( );
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
        tmpId  = new uint64_t;
        *tmpId = iter->rootId;
        node->setData( tmpId ); // TODO: list object is already allocated, use it?
        break;

      case EventStream::MPI_SEND:
        node->setReferencedStreamId ( iter->partnerId );
        break;

      default: throw RTException( "Not a valid MPICommRecord type here" );
    }
  }
}

/**
 * Analysis rules for non-blocking MPI communication.
 * 
 * Add the request of an unfinished non-blocking MPI operation to a list of requests.
 * 
 * @param request MPI request of replayed non-blocking communication
 */
void
AnalysisParadigmMPI::addPendingMPIRequest( uint64_t requestId, MPI_Request request )
{
  //std::cerr << "[" << mpiRank << "] addPendingMPIRequest " << requestId << std::endl;
  //pendingMPIRequests[requestId] = request;
}

void
AnalysisParadigmMPI::addPendingMPIRequestId( uint64_t requestId, std::pair< MPI_Request, MPI_Request > requests )
{
  pendingMPIRequests[requestId] = requests;
}

/**
 * Analysis rules for non-blocking MPI communication:
 * 
 * Wait for open MPI_Request handles. Should be called before MPI_Finalize().
 */
void
AnalysisParadigmMPI::waitForAllPendingMPIRequests( )
{
  MPIRequestMap::const_iterator it = pendingMPIRequests.begin();
  
  //std::cerr << "[" << mpiRank << "] PendingMPIRequests: " << pendingMPIRequests.size() << std::endl;
  
  for (; it != pendingMPIRequests.end( ); ++it )
  {
    MPI_Status status;
    MPI_Request request = it->second.first;

    //std::cerr << "[" << mpiRank << "] wait for request: " << request << std::endl;
    if( MPI_REQUEST_NULL != request )
      MPI_CHECK( MPI_Wait( &request, &status ) );
    
    request = it->second.second;
    
    if( MPI_REQUEST_NULL != request )
      MPI_CHECK( MPI_Wait( &request, &status ) );
  }
  
  pendingMPIRequests.clear();
}

/**
 * If the MPI request is not complete (is still in the list) we wait for it and
 * remove the handle from the list. Otherwise it might have been completed
 * before.
 * 
 * @param request MPI request for replayed non-blocking communication to be completed.
 * 
 * @return true, if the handle was found, otherwise false
 */
bool
AnalysisParadigmMPI::waitForPendingMPIRequest( uint64_t requestId )
{
    //std::cerr << "[" << mpiRank << "] waitForPendingMPIRequest - size: " << pendingMPIRequests.size();
 
    MPIRequestMap::iterator it = pendingMPIRequests.begin();
    
    for (; it != pendingMPIRequests.end( ); ++it )
    {
      if ( it->first == requestId );
      {
        //std::cerr << std::endl << " Wait for request ID " << requestId << std::endl;
        
        MPI_Status status;
        
        if( it->second.first )
        {
          MPI_CHECK( MPI_Wait( &(it->second.first), &status ) );
        }
        
        if( it->second.second )
        {
          MPI_CHECK( MPI_Wait( &(it->second.second), &status ) );
        }
        
        pendingMPIRequests.erase( it );
        
        return true;
      }
    }
    
    std::cerr << std::endl << "[" << mpiRank << "] OTF2 MPI request ID " << requestId
              << " could not be found. Has already completed?" << std::endl;
    
    return false;
}
