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
 */

#include "mpi/AnalysisParadigmMPI.hpp"
#include "AnalysisEngine.hpp"

#include "mpi/RecvRule.hpp"
#include "mpi/SendRule.hpp"
#include "mpi/CollectiveRule.hpp"
#include "mpi/SendRecvRule.hpp"
#include "mpi/OneToAllRule.hpp"
#include "mpi/AllToOneRule.hpp"

using namespace casita;
using namespace casita::mpi;

AnalysisParadigmMPI::AnalysisParadigmMPI( AnalysisEngine* analysisEngine,
                                          uint32_t        mpiRank,
                                          uint32_t        mpiSize ) :
  IAnalysisParadigm( analysisEngine )
{
  addRule( new RecvRule( 1 ) );
  addRule( new SendRule( 1 ) );
  addRule( new CollectiveRule( 1 ) );
  addRule( new SendRecvRule( 1 ) );
  addRule( new OneToAllRule( 1 ) );
  addRule( new AllToOneRule( 1 ) );
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
        node->setData( tmpId );
        break;

      case EventStream::MPI_SEND:
        tmpId  = new uint64_t;
        *tmpId = iter->partnerId;
        node->setData( tmpId );
        break;

      default:
        throw RTException( "Not a valid MPICommRecord type here" );
    }
  }
}
