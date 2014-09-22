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

#define __STDC_LIMIT_MACROS

#include <stdlib.h>
#include <string.h>

#include "common.hpp"
#include "Parser.hpp"
#include "Runner.hpp"

using namespace casita;
using namespace casita::io;

void
computeCriticalPaths( Runner* runner, uint32_t mpiRank )
{
  runner->getCriticalPath( );
}

int
main( int argc, char** argv )
{
  int mpiRank = 0;
  int mpiSize = 0;

  MPI_CHECK( MPI_Init( &argc, &argv ) );

  MPI_CHECK( MPI_Comm_rank( MPI_COMM_WORLD, &mpiRank ) );
  MPI_CHECK( MPI_Comm_size( MPI_COMM_WORLD, &mpiSize ) );

  if ( mpiRank == 0 )
  {
    printf( "[0] Running with %d analysis processes\n", mpiSize );
  }

  if ( !Parser::getInstance( ).init( argc, argv ) )
  {
    return -1;
  }
  ProgramOptions& options = Parser::getInstance( ).getProgramOptions( );

  Runner* runner          = new Runner( mpiRank, mpiSize );

  runner->readOTF( );

  EventStream::SortedGraphNodeList allNodes;
  runner->getAnalysis( ).getAllNodes( allNodes );

  runner->runAnalysis( PARADIGM_CUDA, allNodes );
  runner->runAnalysis( PARADIGM_OMP, allNodes );
  runner->runAnalysis( PARADIGM_MPI, allNodes );

  MPI_Barrier( MPI_COMM_WORLD );

  if ( mpiRank == 0 )
  {
    printf( "[%u] Computing the critical path\n", mpiRank );
  }

  computeCriticalPaths( runner, mpiRank );

  /* create OTF with wait states and critical path */
  if ( options.createOTF )
  {
    MPI_Barrier( MPI_COMM_WORLD );
    if ( mpiRank == 0 )
    {
      printf( "[%u] Writing result to %s\n", mpiRank, options.outOtfFile.c_str( ) );
    }
  }

  runner->getAnalysis( ).saveParallelEventGroupToFile(
    options.outOtfFile,
    options.filename,
    false,
    options.createOTF,
    options.verbose >=
    VERBOSE_ANNOY );

  if ( options.mergeActivities )
  {

    if ( mpiRank == 0 )
    {
      printf( "[%u] Merging activity statistics... \n", mpiRank );
    }

    runner->mergeActivityGroups( );

    runner->printAllActivities( );

    MPI_Barrier( MPI_COMM_WORLD );
  }

  delete runner;

  MPI_CHECK( MPI_Finalize( ) );
  return 0;
}
