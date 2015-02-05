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
 * What this file does:
 * This file contains the main routine.
 * - Initialize communication
 * - Parse command line options
 * - Create runner
 * - trigger reading of trace file
 * - trigger analysis for different paradigms
 * - trigger computation of critical path
 * - trigger creation of output OTF file
 * - trigger display and calculation of summary
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
  int status  = 0;
  int mpiRank = 0;
  int mpiSize = 0;

  MPI_CHECK( MPI_Init( &argc, &argv ) );

  MPI_CHECK( MPI_Comm_rank( MPI_COMM_WORLD, &mpiRank ) );
  MPI_CHECK( MPI_Comm_size( MPI_COMM_WORLD, &mpiSize ) );

  UTILS_DBG_MSG( mpiRank == 0, "[%u] Running with %d analysis processes", mpiRank, mpiSize );

  if ( !Parser::getInstance( ).init( argc, argv ) )
  {
    return -1;
  }

  try
  {
    ProgramOptions& options = Parser::getInstance( ).getProgramOptions( );

    Runner* runner          = new Runner( mpiRank, mpiSize );

    runner->readOTF( );

    EventStream::SortedGraphNodeList allNodes;
    runner->getAnalysis( ).getAllNodes( allNodes );

    runner->runAnalysis( PARADIGM_CUDA, allNodes );
    runner->runAnalysis( PARADIGM_OMP, allNodes );
    runner->runAnalysis( PARADIGM_MPI, allNodes );

    MPI_Barrier( MPI_COMM_WORLD );

    UTILS_DBG_MSG( mpiRank == 0, "[%u] Computing the critical path", mpiRank );

    computeCriticalPaths( runner, mpiRank );

    /* create OTF with wait states and critical path */
    if ( options.createOTF )
    {
      MPI_Barrier( MPI_COMM_WORLD );
      UTILS_DBG_MSG( mpiRank == 0, "[%u] Writing result to %s",
                     mpiRank, options.outOtfFile.c_str( ) );
    }

    runner->getAnalysis( ).saveParallelEventGroupToFile(
      options.outOtfFile,
      options.filename,
      options.createOTF,
      options.ignoreAsyncMpi,
      options.verbose >=
      VERBOSE_ANNOY );

    if ( options.mergeActivities )
    {
      UTILS_DBG_MSG( mpiRank == 0, "[%u] Merging activity statistics...", mpiRank );

      runner->mergeActivityGroups( );

      runner->printAllActivities( );

      MPI_Barrier( MPI_COMM_WORLD );
    }

    delete runner;
  }
  catch ( RTException e )
  {
    status = 1;
  }
  MPI_CHECK( MPI_Finalize( ) );
  return status;
}
