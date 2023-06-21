/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015, 2018-2019
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
 * - trigger the analysis
 * - trigger display and calculation of summary
 *
 */

#define __STDC_LIMIT_MACROS

#include <stdlib.h>
#include <string.h>
#include <time.h>       /* clock_t, clock, CLOCKS_PER_SEC */

#include <fstream>

#include "common.hpp"
#include "Parser.hpp"
#include "Runner.hpp"

using namespace casita;
using namespace casita::io;

int
main( int argc, char** argv )
{
  int status  = 0;
  int mpiRank = 0;
  int mpiSize = 0;

  MPI_CHECK( MPI_Init( &argc, &argv ) );

  MPI_CHECK( MPI_Comm_rank( MPI_COMM_WORLD, &mpiRank ) );
  MPI_CHECK( MPI_Comm_size( MPI_COMM_WORLD, &mpiSize ) );

  /* print CASITA version and parallelization configuration to console */
  if ( mpiRank == 0 )
  {
    UTILS_OUT_NOBR( "Running CASITA " CASITA_VERSION
        " with %d analysis MPI process", mpiSize );
    if ( mpiSize > 1 )
    {
      UTILS_OUT_NOBR( "es" );
    }
#ifdef _OPENMP
    if ( omp_get_max_threads( ) > 1 )
    {
      UTILS_OUT_NOBR( " and %d OpenMP threads", omp_get_max_threads( ) );
    }
#endif
    UTILS_OUT( "" ); /* line break */
  }

  if ( !Parser::getInstance( ).init( mpiRank, argc, argv ) )
  {
    if ( mpiRank == 0 )
    {
      Parser::getInstance( ).printHelp( );
    }

    MPI_CHECK( MPI_Finalize( ) );
    return -1;
  }

  try
  {
    clock_t         timestamp = clock( );

    ProgramOptions& options   = Parser::getInstance( ).getProgramOptions( );

    /* write casita command line arguments to summary file */
    ofstream        summaryFile;
    std::string     sFileName = Parser::getInstance( ).getSummaryFileName( );
    summaryFile.open( sFileName.c_str( ) );
    for ( int i = 0; i < argc; i++ )
    {
      summaryFile << argv[i] << " ";
    }
    summaryFile << std::endl;

    Runner* runner = new Runner( mpiRank, mpiSize );

    /* start the analysis run (read OTF2, generate graph, run paradigm analysis and CPA) */
    runner->prepareAnalysis( );

    /* if selected as parameter, the summary statistics are merged and printed */
    if ( options.mergeActivities )
    {
      UTILS_MSG_NOBR( mpiRank == 0 && options.verbose >= VERBOSE_TIME,
          "- Merge process statistics:" );

      /* get statistical values */
      clock_t ts_merge_stats = clock( );
      runner->mergeStatistics( );
      ts_merge_stats = clock( ) - ts_merge_stats;

      UTILS_MSG_NOBR( mpiRank == 0 && options.verbose >= VERBOSE_TIME,
          " %f sec", ( (float)ts_merge_stats ) / CLOCKS_PER_SEC );

      runner->writeStatistics( );

      clock_t ts_merge_acts  = clock( );
      runner->mergeActivityGroups( );
      ts_merge_acts  = clock( ) - ts_merge_acts;

      UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_TIME,
          " + %f sec = %f sec",
          ( (float)ts_merge_acts ) / CLOCKS_PER_SEC,
          ( (float)( ts_merge_stats + ts_merge_acts ) ) / CLOCKS_PER_SEC );

      runner->writeActivityRating( );

      runner->printToStdout( );
      /* not needed */
      /* MPI_Barrier( MPI_COMM_WORLD ); */
    }

    delete runner;

    UTILS_MSG( mpiRank == 0, "Total CASITA runtime: %f seconds.\n",
        ( (float)( clock( ) - timestamp ) ) / CLOCKS_PER_SEC );
  }
  catch( RTException e )
  {
    status = 1;
  }
  MPI_CHECK( MPI_Finalize( ) );

  return status;
}
