/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
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

#include "common.hpp"
#include "Parser.hpp"
#include "Runner.hpp"

#define CASITA_VERSION "1.3"

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

  UTILS_MSG( mpiRank == 0, "Running CASITA %s with %d analysis processes", 
                           CASITA_VERSION, mpiSize );

  if ( !Parser::getInstance( ).init( argc, argv ) )
  {
    MPI_CHECK( MPI_Finalize() );
    return -1;
  }

  try
  {
    clock_t timestamp = clock();
    
    ProgramOptions& options = Parser::getInstance( ).getProgramOptions( );

    Runner* runner = new Runner( mpiRank, mpiSize );
    
    // start the analysis run (read OTF2, generate graph, run paradigm analysis and CPA)
    runner->startAnalysisRun( );
    
    // if selected as parameter, the summary statistics are merged and printed
    if ( options.mergeActivities )
    {
      UTILS_MSG_NOBR( mpiRank == 0, "Generate optimization rating:" );
      
      clock_t ts_merge = clock() - timestamp;

      runner->mergeActivityGroups( );
      
      ts_merge = clock() - ts_merge;
    
      UTILS_MSG( mpiRank == 0, " (%f sec)", ( (float) ts_merge ) / CLOCKS_PER_SEC );

      runner->printAllActivities( );

      MPI_Barrier( MPI_COMM_WORLD );
    }

    delete runner;
    
    timestamp = clock() - timestamp;
    
    UTILS_MSG( mpiRank == 0, "CASITA analysis took %f seconds.\n", ( (float) timestamp ) / CLOCKS_PER_SEC );
  }
  catch( RTException e )
  {
    status = 1;
  }
  MPI_CHECK( MPI_Finalize( ) );
  return status;
}
