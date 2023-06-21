/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2019,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <string>
#include <iomanip>
#include <stdio.h>
#include <stdint.h>
#include <mpi.h>
#include <map>

#ifdef _OPENMP
# include <omp.h>
#else
# define omp_get_max_threads( ) 1
#endif

#include "common.hpp"
#include "Parser.hpp"
#include "AnalysisEngine.hpp"
#include "CallbackHandler.hpp"
#include "otf/OTF2DefinitionHandler.hpp"
#include "otf/OTF2TraceReader.hpp"
#include "otf/OTF2ParallelTraceWriter.hpp"

namespace casita
{
  static int  printPrecision = -1;
  static char formattedDuration[16];

  static void
  setPrecision( double seconds )
  {
    /* if duration is more than an hour, */
    if ( seconds >= 3600 )
    {
      printPrecision = 0;
    }
    else
    if ( seconds >= 100 ) {
      printPrecision = 3; /* milliseconds */
    }
    else
    {
      printPrecision = 6; /* microseconds */
    }
  }

  static const char*
  formatDuration( double seconds )
  {
    /* fill with spaces */
    memset( formattedDuration, ' ', 16 );

    uint32_t hours = 0;
    if ( seconds > 5940 ) /* more than 99 minutes (stay within 2 digits) */
    {
      hours    = seconds / 3600;
      seconds -= ( hours * 3600 );
    }

    uint32_t min   = 0;
    if ( seconds >= 60 )
    {
      min      = seconds / 60;
      seconds -= ( min * 60 );
    }

    if ( hours > 0 )
    {
      snprintf( formattedDuration, 16, "%dh%02dm%.0lf s", hours, min, seconds );
    }
    else
    if ( min > 0 ) {
      snprintf( formattedDuration, 16, "%dm%02.*lf s", min,
          printPrecision >= 0 ? printPrecision : 3, seconds );
    }
    else
    {
      snprintf( formattedDuration, 16, "%.*lf s",
          printPrecision >= 0 ? printPrecision : 6, seconds );
    }

    return formattedDuration;
  }

  class Runner
  {
    private:
      typedef std::vector< MPIAnalysis::CriticalPathSection > SectionsList;

      typedef struct
      {
        uint32_t instances;
        uint64_t duration;
        uint64_t durationCP;
        double   blameOnCP;
        double   blame;
        uint32_t count;
      } AddUpValues;

    public:

      Runner( int mpiRank, int mpiSize );

      virtual
      ~Runner( );

      void
      prepareAnalysis( );

      void
      writeTrace( );

      void
      runAnalysis( EventStream::SortedGraphNodeList& allNodes );

      void
      writeActivityRating( );

      void
      mergeActivityGroups( );

      void
      mergeActivityGroupsP2P( );

      void
      mergeStatistics( );

      void
      writeStatistics( );

      void
      printToStdout( );

    private:
      int mpiRank;
      int mpiSize;

      /* <! stores all program options */
      ProgramOptions& options;

      /* <! handle all OTF2 definition data */
      io::OTF2DefinitionHandler definitions;

      /* <! the one and only analysis engine */
      AnalysisEngine  analysis;

      /* <! handle event callbacks (and still also definition callbacks) */
      CallbackHandler callbacks;

      /* <! summarizes and writes the analysis results */
      io::OTF2ParallelTraceWriter* writer;

      /* !< class members to determine the critical path length */
      uint64_t globalLengthCP;

      /* !< intermediate critical path start and end */
      std::pair< uint64_t, uint64_t > criticalPathStart; /* * < stream ID, time > */
      std::pair< uint64_t, uint64_t > criticalPathEnd;  /* < stream ID, time > */

      uint64_t totalEventsRead;

      /* ///// \todo: only used on rank 0 ///// */
      uint64_t maxWaitingTime;
      uint64_t minWaitingTime;
      int      maxWtimeRank;
      int      minWtimeRank;

      /* total blame over all processes */
      double   globalBlame;

      /* ////////////////////////////////////// */

      /**
       * The function that triggers trace reading, analysis and writing.
       *
       * @param traceReader
       */
      void
      processTrace( OTF2TraceReader* traceReader );

      /* critical path */
      void
      computeCriticalPath( const bool firstInterval, const bool lastInterval );

      void
      getCriticalPathIntern( GraphNode*     start,
          GraphNode*                        end,
          EventStream::SortedGraphNodeList& cpNodes );

      void
      getCriticalLocalNodes( MPIAnalysis::CriticalPathSection* section,
          EventStream::SortedGraphNodeList&                    localNodes );

      void
      processSectionsParallel( MPIAnalysis::CriticalSectionsList& sections,
          EventStream::SortedGraphNodeList&                       localNodes );

      /*void
      findGlobalLengthCP();*/

      void
      findCriticalPathStart( );

      void
      findCriticalPathEnd( );

      int
      findLastMpiNode( GraphNode** node );

      void
      detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionsList,
          EventStream::SortedGraphNodeList&                        localNodes );

  };

}
