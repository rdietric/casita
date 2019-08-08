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
#include <omp.h>
#else
#define omp_get_max_threads() 1
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
  static int timeFormat = 0;
  
  static void
  setTimeFormat( double runtime )
  {
    if( runtime > 3600 ) // more than 1 hour
    {
      timeFormat = 2;
    }
    else if( runtime > 60 ) //more than 1 minute
    {
      timeFormat = 1;
    }
  }
  
  static const char*
  convertSecondsToStr( double seconds )
  {
    stringstream sout;

    if( timeFormat == 2 ) // force h:m:s
    {
      uint32_t hours = 0;
      if( seconds > 3600 ) // more than 1 hour
      {
        hours = seconds/3600;
        seconds = seconds - ( hours * 3600 );
      }
      sout << hours << "h";
    }
    
    if( timeFormat > 0 ) // more than 1 minute
    {
      uint32_t min = 0;
      if( seconds > 60 )
      {
        min = seconds/60;
        seconds = seconds - ( min * 60 );
      }
      sout << std::setfill('0') << std::setw(2) << min << "m";
    }
    
    sout << std::fixed << std::right;
    
    if( timeFormat == 0 )
    {
      sout.precision(6);
      sout << std::setw(12) << seconds << " s";
    }
    else
    {
      sout << std::setw(13) << seconds << "s";
    }
    
    const std::string& tmp = sout.str();   
    const char* cstr = tmp.c_str();
    
    return cstr;
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
     ~Runner();
     
     void
     prepareAnalysis();
     
     void
     writeTrace();

     void
     runAnalysis( EventStream::SortedGraphNodeList& allNodes );

     void
     writeActivityRating();

     void
     mergeActivityGroups();
     
     void
     mergeActivityGroupsP2P();
     
     void
     mergeStatistics();
     
     void
     writeStatistics();
     
     void
     printToStdout();

   private:
     int mpiRank;
     int mpiSize;
     
     //<! stores all program options
     ProgramOptions& options;
     
     //<! handle all OTF2 definition data
     io::OTF2DefinitionHandler definitions;
     
     //<! the one and only analysis engine
     AnalysisEngine analysis;
     
     //<! handle event callbacks (and still also definition callbacks)
     CallbackHandler callbacks;
     
     //<! summarizes and writes the analysis results
     io::OTF2ParallelTraceWriter* writer;
     
     //!< class members to determine the critical path length
     uint64_t globalLengthCP;
     
     //!< intermediate critical path start and end
     std::pair< uint64_t, uint64_t > criticalPathStart; //* < stream ID, time >
     std::pair< uint64_t, uint64_t > criticalPathEnd;   // < stream ID, time >
     
     uint64_t totalEventsRead;
     
     /////// \todo: only used on rank 0 /////
     uint64_t maxWaitingTime;
     uint64_t minWaitingTime;
     int maxWtimeRank;
     int minWtimeRank;
     
     // total blame over all processes
     double globalBlame;
     
     ////////////////////////////////////////
     
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
     getCriticalPathIntern( GraphNode*                        start,
                            GraphNode*                        end,
                            EventStream::SortedGraphNodeList& cpNodes );
     
     void
     getCriticalLocalNodes( MPIAnalysis::CriticalPathSection* section,
                            EventStream::SortedGraphNodeList& localNodes );
     
     void
     processSectionsParallel( MPIAnalysis::CriticalSectionsList& sections,
                              EventStream::SortedGraphNodeList& localNodes );
     
     /*void
     findGlobalLengthCP();*/
     
     void
     findCriticalPathStart();
     
     void
     findCriticalPathEnd();

     int
     findLastMpiNode( GraphNode** node );

     void
     detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionsList,
                               EventStream::SortedGraphNodeList& localNodes);

 };

}
