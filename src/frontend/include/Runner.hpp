/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <string>
#include <stdio.h>
#include <stdint.h>
#include <mpi.h>
#include <map>

#include "common.hpp"
#include "Parser.hpp"
#include "AnalysisEngine.hpp"
#include "CallbackHandler.hpp"
#include "otf/OTF2TraceReader.hpp"
#include "otf/OTF2ParallelTraceWriter.hpp"

namespace casita
{

 class Runner
 {
   private:
     typedef std::vector< MPIAnalysis::CriticalPathSection > SectionsList;
     typedef std::vector< MPIAnalysis::ProcessNodePair > MPINodeList;

   public:

     Runner( int mpiRank, int mpiSize );
     
     virtual
     ~Runner( );
     
     void
     startAnalysisRun( );
     
     void
     writeTrace( );

     void
     runAnalysis( Paradigm paradigm, EventStream::SortedGraphNodeList& allNodes );

     void
     computeCriticalPath( );
     

     ProgramOptions&
     getOptions( );

     AnalysisEngine&
     getAnalysis( );

     void
     printAllActivities( );

     void
     mergeActivityGroups( );

   private:
     int      mpiRank;
     int      mpiSize;
     AnalysisEngine analysis;
     ProgramOptions& options;
     CallbackHandler callbacks;
     
     //!< members to determine the critical path length
     uint64_t globalLengthCP;
     
     //!< intermediate critical path start and end
     std::pair< uint64_t, uint64_t > criticalPathStart; //* < stream ID, time >
     std::pair< uint64_t, uint64_t > criticalPathEnd;   // < stream ID, time >
     
     /**
      * The function that triggers trace reading, analysis and writing.
      * 
      * @param traceReader
      */
     void
     processTrace( OTF2TraceReader* traceReader );

     /* critical path */
     void
     getCriticalPathIntern( GraphNode*                        start,
                            GraphNode*                        end,
                            EventStream::SortedGraphNodeList& cpNodes,
                            Graph&                            subGraph );
     
     void
     getCriticalLocalNodes( MPIAnalysis::CriticalSectionsList& sections,
                            EventStream::SortedGraphNodeList& localNodes );
     
     void
     findGlobalLengthCP( );
     
     void
     findCriticalPathStart( );
     
     void
     findCriticalPathEnd( );

     int
     findLastMpiNode( GraphNode** node );

     void
     detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionsList,
                               EventStream::SortedGraphNodeList& localNodes);
     
     void
     detectCriticalPathMPIReversReplay( MPIAnalysis::CriticalSectionsList& sectionsList );

 };

}
