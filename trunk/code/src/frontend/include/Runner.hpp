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
#include "otf/ITraceReader.hpp"
#include "otf/IParallelTraceWriter.hpp"
#include "otf/IKeyValueList.hpp"

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
     findGlobalLengthCP( );

     uint64_t
     getGlobalLengthCP( );

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
     uint64_t globalLengthCP;
     
     /**
      * The function that triggers trace reading, analysis and writing.
      * 
      * @param traceReader
      */
     void
     processTrace( ITraceReader* traceReader );

     /* critical path */
     void
     getCriticalPathIntern( GraphNode*                        start,
                            GraphNode*                        end,
                            EventStream::SortedGraphNodeList& cpNodes,
                            Graph&                            subGraph );
     
     void
     getCriticalLocalNodes( MPIAnalysis::CriticalSectionsList& sections,
                            EventStream::SortedGraphNodeList& localNodes );

     int
     findLastMpiNode( GraphNode** node );

     void
     detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionsList,
                               EventStream::SortedGraphNodeList& localNodes);
     
     void
     detectCriticalPathMPIReversReplay( MPIAnalysis::CriticalSectionsList& sectionsList );

 };

}
