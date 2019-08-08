/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <map>
#include <set>
#include <stack>
#include <mpi.h>

#include "GraphEngine.hpp"
#include "graph/EventNode.hpp"

#include "MPIAnalysis.hpp"
//#include "cuda/AnalysisParadigmCUDA.hpp"
//#include "opencl/AnalysisParadigmOpenCL.hpp"
#include "offload/AnalysisParadigmOffload.hpp"
#include "omp/AnalysisParadigmOMP.hpp"

#include "Statistics.hpp"

#include "otf/OTF2DefinitionHandler.hpp"
#include "otf/OTF2TraceReader.hpp"

using namespace casita::io;

namespace casita
{
  enum AnalysisFeature
  {
    CUDA_EVENTS = ( 1 << 0 )
  };

 class IAnalysisParadigm;

 class AnalysisEngine :
   public GraphEngine
 {
   public:
 
     AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize );

     virtual
     ~AnalysisEngine();

     uint32_t
     getMPIRank();

     uint32_t
     getMPISize();

     MPIAnalysis&
     getMPIAnalysis();
     
     void
     setDefinitionHandler( OTF2DefinitionHandler* defHandler );
     
     void
     checkPendingMPIRequests();
     
     bool 
     haveParadigm( Paradigm paradigm ) const;
     
     void
     addDetectedParadigm( Paradigm paradigm );
     
     void
     addAnalysisFeature( AnalysisFeature feature );
     
     bool
     haveAnalysisFeature( AnalysisFeature feature ) const;

     
     void
     addFilteredRegion( uint32_t regionId );

     bool
     isRegionFiltered( uint32_t regionId );

     GraphNode*
     newGraphNode( uint64_t time, uint64_t streamId,
                   const char* name, Paradigm paradigm,
                   RecordType recordType, int nodeType );

     GraphNode*
     addNewGraphNode( uint64_t            time,
                      EventStream*        stream,
                      const char*         name,
                      FunctionDescriptor* funcDesc );

     void
     applyRules( GraphNode* node );

     void
     addAnalysis( IAnalysisParadigm* paradigm );

     IAnalysisParadigm*
     getAnalysis( Paradigm paradigm );
     
     void
     createIntermediateBegin();

     void
     handlePostEnter( GraphNode* node );

     void
     handlePostLeave( GraphNode* node );

     void
     handleKeyValuesEnter( OTF2TraceReader*  reader,
                           GraphNode*        node,
                           OTF2KeyValueList* list );

     void
     handleKeyValuesLeave( OTF2TraceReader*  reader,
                           GraphNode*        node,
                           GraphNode*        oldNode,
                           OTF2KeyValueList* list );

     EventStream*
     getNullStream() const;

     size_t
     getNumAllDeviceStreams();

     GraphNode*
     getLastLeaveNode( uint64_t timestamp, uint64_t streamId ) const;
     
     void
     getLastLeaveEvent( EventStream **stream, uint64_t *timestamp );

     void
     reset();
     
     /**
      * Get elapsed time from an event time stamp, e.g. to spot an event in Vampir.
      * 
      * @param t event time stamp
      * @return elapsed time in seconds
      */
     double
     getElapsedTime( uint64_t t );

     void
     runAnalysis();
     
     void
     clearNodes();
     
     /**
      * Add a node to the deferred nodes list that could not be processed.
      * 
      * @param node node to be deferred.
      */
     void
     addDeferredNode( GraphNode* node);
     
     /**
      * Process all nodes in the deferred nodes list. 
      */
     void
     processDeferredNodes();
     
     Statistics&
     getStatistics( void );

   private:
     OTF2DefinitionHandler* defHandler;
     
     MPIAnalysis mpiAnalysis;
     
     Statistics statistics;

     // map of analysis paradigms
     typedef std::map< Paradigm, IAnalysisParadigm* > AnalysisParadigmsMap;
     
     // map of paradigms and the according analysis
     AnalysisParadigmsMap analysisParadigms;
     
     EventStream::SortedGraphNodeList allNodes;
     
     //!< defer nodes that could not be processed
     EventStream::SortedGraphNodeList deferredNodes;
     
     std::set< uint32_t > filteredFunctions;
     
     // maximum metric class and member IDs that has been read by the event reader
     uint32_t maxMetricClassId;
     uint32_t maxMetricMemberId;
     
     // maximum attribute ID that has been read by the event reader
     uint32_t maxAttributeId;
     
     // available analysis paradigms (identified during reading the trace)
     int availableParadigms;
     
     // available analysis features (set by parsing the OTF2 definitions)
     int analysisFeature;
     
     //
     size_t offloadTasksActive;
 };

}
