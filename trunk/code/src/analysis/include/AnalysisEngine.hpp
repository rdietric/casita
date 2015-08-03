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

#include <map>
#include <set>
#include <stack>
#include <mpi.h>

#include "GraphEngine.hpp"
#include "MPIAnalysis.hpp"
#include "graph/EventNode.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/IParallelTraceWriter.hpp"

using namespace casita::io;

namespace casita
{

 class IAnalysisParadigm;

 class AnalysisEngine :
   public GraphEngine
 {
   public:

     AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize );

     virtual
     ~AnalysisEngine( );

     uint32_t
     getMPIRank( );

     uint32_t
     getMPISize( );

     MPIAnalysis&
     getMPIAnalysis( );

     static bool
     getFunctionType( uint32_t            id,
                      const char*         name,
                      EventStream*        stream,
                      FunctionDescriptor* descr,
                      bool                ignoreAsyncMpi );

     void
     addFunction( uint32_t funcId, const char* name );

     uint32_t
     getNewFunctionId( );

     void
     setWaitStateFunctionId( uint32_t id );

     const char*
     getFunctionName( uint32_t id );

     GraphNode*
     newGraphNode( uint64_t time, uint64_t streamId,
                   const std::string name, Paradigm paradigm,
                   NodeRecordType recordType, int nodeType );

     GraphNode*
     addNewGraphNode( uint64_t       time,
                      EventStream*   stream,
                      const char*    name,
                      Paradigm       paradigm,
                      NodeRecordType recordType,
                      int            nodeType );

     /*
     void
     addPendingIRecvRequest( uint64_t request );

     void
     addPendingIRecv( GraphNode* node );

     void
     addDataForPendingIRecv( uint64_t request, uint64_t partner );

     void
     addPendingMPIRequest( MPI_Request* request );

     MPI_Request*
     getPendingMPIRequest( );

     void
     addPendingISendPartner( uint64_t partner );

     void
     getPendingISendPartner( );
     
     void
     addPendingMPICommForWaitAll( );

     uint64_t
     getNumberOfPendingMPICommForWaitAll( );
     */
     bool
     applyRules( GraphNode* node, Paradigm paradigm, bool verbose );

     void
     addAnalysisParadigm( IAnalysisParadigm* paradigm );

     IAnalysisParadigm*
     getAnalysisParadigm( Paradigm paradigm );

     void
     handlePostEnter( GraphNode* node );

     void
     handlePostLeave( GraphNode* node );

     void
     handleKeyValuesEnter( ITraceReader*  reader,
                           GraphNode*     node,
                           IKeyValueList* list );

     void
     handleKeyValuesLeave( ITraceReader*  reader,
                           GraphNode*     node,
                           GraphNode*     oldNode,
                           IKeyValueList* list );

     EventStream*
     getNullStream( ) const;

     size_t
     getNumAllDeviceStreams( );

     GraphNode*
     getLastLeave( uint64_t timestamp, uint64_t streamId ) const;

     void
     reset( );

     void
     saveParallelEventGroupToFile( std::string filename,
                                   std::string origFilename,
                                   bool        writeToFile,
                                   bool        ignoreAsyncMpi,
                                   bool        verbose );

     io::IParallelTraceWriter::ActivityGroupMap*
     getActivityGroupMap( );

     double
     getRealTime( uint64_t t );

   private:
     MPIAnalysis mpiAnalysis;

     // map of analysis paradigms
     typedef std::map< Paradigm, IAnalysisParadigm* > AnalysisParadigmsMap;
     AnalysisParadigmsMap      analysisParadigms;

     io::IParallelTraceWriter* writer;
/*
     uint64_t pendingIRecvRequest;
     uint64_t pendingISendPartner;
     uint64_t pendingMPICommForWaitAll;
     std::map< uint64_t, GraphNode* > requestIRecvMap;
     std::vector< MPI_Request* > pendingMPIRequests;
*/
     std::map< uint32_t, std::string > functionMap;
     uint32_t maxFunctionId;
     uint32_t waitStateFuncId;
 };

}
