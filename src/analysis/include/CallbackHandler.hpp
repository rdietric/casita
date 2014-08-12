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

#include "Parser.hpp"
#include "AnalysisEngine.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/IKeyValueList.hpp"

namespace casita
{

 class CallbackHandler
 {
   public:
     CallbackHandler( ProgramOptions& options, AnalysisEngine& analysis );

     ProgramOptions&
     getOptions( );

     AnalysisEngine&
     getAnalysis( );

     void
     printNode( GraphNode* node, EventStream* stream );

     /* handlers */
     static void
     handleProcessMPIMapping( io::ITraceReader* reader,
                              uint64_t streamId, uint32_t mpiRank );

     static void
     handleDefProcess( io::ITraceReader* reader, uint32_t stream,
                       uint64_t streamId, uint64_t parentId, const char* name,
                       io::IKeyValueList* list, bool isCUDA, bool isCUDANull );

     static void
     handleDefFunction( io::ITraceReader* reader,
                        uint64_t streamId,
                        uint32_t functionId,
                        const char* name,
                        uint32_t functionGroupId );

     static void
     handleEnter( io::ITraceReader* reader, uint64_t time, uint32_t functionId,
                  uint64_t streamId, io::IKeyValueList* list );

     static void
     handleAdditionalEnter( io::ITraceReader* reader,
                            uint64_t time,
                            uint64_t functionId,
                            uint64_t streamId,
                            io::IKeyValueList* list );

     static void
     handleLeave( io::ITraceReader* reader,
                  uint64_t time,
                  uint32_t functionId,
                  uint64_t streamId,
                  io::IKeyValueList* list );

     static void
     handleAdditionalLeave( io::ITraceReader* reader,
                            uint64_t time,
                            uint32_t functionId,
                            uint64_t streamId,
                            io::IKeyValueList* list );

     static void
     handleMPIComm( io::ITraceReader* reader,
                    io::MPIType mpiType,
                    uint64_t streamId,
                    uint64_t partnerId,
                    uint32_t root,
                    uint32_t tag );

     static void
     handleMPICommGroup( io::ITraceReader* reader, uint32_t group,
                         uint32_t numProcs, const uint64_t* procs );

   private:
     ProgramOptions& options;
     AnalysisEngine& analysis;
     int mpiRank;

     /* OTF misc */
     void
     applyStreamRefsEnter( io::ITraceReader* reader, GraphNode* node,
                           io::IKeyValueList* list, Paradigm paradigm );

     void
     applyStreamRefsLeave( io::ITraceReader* reader, GraphNode* node,
                           GraphNode* oldNode, io::IKeyValueList* list,
                           Paradigm paradigm );

     static uint32_t
     readKeyVal( io::ITraceReader* reader, const char* keyName,
                 io::IKeyValueList* list );

 };

}
