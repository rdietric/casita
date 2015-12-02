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

     typedef struct
     {
       uint64_t callTime;
       uint64_t followingEventTime;
       uint32_t regionRef;
     } OTF2_Barrier_Event;

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
                       io::OTF2KeyValueList* list, bool isCUDA, bool isCUDANull );

     static void
     handleDefFunction( io::ITraceReader* reader,
                        uint64_t          streamId,
                        uint32_t          functionId,
                        const char*       name,
                        uint32_t          functionGroupId );
     
     static void
     handleDefAttribute( io::ITraceReader* reader,
                         uint64_t          streamId,
                         uint32_t          key, 
                         const char*       name,
                         const char*       description );

     static void
     handleEnter( io::ITraceReader* reader, uint64_t time, uint32_t functionId,
                  uint64_t streamId, io::OTF2KeyValueList* list );

     static bool
     handleLeave( io::ITraceReader*  reader,
                  uint64_t           time,
                  uint32_t           functionId,
                  uint64_t           streamId,
                  io::OTF2KeyValueList* list );

     static void
     handleMPIComm( io::ITraceReader* reader,
                    io::MPIType       mpiType,
                    uint64_t          streamId,
                    uint64_t          partnerId,
                    uint32_t          root,
                    uint32_t          tag );

     static void
     handleMPICommGroup( io::ITraceReader* reader, uint32_t group,
                         uint32_t numProcs, const uint64_t* procs );

     static void
     handleMPIIsend( ITraceReader* reader, uint64_t streamId, uint64_t receiver,
                     uint64_t request );
     
     static void
     handleMPIIrecv( ITraceReader* reader, uint64_t streamId, uint64_t sender,
                     uint64_t request );

     static void
     handleMPIIrecvRequest( ITraceReader* reader, uint64_t streamId, uint64_t request );

     static void
     handleMPIIsendComplete( ITraceReader* reader, uint64_t streamId, uint64_t request );

   private:
     ProgramOptions& options;
     AnalysisEngine& analysis;
     int mpiRank;

     std::map< uint64_t, OTF2_Barrier_Event > lastBarrierEvent;

     /* OTF misc */
     void
     applyStreamRefsEnter( io::ITraceReader* reader, GraphNode* node,
                           io::OTF2KeyValueList* list, Paradigm paradigm );

     void
     applyStreamRefsLeave( io::ITraceReader* reader, GraphNode* node,
                           GraphNode* oldNode, io::OTF2KeyValueList* list,
                           Paradigm paradigm );

     static uint32_t
     readKeyVal( io::ITraceReader* reader, const char* keyName,
                 io::OTF2KeyValueList* list );
     
     /**
      * Get an uint64_t type attribute (or key-value) from the given key value list.
      * 
      * @param reader the trace reader
      * @param keyName the name of the attribute
      * @param list the attribute list
      * 
      * @return the uint64_t type attribute
      */
     static uint64_t
     readKeyValUInt64( io::ITraceReader* reader, const char* keyName,
                       io::OTF2KeyValueList* list );

 };

}
