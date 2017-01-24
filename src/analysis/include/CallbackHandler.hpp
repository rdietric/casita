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

#include "Parser.hpp"
#include "AnalysisEngine.hpp"

#include "otf/OTF2TraceReader.hpp"

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

     CallbackHandler( AnalysisEngine& analysis );

     AnalysisEngine&
     getAnalysis( );

     void
     printNode( GraphNode* node, EventStream* stream );

     /* handlers */
     static void
     handleProcessMPIMapping( io::OTF2TraceReader* reader,
                              uint64_t streamId, uint32_t mpiRank );

     static void
     handleDefProcess( io::OTF2TraceReader* reader, uint64_t streamId, 
                       uint64_t parentId, const char* name,
                       io::OTF2KeyValueList* list, bool isGPU );

     static void
     handleDefFunction( io::OTF2TraceReader* reader,
                        uint64_t          streamId,
                        uint32_t          functionId,
                        const char*       name,
                        uint32_t          functionGroupId );
     
     static void
     handleDefAttribute( io::OTF2TraceReader* reader,
                         uint64_t          streamId,
                         uint32_t          key, 
                         const char*       name );

     static void
     handleEnter( io::OTF2TraceReader* reader, uint64_t time, uint32_t functionId,
                  uint64_t streamId, io::OTF2KeyValueList* list );

     static bool
     handleLeave( io::OTF2TraceReader*  reader,
                  uint64_t              time,
                  uint32_t              functionId,
                  uint64_t              streamId,
                  io::OTF2KeyValueList* list );
     
     /**
      * Handle RMA window destroy events as they are most often the last events in a 
      * stream.
      * 
      * @param location
      * @param time
      * @param userData
      * @param attributeList
      * @param win
      * @return 
      */
     static void
     handleRmaWinDestroy( io::OTF2TraceReader* reader,
                          uint64_t             time,
                          uint64_t             streamId );

     static void
     handleMPIComm( io::OTF2TraceReader* reader,
                    io::MPIType       mpiType,
                    uint64_t          streamId,
                    uint64_t          partnerId,
                    uint32_t          root,
                    uint32_t          tag );

     static void
     handleMPICommGroup( io::OTF2TraceReader* reader, uint32_t group,
                         uint32_t numProcs, const uint64_t* procs );

     static void
     handleMPIIsend( OTF2TraceReader* reader, uint64_t streamId, uint64_t receiver,
                     uint64_t request );
     
     static void
     handleMPIIrecv( OTF2TraceReader* reader, uint64_t streamId, uint64_t sender,
                     uint64_t request );

     static void
     handleMPIIrecvRequest( OTF2TraceReader* reader, uint64_t streamId, uint64_t request );

     static void
     handleMPIIsendComplete( OTF2TraceReader* reader, uint64_t streamId, uint64_t request );

   private:
     AnalysisEngine& analysis;
     int mpiRank;

     std::map< uint64_t, OTF2_Barrier_Event > lastBarrierEvent;

     /**
      * Get an uint32_t type attribute (or key-value) from the given key value list.
      * 
      * @param reader the trace reader
      * @param keyName the name of the attribute
      * @param list the attribute list
      * 
      * @return the uint32_t type attribute
      */
     static uint32_t
     readAttributeUint32( io::OTF2TraceReader* reader, const char* keyName,
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
     readAttributeUint64( io::OTF2TraceReader* reader, const char* keyName,
                       io::OTF2KeyValueList* list );

 };

}
