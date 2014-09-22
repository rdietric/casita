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

#include <open-trace-format/otf.h>
#include <vector>
#include <map>
#include <stack>
#include "otf/IParallelTraceWriter.hpp"
#include "OTF1TraceReader.hpp"

namespace casita
{
 namespace io
 {

  typedef std::map< uint32_t, std::vector< OTF_ATTR_TYPE > > AttrListMap;

  typedef struct
  {
    uint32_t    maxFunctionID;
    uint64_t    timerResolution;
    OTF_Writer* writer;
  } WriterData;

  enum OTF1EVENT_TYPE { OTF1_ENTER, OTF1_LEAVE, OTF1_MISC };

  typedef struct
  {
    uint64_t       time;
    uint32_t       regionRef;
    uint32_t       location;
    OTF1EVENT_TYPE type;
  } OTF1Event;

  class OTF1ParallelTraceWriter :
    public IParallelTraceWriter
  {
    public:
      bool writeToFile;

      OTF1ParallelTraceWriter( const char* streamRefKeyName,
                               const char* eventRefKeyName,
                               const char* funcResultKeyName,
                               uint32_t    mpiRank,
                               uint32_t    mpiSize,
                               const char* originalFilename,
                               bool        writeToFile );
      virtual
      ~OTF1ParallelTraceWriter( );

      void
      open( const std::string otfFilename, uint32_t maxFiles,
            uint32_t numStreams );

      void
      close( );

      void
      writeDefProcess( uint64_t id, uint64_t parentId,
                       const char* name, ProcessGroup pg );

      void
      writeDefCounter( uint32_t id, const char* name, int properties );

      void
      writeNode( GraphNode*       node,
                 CounterTable&    ctrTable,
                 bool             lastProcessNode,
                 const GraphNode* futureNode );

      void
      writeProcess( uint64_t                          processId,
                    EventStream::SortedGraphNodeList* nodes,
                    bool                              enableWaitStates,
                    GraphNode*                        pLastGraphNode,
                    bool                              verbose,
                    CounterTable*                     ctrTable,
                    Graph*                            graph );

    private:
      uint32_t               totalNumStreams;
      uint64_t               timerResolution;
      int*                   mpiNumProcesses;

      typedef OTF_WStream* OTF_WStream_ptr;

      std::string            outputFilename, originalFilename;

      OTF_FileManager*       fileMgr;
      OTF_KeyValueList*      kvList;
      OTF_Writer*            globalWriter;
      OTF_Reader*            reader;

      std::map< uint32_t, OTF_WStream_ptr > processWStreamMap;
      std::stack< uint64_t > cpTimeCtrStack;

      uint32_t               streamRefKey, eventRefKey, funcResultKey;

      void
      copyGlobalDefinitions( );

      void
      copyMasterControl( );

      bool
      processNextNode( OTF1Event event );

      bool
      processCPUEvent( OTF1Event event );

      void
      assignBlame( uint64_t currentTime, uint64_t currentStream );

      EventStream::SortedGraphNodeList* processNodes;
      bool        enableWaitStates;
      EventStream::SortedGraphNodeList::iterator iter;
      GraphNode*  lastGraphNode;
      CounterTable* cTable;
      Graph*      graph;

      bool        verbose;

      uint32_t    cpuNodes;
      uint32_t    currentStackLevel;

      std::map< uint32_t, std::list< OTF1Event > > currentCPUNodes;
      std::map< uint32_t, std::string > regionNameList;
      AttrListMap attrListMap;
      std::map< uint32_t, std::list< uint32_t > > processGroupProcessListMap;
      std::map< uint32_t, bool >       deviceStreamMap;
      std::map< uint64_t, GraphNode* > lastProcessedNodePerProcess;
      std::map< uint64_t, OTF1Event >  lastCPUEventPerProcess;
      std::stack< uint32_t > eventStack;
      std::map< uint64_t, uint64_t > lastTimeOnCriticalPath;
      std::list< uint32_t > currentlyRunningCPUFunctions;
      Graph::EdgeList openEdges;
      uint32_t lastNodeCheckedForEdgesId;

      static int
      otf1HandleDefProcess( void*             userData,
                            uint32_t          stream,
                            uint32_t          processId,
                            const char*       name,
                            uint32_t          parent,
                            OTF_KeyValueList* list );

      static int
      otf1HandleDefProcessGroup( void*             userData,
                                 uint32_t          stream,
                                 uint32_t          procGroup,
                                 const char*       name,
                                 uint32_t          numberOfProcs,
                                 const uint32_t*   procs,
                                 OTF_KeyValueList* list );

      static int
      otf1HandleDefProcessOrGroupAttributes( void*             userData,
                                             uint32_t          stream,
                                             uint32_t          proc_token,
                                             uint32_t          attr_token,
                                             OTF_KeyValueList* list );

      static int
      otf1HandleDefAttributeList( void*             userData,
                                  uint32_t          stream,
                                  uint32_t          attr_token,
                                  uint32_t          num,
                                  OTF_ATTR_TYPE*    array,
                                  OTF_KeyValueList* list );

      static int
      otf1HandleDefFunction( void*             userData,
                             uint32_t          stream,
                             uint32_t          func,
                             const char*       name,
                             uint32_t          funcGroup,
                             uint32_t          source,
                             OTF_KeyValueList* list );

      static int
      otf1HandleDefFunctionGroup( void*             userData,
                                  uint32_t          stream,
                                  uint32_t          funcGroup,
                                  const char*       name,
                                  OTF_KeyValueList* list );

      static int
      otf1HandleDefKeyValue( void*             userData,
                             uint32_t          stream,
                             uint32_t          key,
                             OTF_Type          type,
                             const char*       name,
                             const char*       description,
                             OTF_KeyValueList* list );

      static int
      otf1HandleDefTimerResolution( void*             userData,
                                    uint32_t          stream,
                                    uint64_t          ticksPerSecond,
                                    OTF_KeyValueList* list );

      static int
      otf1HandleEnter( void*             userData,
                       uint64_t          time,
                       uint32_t          functionId,
                       uint32_t          processId,
                       uint32_t          source,
                       OTF_KeyValueList* list );

      static int
      otf1HandleLeave( void*             userData,
                       uint64_t          time,
                       uint32_t          functionId,
                       uint32_t          processId,
                       uint32_t          source,
                       OTF_KeyValueList* list );

      static int
      otf1HandleSendMsg( void*             userData,
                         uint64_t          time,
                         uint32_t          sender,
                         uint32_t          receiver,
                         uint32_t          group,
                         uint32_t          type,
                         uint32_t          length,
                         uint32_t          source,
                         OTF_KeyValueList* list );

      static int
      otf1HandleRecvMsg( void*             userData,
                         uint64_t          time,
                         uint32_t          receiver,
                         uint32_t          sender,
                         uint32_t          group,
                         uint32_t          type,
                         uint32_t          length,
                         uint32_t          source,
                         OTF_KeyValueList* list );

      static int
      otf1HandleBeginCollectiveOperation( void*             userData,
                                          uint64_t          time,
                                          uint32_t          process,
                                          uint32_t          collOp,
                                          uint64_t          matchingId,
                                          uint32_t          procGroup,
                                          uint32_t          rootProc,
                                          uint64_t          sent,
                                          uint64_t          received,
                                          uint32_t          scltoken,
                                          OTF_KeyValueList* list );

      static int
      otf1HandleEndCollectiveOperation( void*             userData,
                                        uint64_t          time,
                                        uint32_t          process,
                                        uint64_t          matchingId,
                                        OTF_KeyValueList* list );

      static int
      otf1HandleRMAEnd( void*             userData,
                        uint64_t          time,
                        uint32_t          process,
                        uint32_t          remote,
                        uint32_t          communicator,
                        uint32_t          tag,
                        uint32_t          source,
                        OTF_KeyValueList* list );

      static int
      otf1HandleRMAGet( void*             userData,
                        uint64_t          time,
                        uint32_t          process,
                        uint32_t          origin,
                        uint32_t          target,
                        uint32_t          communicator,
                        uint32_t          tag,
                        uint64_t          bytes,
                        uint32_t          source,
                        OTF_KeyValueList* list );

      static int
      otf1HandleRMAPut( void*             userData,
                        uint64_t          time,
                        uint32_t          process,
                        uint32_t          origin,
                        uint32_t          target,
                        uint32_t          communicator,
                        uint32_t          tag,
                        uint64_t          bytes,
                        uint32_t          source,
                        OTF_KeyValueList* list );

  };

 }

}
