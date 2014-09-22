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
#include <map>
#include <vector>
#include <stack>
#include <set>
#include <list>
#include "ITraceReader.hpp"
#include "OTF1KeyValueList.hpp"

namespace casita
{
 namespace io
 {

  class OTF1TraceReader :
    public ITraceReader
  {
    public:

      typedef struct
      {
        char*     name;
        bool      isCUDA;
        bool      isCUDAMaster;
        uint32_t  numProcs;
        uint32_t* procs;
      } ProcessGroup;

      enum CommEventType { OTF1_COLL_BEGIN, OTF1_COLL_END, OTF1_RMA_GET,
                           OTF1_RMA_PUT, OTF1_RMA_END,
                           OTF1_SEND_MSG, OTF1_RECV_MSG };

      typedef struct
      {
        CommEventType type;
        uint32_t      idInList;
        uint64_t      time;
      } OTF1CommEvent;

      typedef struct
      {
        uint64_t          time;
        uint32_t          sender;
        uint32_t          receiver;
        uint32_t          group;
        uint32_t          type;
        uint32_t          length;
        uint32_t          source;
        OTF_KeyValueList* list;
      } OTF1SendMsg;

      typedef struct
      {
        uint64_t          time;
        uint32_t          sender;
        uint32_t          receiver;
        uint32_t          group;
        uint32_t          type;
        uint32_t          length;
        uint32_t          source;
        OTF_KeyValueList* list;
      } OTF1RecvMsg;

      typedef struct
      {
        uint64_t          time;
        uint32_t          process;
        uint32_t          collOp;
        uint64_t          matchingId;
        uint32_t          procGroup;
        uint32_t          rootProc;
        uint64_t          sent;
        uint64_t          received;
        uint32_t          scltoken;
        OTF_KeyValueList* list;
      } OTF1CollBeginOp;

      typedef struct
      {
        uint64_t          time;
        uint32_t          process;
        uint64_t          matchingId;
        OTF_KeyValueList* list;
      } OTF1CollEndOp;

      typedef struct
      {
        uint64_t          time;
        uint32_t          process;
        uint32_t          remote;
        uint32_t          communicator;
        uint32_t          tag;
        uint32_t          source;
        OTF_KeyValueList* list;
      } OTF1RMAEnd;

      typedef struct
      {
        uint64_t          time;
        uint32_t          process;
        uint32_t          origin;
        uint32_t          target;
        uint32_t          communicator;
        uint32_t          tag;
        uint64_t          bytes;
        uint32_t          source;
        OTF_KeyValueList* list;
      } OTF1RMAPut;

      typedef struct
      {
        uint64_t          time;
        uint32_t          process;
        uint32_t          origin;
        uint32_t          target;
        uint32_t          communicator;
        uint32_t          tag;
        uint64_t          bytes;
        uint32_t          source;
        OTF_KeyValueList* list;
      } OTF1RMAGet;

      typedef uint32_t Token;
      typedef std::multimap< std::string, Token > NameTokenMap;
      typedef std::map< Token, std::string > TokenNameMap;
      typedef std::map< Token, Token > TokenTokenMap;
      typedef std::set< Token > TokenSet;
      typedef std::map< Token, TokenSet > TokenSetMap;

      typedef std::map< Token, ProcessGroup* > ProcessGroupMap;
      typedef std::map< Token, std::vector< OTF_ATTR_TYPE > > AttrListMap;
      typedef std::map< Token, std::stack< Token > > ProcessFuncStack;

      OTF1TraceReader( void* userData, uint32_t mpiRank );
      ~OTF1TraceReader( );

      uint32_t
      getMPIRank( );

      uint32_t
      getMPIProcessId( );

      void
      setMPIProcessId( uint32_t processId );

      TokenTokenMap&
      getProcessRankMap( );

      TokenTokenMap&
      getProcessFamilyMap( );

      void
      open( const std::string otfFilename, uint32_t maxFiles );

      void
      close( );

      void
      readEvents( );

      void
      readEventsForProcess( uint64_t id );

      void
      readDefinitions( );

      void
      readCommunication( );

      OTF1KeyValueList&
      getKVList( );

      NameTokenMap&
      getNameKeysMap( );

      TokenNameMap&
      getKeyNameMap( );

      TokenNameMap&
      getFuncNameMap( );

      TokenNameMap&
      getProcNameMap( );

      ProcessGroupMap&
      getProcGoupMap( );

      AttrListMap&
      getAttrListMap( );

      ProcessFuncStack&
      getFuncStack( );

      std::list< OTF1CommEvent >&
      getCommEventList( uint32_t processId );

      OTF1CommEvent
      getCurrentCommEvent( uint32_t processId );

      uint64_t
      getCurrentCommEventTime( uint32_t processId );

      std::vector< OTF1CollBeginOp >&
      getCollBeginList( uint32_t processId );

      std::vector< OTF1SendMsg >&
      getSendMsgList( uint32_t processId );

      std::vector< OTF1RecvMsg >&
      getRecvMsgList( uint32_t processId );

      std::vector< OTF1CollEndOp >&
      getCollEndList( uint32_t processId );

      std::vector< OTF1RMAEnd >&
      getRmaEndList( uint32_t processId );

      std::vector< OTF1RMAGet >&
      getRmaGetList( uint32_t processId );

      std::vector< OTF1RMAPut >&
      getRmaPutList( uint32_t processId );

      std::string
      getKeyName( uint32_t id );

      std::string
      getFunctionName( uint32_t id );

      std::string
      getProcessName( uint64_t id );

      std::vector< uint32_t >
      getKeys( const std::string keyName );

      int32_t
      getFirstKey( const std::string keyName );

      uint64_t
      getTimerResolution( );

      uint64_t
      getTimerOffset( );

      void
      setTimerResolution( uint64_t ticksPerSecond );

      bool
      isChildOf( uint32_t child, uint32_t parent );

      int
      getProcessingPhase( );

    private:
      static int
      otf1HandleDefProcessGroupMPI( void*             userData,
                                    uint32_t          stream,
                                    uint32_t          procGroup,
                                    const char*       name,
                                    uint32_t          numberOfProcs,
                                    const uint32_t*   procs,
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
      otf1HandleDefFunction( void*       userData,
                             uint32_t    stream,
                             uint32_t    func,
                             const char* name,
                             uint32_t    funcGroup,
                             uint32_t    source );

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

      void
      setEventHandlers( OTF_HandlerArray* handlers );

      void
      setCommEventHandlers( OTF_HandlerArray* handlers );

      uint32_t         mpiRank;
      uint32_t         mpiProcessId;
      TokenTokenMap    processRankMap;    /* maps (parent) process ID
                                           * to MPI rank */
      TokenTokenMap    processFamilyMap;    /* tracks for each process
                                             * its direct parent */

      OTF_FileManager* fileMgr;
      OTF_Reader*      reader;
      OTF1KeyValueList kvList;

      std::map< uint32_t, std::list< OTF1CommEvent > >     commEventListMap;
      std::map< uint32_t, std::vector< OTF1CollBeginOp > > collBeginListMap;
      std::map< uint32_t, std::vector< OTF1CollEndOp > >   collEndListMap;
      std::map< uint32_t, std::vector< OTF1RMAEnd > >      rmaEndListMap;
      std::map< uint32_t, std::vector< OTF1RMAGet > >      rmaGetListMap;
      std::map< uint32_t, std::vector< OTF1RMAPut > >      rmaPutListMap;
      std::map< uint32_t, std::vector< OTF1SendMsg > >     sendMsgListMap;
      std::map< uint32_t, std::vector< OTF1RecvMsg > >     recvMsgListMap;

      std::string      baseFilename;
      NameTokenMap     nameKeysMap;
      TokenNameMap     kNameMap;
      TokenNameMap     fNameMap;
      TokenNameMap     pNameMap;
      ProcessGroupMap  processGroupMap;
      AttrListMap      attrListMap;
      ProcessFuncStack funcStack;
      uint64_t ticksPerSecond;
      uint64_t timerOffset;

      int processingPhase;
  };
 }
}
