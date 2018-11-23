/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <map>
#include <vector>
#include <stack>
#include <set>
#include <list>

#include "OTF2DefinitionHandler.hpp"
#include "OTF2KeyValueList.hpp"

namespace casita
{
 namespace io
 {
  enum MPIType
  {
    MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_ONEANDALL, MPI_ISEND, MPI_IRECV
  };
  
  class OTF2TraceReader;

  typedef void ( *HandleEnter )( OTF2TraceReader* reader, uint64_t time,
                                 uint32_t functionId, uint64_t processId,
                                 OTF2KeyValueList* list );
  typedef bool ( *HandleLeave )( OTF2TraceReader* reader, uint64_t time,
                                 uint32_t functionId, uint64_t processId,
                                 OTF2KeyValueList* list );
  typedef void ( *HandleDefProcess )( OTF2TraceReader* reader, 
                                      uint64_t processId, uint64_t parentId,
                                      const char* name,
                                      OTF2KeyValueList* list, 
                                      OTF2_LocationType locationType );
  typedef void ( *HandleProcessMPIMapping )( OTF2TraceReader* reader,
                                             uint64_t      processId,
                                             uint32_t      mpiRank );
  typedef void ( *HandleLocationProperty )( OTF2TraceReader*    reader,
                                            uint64_t            streamId,
                                            const char*         name,
                                            OTF2_Type           type,
                                            OTF2_AttributeValue value );
  typedef void ( *HandleDefFunction )( OTF2TraceReader* reader, 
                                       uint32_t functionId, const char* name,
                                       OTF2_Paradigm paradigm,
                                       OTF2_RegionRole role );
  typedef void ( *HandleDefAttribute )( OTF2TraceReader* reader, uint64_t streamId,
                                        uint32_t key, const char* name );
  typedef void ( *HandleMPIComm )( OTF2TraceReader* reader, MPIType mpiType,
                                   uint64_t processId, uint32_t partnerId,
                                   uint32_t root, uint32_t tag );
  typedef void ( *HandleMPICommGroup )( OTF2TraceReader* reader, uint32_t group,
                                        uint32_t numProcs,
                                        const uint32_t* procs );
  typedef void ( *HandleMPIIsend )( OTF2TraceReader* reader, 
                                    uint64_t         streamId,
                                    uint64_t         receiver,
                                    OTF2_CommRef     communicator,
                                    uint32_t         msgTag,
                                    uint64_t         request );
  typedef void ( *HandleMPIIrecv )( OTF2TraceReader* reader, 
                                    uint64_t         streamId,
                                    uint64_t         sender,
                                    OTF2_CommRef     communicator,
                                    uint32_t         msgTag,
                                    uint64_t         request );
  typedef void ( *HandleMPIIrecvRequest )( OTF2TraceReader* reader,
                                           uint64_t      streamId,
                                           uint64_t      request );
  typedef void ( *HandleMPIIsendComplete )( OTF2TraceReader* reader,
                                            uint64_t      streamId,
                                            uint64_t      request );
  typedef void ( *HandleRmaWinDestroy )( OTF2TraceReader* reader,
                                         uint64_t         time,
                                         uint64_t         streamId );
  typedef void ( *HandleRmaGet )( OTF2TraceReader* reader,
                                  uint64_t         time,
                                  uint64_t         streamId );
  typedef void ( *HandleRmaPut )( OTF2TraceReader* reader,
                                  uint64_t         time,
                                  uint64_t         streamId );
  typedef void ( *HandleRmaOpCompleteBlocking )( OTF2TraceReader* reader,
                                                 uint64_t         time,
                                                 uint64_t         streamId );
  
  typedef void ( *HandleThreadFork )( OTF2TraceReader* reader,
                                      uint64_t         streamId,
                                      uint32_t         requestedThreads );
  
  class OTF2TraceReader
  {
    public:

      typedef struct
      {
        char*     name;
        bool      isCUDA;
        bool      isCUDAMaster;
        uint32_t  numProcs;
        uint64_t* procs;
      } ProcessGroup;

      typedef struct
      {
        OTF2_GroupRef  groupId;
        OTF2_StringRef stringRef;
        OTF2_Paradigm  paradigm;
        OTF2_GroupType groupType;
        OTF2_GroupFlag groupFlag;
        uint32_t       numberOfMembers;
        uint32_t*      members;         //!< OTF2 communication group members
      } OTF2Group;

      typedef uint32_t Token;
      typedef std::multimap< std::string, Token > NameTokenMap;
      typedef std::map< Token, std::string > TokenNameMap;
      typedef std::map< Token, Token > TokenTokenMap;
      typedef std::map< uint64_t, uint64_t > TokenTokenMap64;
      typedef std::map< uint64_t, Token > IdTokenMap;
      typedef std::set< Token > TokenSet;
      typedef std::map< Token, TokenSet > TokenSetMap;
      typedef std::map< uint32_t, OTF2Group > CommGroupMap;
      typedef std::map< uint32_t, uint64_t > RankStreamIdMap;
      typedef std::map< uint64_t, uint32_t > LocationStringRefMap;
      typedef std::map< OTF2_StringRef, const char* > StringRefNameMap;

      typedef std::map< Token, ProcessGroup* > ProcessGroupMap;

      OTF2TraceReader( void* userData, OTF2DefinitionHandler* defHandler,
                       uint32_t mpiRank, uint32_t mpiSize );
      
      ~OTF2TraceReader();

      uint64_t
      getMPIProcessId();

      void
      setMPIStreamId( uint64_t processId );

      //IdTokenMap&
      //getProcessRankMap( );

      TokenTokenMap64&
      getProcessFamilyMap();

      void
      open( const std::string otfFilename, uint32_t maxFiles );

      void
      close();

      void
      setupEventReader( bool ignoreAsyncMPI );
      
      bool
      readEvents( uint64_t *num_events_read );

      void
      readEventsForProcess( uint64_t id, bool ignoreAsyncMPI );

      bool
      readDefinitions();

      void
      readCommunication();

      NameTokenMap&
      getNameKeysMap();

      TokenNameMap&
      getKeyNameMap();

      ProcessGroupMap&
      getProcGoupMap();

      OTF2KeyValueList&
      getKVList();

      std::vector< uint32_t >
      getKeys( const std::string keyName );

      int32_t
      getFirstKey( const std::string keyName );
      
      void*
      getUserData();
      
      HandleEnter             handleEnter;
      HandleLeave             handleLeave;
      HandleDefProcess        handleDefProcess;
      HandleLocationProperty  handleLocationProperty;
      HandleDefFunction       handleDefFunction;
      HandleDefAttribute      handleDefAttribute;
      HandleProcessMPIMapping handleProcessMPIMapping;
      HandleMPIComm           handleMPIComm;
      HandleMPICommGroup      handleMPICommGroup;
      HandleMPIIsend          handleMPIIsend;
      HandleMPIIrecv          handleMPIIrecv;
      HandleMPIIrecvRequest   handleMPIIrecvRequest;
      HandleMPIIsendComplete  handleMPIIsendComplete;
      HandleRmaWinDestroy     handleRmaWinDestroy;
      //HandleRmaPut            handleRmaPut;
      //HandleRmaGet            handleRmaGet;
      //HandleRmaOpCompleteBlocking handleRmaOpCompleteBlocking;
      HandleThreadFork        handleThreadFork;

    private:
      static OTF2_CallbackCode
      otf2CallbackEnter( OTF2_LocationRef    location,
                         OTF2_TimeStamp      time,
                         void*               userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef      region );

      static OTF2_CallbackCode
      otf2CallbackLeave( OTF2_LocationRef    location,
                         OTF2_TimeStamp      time,
                         void*               userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef      region );
      
      static OTF2_CallbackCode
      otf2Callback_MpiCollectiveEnd( OTF2_LocationRef    locationID,
                                     OTF2_TimeStamp      time,
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     OTF2_CollectiveOp   collectiveOp,
                                     OTF2_CommRef        communicator,
                                     uint32_t            root,
                                     uint64_t            sizeSent,
                                     uint64_t            sizeReceived );

      static OTF2_CallbackCode
      otf2Callback_MpiRecv( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            sender,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength );

      static OTF2_CallbackCode
      otf2Callback_MpiIRecvRequest( OTF2_LocationRef    locationID,
                                    OTF2_TimeStamp      time,
                                    void*               userData,
                                    OTF2_AttributeList* attributeList,
                                    uint64_t            requestID );

      static OTF2_CallbackCode
      otf2Callback_MpiIRecv( OTF2_LocationRef    locationID,
                             OTF2_TimeStamp      time,
                             void*               userData,
                             OTF2_AttributeList* attributeList,
                             uint32_t            sender,
                             OTF2_CommRef        communicator,
                             uint32_t            msgTag,
                             uint64_t            msgLength,
                             uint64_t            requestID );

      static OTF2_CallbackCode
      otf2Callback_MpiISend( OTF2_LocationRef    locationID,
                             OTF2_TimeStamp      time,
                             void*               userData,
                             OTF2_AttributeList* attributeList,
                             uint32_t            receiver,
                             OTF2_CommRef        communicator,
                             uint32_t            msgTag,
                             uint64_t            msgLength,
                             uint64_t            requestID );

      static OTF2_CallbackCode
      otf2Callback_MpiISendComplete( OTF2_LocationRef    locationID,
                                     OTF2_TimeStamp      time,
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     uint64_t            requestID );

      static OTF2_CallbackCode
      otf2Callback_MpiSend( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            receiver,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength );

      static OTF2_CallbackCode
      OTF2_GlobalEvtReaderCallback_ThreadFork( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp   time,
                                               void*            userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               OTF2_Paradigm    paradigm,
                                               uint32_t
                                               numberOfRequestedThreads );

      static OTF2_CallbackCode
      OTF2_GlobalEvtReaderCallback_ThreadJoin( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp   time,
                                               void*            userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               OTF2_Paradigm    paradigm );

      /* Definition callbacks */
      static OTF2_CallbackCode
      GlobDefLocation_Register( void*                 userData,
                                OTF2_LocationRef      location,
                                OTF2_StringRef        name,
                                OTF2_LocationType     locationType,
                                uint64_t              numberOfEvents,
                                OTF2_LocationGroupRef locationGroup );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Attribute( void*             userData,
                                              OTF2_AttributeRef self,
                                              OTF2_StringRef    name,
                                              OTF2_StringRef    description,
                                              OTF2_Type         type );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_ClockProperties( void*    userData,
                                                    uint64_t timerResolution,
                                                    uint64_t globalOffset,
                                                    uint64_t traceLength );
      
      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Location( void*             userData,
                                             OTF2_LocationRef  self,
                                             OTF2_StringRef    name,
                                             OTF2_LocationType locationType,
                                             uint64_t          numberOfEvents,
                                             OTF2_LocationGroupRef
                                             locationGroup );
      
      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_LocationProperty( 
                                                  void*               userData,
                                                  OTF2_LocationRef    location,
                                                  OTF2_StringRef      name,
                                                  OTF2_Type           type,
                                                  OTF2_AttributeValue value );
      
      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_LocationGroup( 
                                      void*                  userData,
                                      OTF2_LocationGroupRef  self,
                                      OTF2_StringRef         name,
                                      OTF2_LocationGroupType locationGroupType,
                                      OTF2_SystemTreeNodeRef systemTreeParent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Group( void*           userData,
                                          OTF2_GroupRef   self,
                                          OTF2_StringRef  name,
                                          OTF2_GroupType  groupType,
                                          OTF2_Paradigm   paradigm,
                                          OTF2_GroupFlag  groupFlags,
                                          uint32_t        numberOfMembers,
                                          const uint64_t* members );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Comm( void*          userData,
                                         OTF2_CommRef   self,
                                         OTF2_StringRef name,
                                         OTF2_GroupRef  group,
                                         OTF2_CommRef   parent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_String( void*          userData,
                                           OTF2_StringRef self,
                                           const char*    string );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Region( void*           userData,
                                           OTF2_RegionRef  self,
                                           OTF2_StringRef  name,
                                           OTF2_StringRef  cannonicalName,
                                           OTF2_StringRef  description,
                                           OTF2_RegionRole regionRole,
                                           OTF2_Paradigm   paradigm,
                                           OTF2_RegionFlag regionFlags,
                                           OTF2_StringRef  sourceFile,
                                           uint32_t        beginLineNumber,
                                           uint32_t        endLineNumber );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNode( void*                  userData,
                                                   OTF2_SystemTreeNodeRef self,
                                                   OTF2_StringRef         name,
                                                   OTF2_StringRef         className,
                                                   OTF2_SystemTreeNodeRef
                                                   parent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty( void*          userData,
                                                           OTF2_SystemTreeNodeRef
                                                           systemTreeNode,
                                                           OTF2_StringRef name,
                                                           OTF2_StringRef value );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain( void* userData,
                                                         OTF2_SystemTreeNodeRef
                                                         systemTreeNode,
                                                         OTF2_SystemTreeDomain
                                                         systemTreeDomain );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_RmaWin( void*          userData,
                                           OTF2_RmaWinRef self,
                                           OTF2_StringRef name,
                                           OTF2_CommRef   comm );

      /* communication callbacks */
      static OTF2_CallbackCode
      otf2CallbackComm_MpiCollectiveEnd( OTF2_LocationRef    locationID,
                                         OTF2_TimeStamp      time,
                                         void*               userData,
                                         OTF2_AttributeList* attributeList,
                                         OTF2_CollectiveOp   collectiveOp,
                                         OTF2_CommRef        communicator,
                                         uint32_t            root,
                                         uint64_t            sizeSent,
                                         uint64_t            sizeReceived );

      static OTF2_CallbackCode
      otf2CallbackComm_MpiCollectiveBegin( OTF2_LocationRef    location,
                                           OTF2_TimeStamp      time,
                                           void*               userData,
                                           OTF2_AttributeList* attributeList );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinCreate( OTF2_LocationRef    location,
                                     OTF2_TimeStamp      time,
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     OTF2_RmaWinRef      win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinDestroy( OTF2_LocationRef    location,
                                      OTF2_TimeStamp      time,
                                      void*               userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_RmaWinRef      win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaPut( OTF2_LocationRef    location,
                               OTF2_TimeStamp      time,
                               void*               userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef      win,
                               uint32_t            remote,
                               uint64_t            bytes,
                               uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaOpCompleteBlocking( OTF2_LocationRef    location,
                                              OTF2_TimeStamp      time,
                                              void*               userData,
                                              OTF2_AttributeList* attributeList,
                                              OTF2_RmaWinRef      win,
                                              uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaGet( OTF2_LocationRef    location,
                               OTF2_TimeStamp      time,
                               void*               userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef      win,
                               uint32_t            remote,
                               uint64_t            bytes,
                               uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamBegin( OTF2_LocationRef    locationID,
                                        OTF2_TimeStamp      time,
                                        void*               userData,
                                        OTF2_AttributeList* attributeList,
                                        OTF2_CommRef        threadTeam );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamEnd( OTF2_LocationRef    locationID,
                                      OTF2_TimeStamp      time,
                                      void*               userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_CommRef        threadTeam );

      void
      setEventCallbacks( OTF2_GlobalEvtReaderCallbacks* evtReaderCallbacks );
      
      
      void*            userData;
      
      //<! handler for OTF2 definitions
      OTF2DefinitionHandler* defHandler;

      //<! MPI rank of the analysis process
      uint32_t         mpiRank;
      uint32_t         mpiSize;
      
      //<! location ID (OTF2 location reference) for this MPI rank 
      uint64_t         mpiProcessId;

      
      // Map of MPI ranks with its corresponding stream IDs / OTF2 location references
      RankStreamIdMap  rankStreamMap; 
      
      // tracks for each process its direct parent
      TokenTokenMap64  processFamilyMap; 

      OTF2_Reader*     reader;
      OTF2KeyValueList kvList;

      std::string      baseFilename;
      NameTokenMap     nameKeysMap;
      TokenNameMap     kNameMap;
      
      //!< maps OTF2 location references to OTF2 string references
      LocationStringRefMap locationStringRefMap;

      CommGroupMap     groupMap;

      ProcessGroupMap  processGroupMap;
      uint32_t         ompForkJoinRef;
  };
 }
}
