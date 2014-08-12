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

#include <otf2/otf2.h>
#include <map>
#include <vector>
#include <stack>
#include <set>
#include <list>
#include "ITraceReader.hpp"
#include "OTF2KeyValueList.hpp"
namespace casita
{
 namespace io
 {

  class OTF2TraceReader :
    public ITraceReader
  {
    public:

      typedef struct
      {
        char* name;
        bool isCUDA;
        bool isCUDAMaster;
        uint32_t numProcs;
        uint64_t* procs;
      } ProcessGroup;

      typedef struct
      {
        uint32_t groupId;
        uint32_t stringRef;
        uint32_t numberOfMembers;
        uint8_t paradigm;
        uint8_t groupType;
        OTF2_GroupFlag groupFlag;
        uint64_t* members;
      } OTF2Group;

      typedef uint32_t Token;
      typedef std::multimap< std::string, Token > NameTokenMap;
      typedef std::map< Token, std::string > TokenNameMap;
      typedef std::map< uint64_t, Token > IdNameTokenMap;
      typedef std::map< Token, Token > TokenTokenMap;
      typedef std::map< uint64_t, uint64_t > TokenTokenMap64;
      typedef std::map< uint64_t, Token > IdTokenMap;
      typedef std::set< Token > TokenSet;
      typedef std::map< Token, TokenSet > TokenSetMap;
      typedef std::map< uint32_t, OTF2Group > GroupIdGroupMap;

      typedef std::map< Token, ProcessGroup* > ProcessGroupMap;

      OTF2TraceReader( void* userData, uint32_t mpiRank, uint32_t mpiSize );
      ~OTF2TraceReader( );

      uint32_t
      getMPIRank( );

      uint32_t
      getMPISize( );

      uint64_t
      getMPIProcessId( );

      void
      setMPIProcessId( uint64_t processId );

      IdTokenMap&
      getProcessRankMap( );

      TokenTokenMap64&
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

      NameTokenMap&
      getNameKeysMap( );

      TokenNameMap&
      getKeyNameMap( );

      TokenNameMap&
      getDefinitionTokenStringMap( );

      ProcessGroupMap&
      getProcGoupMap( );

      OTF2KeyValueList&
      getKVList( );

      GroupIdGroupMap&
      getGroupMap( );

      std::string
      getStringRef( Token t );

      std::string
      getKeyName( uint32_t id );

      std::string
      getFunctionName( uint32_t id );

      std::string
      getProcessName( uint64_t id );

      IdNameTokenMap&
      getProcessNameTokenMap( );

      TokenTokenMap&
      getFunctionNameTokenMap( );

      std::vector< uint32_t >
      getKeys( const std::string keyName );

      int32_t
      getFirstKey( const std::string keyName );

      uint64_t
      getTimerResolution( );

      void
      setTimerResolution( uint64_t ticksPerSecond );

      uint64_t
      getTimerOffset( );

      void
      setTimerOffset( uint64_t offset );

      uint64_t
      getTraceLength( );

      void
      setTraceLength( uint64_t length );

      uint32_t
      getOmpRegionRef( );

      bool
      isChildOf( uint64_t child, uint64_t parent );

      int
      getProcessingPhase( );

    private:
      static OTF2_CallbackCode
      otf2CallbackEnter( OTF2_LocationRef location,
                         OTF2_TimeStamp time,
                         void* userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef region );

      static OTF2_CallbackCode
      otf2CallbackLeave( OTF2_LocationRef location,
                         OTF2_TimeStamp time,
                         void* userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef region );

      static OTF2_CallbackCode
      otf2Callback_MpiCollectiveEnd( OTF2_LocationRef locationID,
                                     OTF2_TimeStamp time,
                                     void* userData,
                                     OTF2_AttributeList* attributeList,
                                     OTF2_CollectiveOp collectiveOp,
                                     OTF2_CommRef communicator,
                                     uint32_t root,
                                     uint64_t sizeSent,
                                     uint64_t sizeReceived );

      static OTF2_CallbackCode
      otf2Callback_MpiRecv( OTF2_LocationRef locationID,
                            OTF2_TimeStamp time,
                            void* userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t sender,
                            OTF2_CommRef communicator,
                            uint32_t msgTag,
                            uint64_t msgLength );

      static OTF2_CallbackCode
      otf2Callback_MpiSend( OTF2_LocationRef locationID,
                            OTF2_TimeStamp time,
                            void* userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t receiver,
                            OTF2_CommRef communicator,
                            uint32_t msgTag,
                            uint64_t msgLength );

      static OTF2_CallbackCode
      OTF2_GlobalEvtReaderCallback_ThreadFork( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp time,
                                               void* userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               OTF2_Paradigm paradigm,
                                               uint32_t
                                               numberOfRequestedThreads );

      static OTF2_CallbackCode
      OTF2_GlobalEvtReaderCallback_ThreadJoin( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp time,
                                               void* userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               OTF2_Paradigm paradigm );

      /* Definition callbacks */
      static OTF2_CallbackCode
      GlobDefLocation_Register( void* userData,
                                OTF2_LocationRef location,
                                OTF2_StringRef name,
                                OTF2_LocationType locationType,
                                uint64_t numberOfEvents,
                                OTF2_LocationGroupRef locationGroup );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Attribute( void* userData,
                                              OTF2_AttributeRef self,
                                              OTF2_StringRef name,
                                              OTF2_StringRef description,
                                              OTF2_Type type );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_ClockProperties( void* userData,
                                                    uint64_t timerResolution,
                                                    uint64_t globalOffset,
                                                    uint64_t traceLength );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_LocationGroup( void* userData,
                                                  OTF2_LocationGroupRef self,
                                                  OTF2_StringRef name,
                                                  OTF2_LocationGroupType
                                                  locationGroupType,
                                                  OTF2_SystemTreeNodeRef
                                                  systemTreeParent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Location( void* userData,
                                             OTF2_LocationRef self,
                                             OTF2_StringRef name,
                                             OTF2_LocationType locationType,
                                             uint64_t numberOfEvents,
                                             OTF2_LocationGroupRef
                                             locationGroup );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Group( void* userData,
                                          OTF2_GroupRef self,
                                          OTF2_StringRef name,
                                          OTF2_GroupType groupType,
                                          OTF2_Paradigm paradigm,
                                          OTF2_GroupFlag groupFlags,
                                          uint32_t numberOfMembers,
                                          const uint64_t* members );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Comm( void* userData,
                                         OTF2_CommRef self,
                                         OTF2_StringRef name,
                                         OTF2_GroupRef group,
                                         OTF2_CommRef parent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_String( void* userData,
                                           OTF2_StringRef self,
                                           const char* string );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Region( void* userData,
                                           OTF2_RegionRef self,
                                           OTF2_StringRef name,
                                           OTF2_StringRef cannonicalName,
                                           OTF2_StringRef description,
                                           OTF2_RegionRole regionRole,
                                           OTF2_Paradigm paradigm,
                                           OTF2_RegionFlag regionFlags,
                                           OTF2_StringRef sourceFile,
                                           uint32_t beginLineNumber,
                                           uint32_t endLineNumber );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNode( void* userData,
                                                   OTF2_SystemTreeNodeRef self,
                                                   OTF2_StringRef name,
                                                   OTF2_StringRef className,
                                                   OTF2_SystemTreeNodeRef
                                                   parent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty(
        void* userData,
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
      OTF2_GlobalDefReaderCallback_RmaWin( void* userData,
                                           OTF2_RmaWinRef self,
                                           OTF2_StringRef name,
                                           OTF2_CommRef comm );

      /* communication callbacks */
      static OTF2_CallbackCode
      otf2CallbackComm_MpiCollectiveEnd( OTF2_LocationRef locationID,
                                         OTF2_TimeStamp time,
                                         void* userData,
                                         OTF2_AttributeList* attributeList,
                                         OTF2_CollectiveOp collectiveOp,
                                         OTF2_CommRef communicator,
                                         uint32_t root,
                                         uint64_t sizeSent,
                                         uint64_t sizeReceived );

      static OTF2_CallbackCode
      otf2CallbackComm_MpiCollectiveBegin( OTF2_LocationRef location,
                                           OTF2_TimeStamp time,
                                           void* userData,
                                           OTF2_AttributeList* attributeList );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinCreate( OTF2_LocationRef location,
                                     OTF2_TimeStamp time,
                                     void* userData,
                                     OTF2_AttributeList* attributeList,
                                     OTF2_RmaWinRef win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinDestroy( OTF2_LocationRef location,
                                      OTF2_TimeStamp time,
                                      void* userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_RmaWinRef win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaPut( OTF2_LocationRef location,
                               OTF2_TimeStamp time,
                               void* userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef win,
                               uint32_t remote,
                               uint64_t bytes,
                               uint64_t matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaOpCompleteBlocking( OTF2_LocationRef location,
                                              OTF2_TimeStamp time,
                                              void* userData,
                                              OTF2_AttributeList* attributeList,
                                              OTF2_RmaWinRef win,
                                              uint64_t matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaGet( OTF2_LocationRef location,
                               OTF2_TimeStamp time,
                               void* userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef win,
                               uint32_t remote,
                               uint64_t bytes,
                               uint64_t matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamBegin( OTF2_LocationRef locationID,
                                        OTF2_TimeStamp time,
                                        void* userData,
                                        OTF2_AttributeList* attributeList,
                                        OTF2_CommRef threadTeam );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamEnd( OTF2_LocationRef locationID,
                                      OTF2_TimeStamp time,
                                      void* userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_CommRef threadTeam );

      void
      setEventCallbacks( OTF2_GlobalEvtReaderCallbacks* evtReaderCallbacks );

      uint32_t mpiRank;
      uint32_t mpiSize;
      uint64_t mpiProcessId;
      IdTokenMap processRankMap;       /* maps (parent) process ID to
                                        * MPI rank */
      TokenTokenMap64 processFamilyMap;       /* tracks for each
                                               * process its direct
                                               * parent */

      OTF2_Reader* reader;
      OTF2_GlobalDefReader* gobal_def_reader;
      OTF2KeyValueList kvList;

      std::string baseFilename;
      NameTokenMap nameKeysMap;
      TokenNameMap kNameMap;
      IdNameTokenMap processNameTokenMap;
      TokenTokenMap functionNameTokenMap;
      TokenNameMap definitionTokenStringMap;
      GroupIdGroupMap groupMap;

      ProcessGroupMap processGroupMap;
      uint64_t ticksPerSecond;
      uint64_t timerOffset;
      uint64_t traceLength;
      uint32_t ompParallelRegionRef;

      int processingPhase;
  };
 }
}
