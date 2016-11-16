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

#include <otf2/otf2.h>
#include <vector>
#include <map>
#include <stack>
#include <list>
#include <string>
#include "AnalysisMetric.hpp"
#include "OTF2TraceReader.hpp"
#include "EventStream.hpp"
#include "graph/Graph.hpp"

namespace casita
{
 namespace io
 {
    enum ProcessGroup
    {
      PG_HOST        = 1, PG_DEVICE, PG_DEVICE_NULL
    };

  typedef std::map< uint32_t, uint32_t > CtrInstanceMap;

  typedef struct
  {
    uint64_t         time;
    OTF2_RegionRef   regionRef;
    OTF2_LocationRef location;
    RecordType       type;
  } OTF2Event;

  typedef struct
  {
    uint64_t time;
    uint32_t threadTeam;
    uint64_t location;
  } OTF2ThreadTeamBegin;

  typedef struct
  {
    OTF2_LocationRef locationID;
    OTF2_TimeStamp   time;
    OTF2_Paradigm    paradigm;
    uint32_t         numberOfRequestedThreads;
  } OTF2ThreadFork;

  class OTF2ParallelTraceWriter
  {
    public:
      
      typedef struct
      {
        uint32_t functionId;
        uint32_t numInstances;
        uint32_t numUnifyStreams;
        uint64_t totalDuration;
        uint64_t totalDurationOnCP;
        uint64_t totalBlame;
        uint64_t blameOnCP;
        double   fractionCP;
        double   fractionBlame;
        uint64_t lastEnterTime;
      } ActivityGroup;

      // sort activities by blame on critical path 
      // (if equal CP time, then blame, then function ID)
      typedef struct
      {
        bool
        operator()( const ActivityGroup& g1, const ActivityGroup& g2 ) const
        { 
          if ( g1.blameOnCP == g2.blameOnCP )
          {
            if( g1.totalDurationOnCP == g2.totalDurationOnCP )
            {
              if( g1.totalBlame == g2.totalBlame )
              {
                return g1.functionId > g2.functionId;
              }
              else
              {
                return g1.totalBlame > g2.totalBlame;
              }
            }
            else
            {
              return g1.totalDurationOnCP > g2.totalDurationOnCP;
            }
          }
          else
          {
            return g1.blameOnCP > g2.blameOnCP;
          }
        }
      } ActivityGroupCompare;
    
      // key: OTF2 region reference, value: activity group
      typedef std::map< uint32_t, ActivityGroup > ActivityGroupMap;
      
      bool writeToFile;

      OTF2ParallelTraceWriter( uint32_t        mpiRank,
                               uint32_t        mpiSize,
                               const char*     originalFilename,
                               bool            writeToFile,
                               AnalysisMetric* metrics,
                               bool            ignoreAsyncMpi );
      virtual
      ~OTF2ParallelTraceWriter( );

      void
      open( const std::string otfFilename, uint32_t maxFiles );

      void
      close( );
      
      void
      reset( );

      void
      writeDefProcess( uint64_t id, uint64_t parentId,
                       const char* name, ProcessGroup pg );
      
      /**
       * Write definitions for self-defined (analysis) metrics to output trace file.
       */
      void
      writeAnalysisMetricDefinitions( void );
      
      void
      setupAttributeList( void );
      
      void
      setupEventReader( uint64_t streamId );
      
      bool
      writeStream( EventStream* stream,
                   Graph*       graph,
                   uint64_t*    events_read );

      std::string
      getRegionName( const OTF2_RegionRef regionRef ) const;
      
      ActivityGroupMap*
      getActivityGroupMap( )
      {
        return &activityGroupMap;
      }
      
      static ProcessGroup
      streamTypeToGroup( EventStream::EventStreamType pt )
      {
        switch ( pt )
        {
          case EventStream::ES_DEVICE:
            return PG_DEVICE;
          case EventStream::ES_DEVICE_NULL:
            return PG_DEVICE_NULL;
          default:
            return PG_HOST;
        }
      }

    private:
      
      uint32_t mpiRank, mpiSize;
      
      // maps OTF2 region references to activity groups to collect a global profile
      ActivityGroupMap activityGroupMap;
      
      //!< pointer to the table of available counters
      AnalysisMetric* cTable; 
      
      bool ignoreAsyncMpi;
      int  verbose;
      
      uint64_t timerResolution;
      
      // 
      uint64_t timerOffset;
      
      double
      getRealTime(  uint64_t time );
      
      //!< counter to assign IDs to string definitions
      uint64_t      counterForStringDefinitions;
      
      //!< counter to assign IDs to attribute types
      uint64_t      counterForAttributeId;
      
      //!< regionReference for internal Fork/Join
      uint32_t      ompForkJoinRef;

      std::string   outputFilename, originalFilename, pathToFile;

      //!< maps each process to corresponding evtWriter
      std::map< uint64_t, OTF2_EvtWriter* > evt_writerMap;
      
      OTF2_GlobalDefWriter* global_def_writer;
      OTF2_Archive* archive;
      OTF2_Reader*  reader;
      OTF2_AttributeList* attributes;

      MPI_Comm commGroup;

      std::map< uint32_t, const char* > idStringMap;

      //!< < metric ID, metric value >
      typedef std::map< MetricType, uint64_t > CounterMap;
      
      //!< < event location, stack of counter values >
      typedef std::map< uint64_t, std::stack< CounterMap* > > CounterStackMap;
      
      //!< < event location, stack of region references >
      typedef std::map< uint64_t, std::stack< OTF2_RegionRef > > ActivityStackMap;
      typedef std::map< uint64_t, uint64_t > TimeMap;
      typedef std::list< Edge* > OpenEdgesList;
      typedef std::map< uint64_t, bool > BooleanMap;

      void
      copyGlobalDefinitions( );

      void
      updateActivityGroupMap( OTF2Event event, CounterMap& counters );

      uint64_t
      computeCPUEventBlame( OTF2Event event );
      
      void
      writeEventsWithAttributes( OTF2Event event, CounterMap& counters );
      
      void
      writeEventsWithCounters( OTF2Event event, CounterMap& counters, bool writeEvents );

      void
      processNextEvent( OTF2Event event, const std::string eventName );
      
      void
      clearOpenEdges( );

      EventStream::SortedGraphNodeList* processNodes;
      EventStream::SortedGraphNodeList::iterator currentNodeIter;
      EventStream* currentStream;

      bool   isFirstProcess;
      Graph* graph;
      
      //!< save last counter values to avoid writing of unused counter records
      CounterMap lastCounterValues;
      
      //!< activity value stack map < event.location, stack of CounterMaps >
      CounterStackMap leaveCounterStack;

      //!< Keep track of activity stack per process.
      ActivityStackMap activityStack;
      
      //!< Store last event time per process (necessary to calculate metric values correctly)
      TimeMap lastEventTime;
      
      /* Keep track of edges that exist between past and future nodes.
       * Necessary to distribute correct blame to CPU nodes.
       * When processing internal nodes, all out-edges are opened.
       * Blame was assigned to these edges during analysis, now it is
       * distributed to CPU nodes between internal nodes.
       */
      OpenEdgesList openEdges;
      
      /* Keep track if process is currently on critical path
       * -> necessary to write counter values correctly */
      BooleanMap    processOnCriticalPath;

      /** Tells if a stream is a device stream.
       * (necessary to find out if an event is mapped by an internal node. */
      std::map< uint64_t, bool > deviceStreamMap;
      std::map< uint32_t, OTF2_StringRef > regionNameIdList;

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
      OTF2_GlobalDefReaderCallback_LocationGroup( void*                 userData,
                                                  OTF2_LocationGroupRef self,
                                                  OTF2_StringRef        name,
                                                  OTF2_LocationGroupType
                                                  locationGroupType,
                                                  OTF2_SystemTreeNodeRef
                                                  systemTreeParent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_Location( void*             userData,
                                             OTF2_LocationRef  self,
                                             OTF2_StringRef    name,
                                             OTF2_LocationType locationType,
                                             uint64_t          numberOfEvents,
                                             OTF2_LocationGroupRef
                                             locationGroup );

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
      OTF2_GlobalDefReaderCallback_Region_forParadigmMap( void*          userData,
                                                          OTF2_RegionRef self,
                                                          OTF2_StringRef name,
                                                          OTF2_StringRef
                                                          cannonicalName,
                                                          OTF2_StringRef
                                                          description,
                                                          OTF2_RegionRole
                                                          regionRole,
                                                          OTF2_Paradigm
                                                          paradigm,
                                                          OTF2_RegionFlag
                                                          regionFlags,
                                                          OTF2_StringRef
                                                          sourceFile,
                                                          uint32_t
                                                          beginLineNumber,
                                                          uint32_t
                                                          endLineNumber );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNode( void*                  userData,
                                                   OTF2_SystemTreeNodeRef self,
                                                   OTF2_StringRef         name,
                                                   OTF2_StringRef         className,
                                                   OTF2_SystemTreeNodeRef
                                                   parent );

      static OTF2_CallbackCode
      OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty( 
                                          void*                  userData,
                                          OTF2_SystemTreeNodeRef systemTreeNode,
                                          OTF2_StringRef         name,
                                          OTF2_Type              type,
                                          OTF2_AttributeValue    value );

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

      /* callbacks for comm/enter/leave/fork/join events */
      static OTF2_CallbackCode
      otf2CallbackComm_MpiCollectiveEnd( OTF2_LocationRef    locationID,
                                         OTF2_TimeStamp      time,
                                         uint64_t            eventPosition,
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
                                           uint64_t            eventPosition,
                                           void*               userData,
                                           OTF2_AttributeList* attributeList );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinCreate( OTF2_LocationRef    location,
                                     OTF2_TimeStamp      time,
                                     uint64_t            eventPosition,
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     OTF2_RmaWinRef      win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaWinDestroy( OTF2_LocationRef    location,
                                      OTF2_TimeStamp      time,
                                      uint64_t            eventPosition,
                                      void*               userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_RmaWinRef      win );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaPut( OTF2_LocationRef    location,
                               OTF2_TimeStamp      time,
                               uint64_t            eventPosition,
                               void*               userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef      win,
                               uint32_t            remote,
                               uint64_t            bytes,
                               uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaOpCompleteBlocking( OTF2_LocationRef    location,
                                              OTF2_TimeStamp      time,
                                              uint64_t            eventPosition,
                                              void*               userData,
                                              OTF2_AttributeList* attributeList,
                                              OTF2_RmaWinRef      win,
                                              uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_RmaGet( OTF2_LocationRef    location,
                               OTF2_TimeStamp      time,
                               uint64_t            eventPosition,
                               void*               userData,
                               OTF2_AttributeList* attributeList,
                               OTF2_RmaWinRef      win,
                               uint32_t            remote,
                               uint64_t            bytes,
                               uint64_t            matchingId );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamBegin( OTF2_LocationRef    locationID,
                                        OTF2_TimeStamp      time,
                                        uint64_t            eventPosition,
                                        void*               userData,
                                        OTF2_AttributeList* attributeList,
                                        OTF2_CommRef        threadTeam );

      static OTF2_CallbackCode
      otf2CallbackComm_ThreadTeamEnd( OTF2_LocationRef    locationID,
                                      OTF2_TimeStamp      time,
                                      uint64_t            eventPosition,
                                      void*               userData,
                                      OTF2_AttributeList* attributeList,
                                      OTF2_CommRef        threadTeam );

      static OTF2_CallbackCode
      otf2Callback_MpiRecv( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            uint64_t            eventPosition,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            sender,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength );

      static OTF2_CallbackCode
      otf2Callback_MpiSend( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            uint64_t            eventPosition,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            receiver,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength );
      
      static OTF2_CallbackCode
      otf2Callback_MpiIrecvRequest( OTF2_LocationRef    location,
                                    OTF2_TimeStamp      time,
                                    uint64_t            eventPosition,
                                    void*               userData,
                                    OTF2_AttributeList* attributeList,
                                    uint64_t            requestID );
      
      static OTF2_CallbackCode
      otf2Callback_MpiIrecv( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            uint64_t            eventPosition,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            sender,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength,
                            uint64_t            requestID );
      
      
      
      static OTF2_CallbackCode
      otf2Callback_MpiIsend( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            uint64_t            eventPosition,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            receiver,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength,
                            uint64_t            requestID );
      
      static OTF2_CallbackCode
      otf2Callback_MpiIsendComplete( OTF2_LocationRef    location,
                                     OTF2_TimeStamp      time,
                                     uint64_t            eventPosition,
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     uint64_t            requestID );

      static OTF2_CallbackCode
      OTF2_EvtReaderCallback_ThreadFork( OTF2_LocationRef locationID,
                                         OTF2_TimeStamp   time,
                                         uint64_t         eventPosition,
                                         void*            userData,
                                         OTF2_AttributeList*
                                         attributeList,
                                         OTF2_Paradigm    paradigm,
                                         uint32_t
                                         numberOfRequestedThreads );

      static OTF2_CallbackCode
      OTF2_EvtReaderCallback_ThreadJoin( OTF2_LocationRef locationID,
                                         OTF2_TimeStamp   time,
                                         uint64_t         eventPosition,
                                         void*            userData,
                                         OTF2_AttributeList*
                                         attributeList,
                                         OTF2_Paradigm    paradigm );

      static OTF2_CallbackCode
      otf2CallbackEnter( OTF2_LocationRef    location,
                         OTF2_TimeStamp      time,
                         uint64_t            eventPosition,
                         void*               userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef      region );

      static OTF2_CallbackCode
      otf2CallbackLeave( OTF2_LocationRef    location,
                         OTF2_TimeStamp      time,
                         uint64_t            eventPosition,
                         void*               userData,
                         OTF2_AttributeList* attributes,
                         OTF2_RegionRef      region );

      /* tell OTF2 what to do after bufferFlush */
      OTF2_FlushCallbacks      flush_callbacks;
      /* callbacks to support parallel writing */
      OTF2_CollectiveCallbacks coll_callbacks;
  };

 }

}
