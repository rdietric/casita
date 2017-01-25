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
#include "EventStreamGroup.hpp"
#include "graph/Graph.hpp"

namespace casita
{
 namespace io
 {
    enum ProcessGroup
    {
      PG_HOST = 1, PG_DEVICE, PG_DEVICE_NULL
    };

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
      
      // key: OTF2 region reference, value: activity group
      typedef std::map< uint32_t, ActivityGroup > ActivityGroupMap;

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

      OTF2ParallelTraceWriter( uint32_t        mpiRank,
                               uint32_t        mpiSize,
                               const char*     originalFilename,
                               bool            writeToFile,
                               AnalysisMetric* metrics );
      virtual
      ~OTF2ParallelTraceWriter();

      void
      open( const std::string otfFilename, uint32_t maxFiles );

      void
      close();
      
      void
      reset();
      
      /**
       * Write definitions for self-defined (analysis) metrics to output trace file.
       */
      void
      writeAnalysisMetricDefinitions( void );

      void
      setupGlobalEvtReader();
      
      uint64_t
      writeLocations( EventStreamGroup::EventStreamList& streams,
                      Graph*       graph,
                      uint64_t     eventsToRead );
      
      void
      setupEventReader( uint64_t streamId );
      
      bool
      writeStream( EventStream* stream,
                   Graph*       graph,
                   uint64_t*    events_read );
      
      void
      writeMetricStreams();

      std::string
      getRegionName( const OTF2_RegionRef regionRef ) const;
      
      ActivityGroupMap*
      getActivityGroupMap()
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
      
      bool writeToFile;

    private:
      
      uint32_t mpiRank, mpiSize;
      
      MPI_Comm commGroup;
      
      // maps OTF2 region references to activity groups to collect a global profile
      ActivityGroupMap activityGroupMap;
      
      //!< pointer to the table of available counters
      AnalysisMetric* cTable; 
      
      uint64_t timerResolution;
      uint64_t timerOffset;
      
      double
      getRealTime( uint64_t time );
      
      //!< counter to assign IDs to string definitions
      uint64_t counterForStringDefinitions;
      
      //!< regionReference for internal Fork/Join
      uint32_t ompForkJoinRef;

      std::string outputFilename, originalFilename, pathToFile;

      //!< maps each process to corresponding evtWriter
      std::map< uint64_t, OTF2_EvtWriter* > evt_writerMap;
      
      // OTF2 handles
      OTF2_Archive*         otf2Archive;
      OTF2_GlobalDefWriter* otf2GlobalDefWriter;
      OTF2_Reader*          otf2Reader;
      OTF2_GlobalEvtReader* otf2GlobalEventReader;
      //OTF2_AttributeList*   attributes;

      //!< maps OTF2 IDs to strings (global definitions)
      std::map< uint32_t, const char* > idStringMap;

      //!< < metric ID, metric value >
      typedef std::map< MetricType, uint64_t > CounterMap;
      typedef std::map< uint64_t, CounterMap > CounterMapMap;

      //!< < event location, stack of region references >
      typedef std::map< uint64_t, std::stack< OTF2_RegionRef > > ActivityStackMap;
      typedef std::map< uint64_t, uint64_t > TimeMap;
      typedef std::list< Edge* > OpenEdgesList;
      typedef std::map < uint64_t, OpenEdgesList > OpenEdgesMap;
      typedef std::map< uint64_t, bool > BooleanMap;

      void
      copyGlobalDefinitions();
      
      void
      registerEventCallbacks();

      void
      updateActivityGroupMap( OTF2Event event, CounterMap& counters );

      uint64_t
      computeCPUEventBlame( OTF2Event event );
      
      void
      writeEventsWithAttributes( OTF2Event event, OTF2_AttributeList* attributes, 
                                 CounterMap& counters );
      
      void
      writeEventsWithCounters( OTF2Event event, CounterMap& counters, 
                               bool writeEvents );

      void
      processNextEvent( OTF2Event event, OTF2_AttributeList* attributes );
      
      void
      clearOpenEdges();
      
      typedef std::map < uint64_t, EventStream::SortedGraphNodeList::iterator > NodeListIterMap;
      typedef std::map < uint64_t, EventStream* > IdStreamMap;
      
      NodeListIterMap currentNodeIterMap;
      IdStreamMap currentStreamMap;

      Graph* graph;
      
      //!< save last counter values to avoid writing of unused counter records
      CounterMapMap lastMetricValues;
      
#if defined(BLAME_COUNTER_FALSE)
      //!< < event location, stack of counter values >
      typedef std::map< uint64_t, std::stack< CounterMap* > > CounterStackMap;
      //!< activity value stack map < event.location, stack of CounterMaps >
      CounterStackMap leaveCounterStack;
#endif

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
      //OpenEdgesList openEdges;
      OpenEdgesMap openEdgesMap;
      
      // per location information
      /*typedef struct
      {
        EventStream::SortedGraphNodeList::iterator currentNodeIter;
        EventStream* currentStream;
        CounterMap lastMetricValues;
        std::stack< OTF2_RegionRef > activityStack;
        uint64_t lastEventTime;
        OpenEdgesList openEdges;
      }StreamStatus;
      
      std::map < uint64_t, StreamStatus > streamStatusMap;*/
      
      /* Keep track if process is currently on critical path
       * -> necessary to write counter values correctly */
      BooleanMap processOnCriticalPath;

      /** Tells if a stream is a device stream.
       * (necessary to find out if an event is mapped by an internal node. */
      //BooleanMap deviceStreamMap;
      
      typedef std::map< uint64_t, uint64_t > LocationParentMap;
      LocationParentMap locationParentMap;
      
      //std::vector< uint64_t > metricStreamVector;
      
      //!< maps OTF2 region IDs to OTF2 string reference (key: OTF2 region ID)
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
      otf2GlobalDefReaderCallback_MetricMember(  void*                userData,
                                                 OTF2_MetricMemberRef self,
                                                 OTF2_StringRef       name,
                                                 OTF2_StringRef       description,
                                                 OTF2_MetricType      metricType,
                                                 OTF2_MetricMode      metricMode,
                                                 OTF2_Type            valueType,
                                                 OTF2_Base            base,
                                                 int64_t              exponent,
                                                 OTF2_StringRef       unit );
      
      static OTF2_CallbackCode
      otf2GlobalDefReaderCallback_MetricClass(
                                  void*                       userData,
                                  OTF2_MetricRef              self,
                                  uint8_t                     numberOfMetrics,
                                  const OTF2_MetricMemberRef* metricMembers,
                                  OTF2_MetricOccurrence       metricOccurrence,
                                  OTF2_RecorderKind           recorderKind );
      
      static OTF2_CallbackCode
      otf2GlobalDefReaderCallback_MetricInstance(  void*            userData,
                                                   OTF2_MetricRef   self,
                                                   OTF2_MetricRef   metricClass,
                                                   OTF2_LocationRef recorder,
                                                   OTF2_MetricScope metricScope,
                                                   uint64_t         scope );

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
      
      static OTF2_CallbackCode
      OTF2_DefReaderCallback_MetricMember( 
                                           void*                userData,
                                           OTF2_MetricMemberRef self,
                                           OTF2_StringRef       name,
                                           OTF2_StringRef       description,
                                           OTF2_MetricType      metricType,
                                           OTF2_MetricMode      metricMode,
                                           OTF2_Type            valueType,
                                           OTF2_Base            base,
                                           int64_t              exponent,
                                           OTF2_StringRef       unit );
      
      static OTF2_CallbackCode
      OTF2_DefReaderCallback_MetricClass( 
                                          void*                       userData,
                                          OTF2_MetricRef              self,
                                          uint8_t                     numberOfMetrics,
                                          const OTF2_MetricMemberRef* metricMembers,
                                          OTF2_MetricOccurrence       metricOccurrence,
                                          OTF2_RecorderKind           recorderKind );

      /* callbacks for comm/enter/leave/fork/join events */
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
      otf2Callback_MpiSend( OTF2_LocationRef    locationID,
                            OTF2_TimeStamp      time,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            receiver,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength );
      
      static OTF2_CallbackCode
      otf2Callback_MpiIrecvRequest( OTF2_LocationRef    location,
                                    OTF2_TimeStamp      time,
                                    void*               userData,
                                    OTF2_AttributeList* attributeList,
                                    uint64_t            requestID );
      
      static OTF2_CallbackCode
      otf2Callback_MpiIrecv( OTF2_LocationRef   locationID,
                            OTF2_TimeStamp      time,
                            void*               userData,
                            OTF2_AttributeList* attributeList,
                            uint32_t            sender,
                            OTF2_CommRef        communicator,
                            uint32_t            msgTag,
                            uint64_t            msgLength,
                            uint64_t            requestID );
      
      
      
      static OTF2_CallbackCode
      otf2Callback_MpiIsend( OTF2_LocationRef   locationID,
                            OTF2_TimeStamp      time,
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
                                     void*               userData,
                                     OTF2_AttributeList* attributeList,
                                     uint64_t            requestID );

      static OTF2_CallbackCode
      otf2EvtCallbackThreadFork(  OTF2_LocationRef locationID,
                                  OTF2_TimeStamp   time,
                                  void*            userData,
                                  OTF2_AttributeList* attributeList,
                                  OTF2_Paradigm    paradigm,
                                  uint32_t         numberOfRequestedThreads );

      static OTF2_CallbackCode
      otf2EvtCallbackThreadJoin( OTF2_LocationRef locationID,
                                 OTF2_TimeStamp   time,
                                 void*            userData,
                                 OTF2_AttributeList* attributeList,
                                 OTF2_Paradigm    paradigm );

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
      otf2CallbackMetric( 
                            OTF2_LocationRef        location,
                            OTF2_TimeStamp          time,
                            void*                   userData,
                            OTF2_AttributeList*     attributeList,
                            OTF2_MetricRef          metric,
                            uint8_t                 numberOfMetrics,
                            const OTF2_Type*        typeIDs,
                            const OTF2_MetricValue* metricValues );

      /* tell OTF2 what to do after bufferFlush */
      OTF2_FlushCallbacks      flush_callbacks;
      /* callbacks to support parallel writing */
      OTF2_CollectiveCallbacks coll_callbacks;
  };

 }

}
