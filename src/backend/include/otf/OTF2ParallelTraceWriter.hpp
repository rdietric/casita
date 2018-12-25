/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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

#include "OTF2DefinitionHandler.hpp"
#include "AnalysisEngine.hpp"
#include "AnalysisMetric.hpp"

namespace casita
{
 namespace io
 {
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
      
      //<! set of metrics for a program region
      typedef struct
      {
        uint32_t functionId;
        uint32_t numInstances;
        uint32_t numUnifyStreams;
        uint64_t totalDuration;
        uint64_t totalDurationOnCP;
        uint64_t waitingTime;
        double   blame4[ REASON_NUMBER ]; //index is blame reason
        double   totalBlame;
        double   blameOnCP;
      } ActivityGroup;
      
      // key: OTF2 region reference (function ID), value: activity group
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

      OTF2ParallelTraceWriter( AnalysisEngine*        analysis, 
                               OTF2DefinitionHandler* defHandler );
      
      virtual
      ~OTF2ParallelTraceWriter();

      void
      open();

      void
      close();
      
      void
      reset();
      
      void
      clearOpenEdges();
      
      void
      finalizeStreams();
      
      void
      writeDeviceIdleDefinitions( void );
      
      /**
       * Write definitions for self-defined (analysis) metrics to output trace file.
       */
      void
      writeAnalysisMetricDefinitions( void );

      void
      setupGlobalEvtReader();
      
      uint64_t
      writeLocations( const uint64_t eventsToRead );
      
      void
      setupEventReader( uint64_t streamId );
      
      void
      writeMetricStreams();

      ActivityGroupMap*
      getActivityGroupMap()
      {
        return &activityGroupMap;
      }
    private:
      
      AnalysisEngine* analysis;
      
      Statistics* statistics;
      
      OTF2DefinitionHandler* defHandler;
      
      uint32_t mpiRank, mpiSize;

      MPI_Comm commGroup;
      
      bool writeToFile;
      
      // maps OTF2 region references to activity groups to collect a global profile
      ActivityGroupMap activityGroupMap;
      
      //!< pointer to the table of available counters
      AnalysisMetric* cTable; 
      
      //!< ticks per nano second (timer resolution (ticks per second) divided by nano seconds)
      double timeConversionFactor;

      //!< maps each process to corresponding evtWriter
      std::map< uint64_t, OTF2_EvtWriter* > evt_writerMap;
      
      // OTF2 handles
      OTF2_Archive*         otf2Archive;
      OTF2_GlobalDefWriter* otf2GlobalDefWriter;
      OTF2_Reader*          otf2Reader;
      OTF2_GlobalEvtReader* otf2GlobalEventReader;
      //OTF2_AttributeList*   attributes;

      //!< region reference for device idle
      uint32_t devIdleRegRef;
      
      //!< region reference for device compute idle
      uint32_t devComputeIdleRegRef;

      void
      copyGlobalDefinitions();
      
      void
      registerEventCallbacks();
      
      //!< < metric ID, metric value >
      typedef std::map< MetricType, uint64_t > CounterMap;

      void
      updateActivityGroupMap( OTF2Event event, bool evtOnCP, 
                              uint64_t waitingTime, double blame,
                              bool graphNodesAvailable );
      
      void
      updateActivityGroupMap( OTF2Event event, bool evtOnCP, 
                              uint64_t waitingTime, double blame, 
                              BlameMap* blameMap, bool graphNodesAvailable );

      double
      computeBlame( OTF2Event event );
      
      double
      computeBlameMap ( OTF2Event event, BlameMap* blameMap );
      
      void
      writeEventsWithWaitingTime( OTF2Event event, 
                                  OTF2_AttributeList* attributes, 
                                  uint64_t waitingTime );
      
      void
      writeCriticalPathMetric( OTF2Event event, bool graphNodesAvailable );
      
      void
      writeBlameMetric( OTF2Event event, double blame );
      
      ///// \todo: works only for a single device per MPI rank ////

      //<! used to detect offloading idle
      int deviceRefCount;
      uint64_t lastIdleStart;
      uint64_t firstOffloadApiEvtTime;
      uint64_t lastOffloadApiEvtTime;

      //<! used to detect offloading compute idle
      int deviceComputeRefCount;
      uint64_t lastComputeIdleStart;
      
      //<! time when data host-device transfers start (to acquire total communication time=
      uint64_t transferStart;
      
      //<! consecutive device communication 
      // \todo: does not work for concurrent communication
      uint64_t lastDeviceComTaskEnterTime;
      bool     currentDeviceComTaskH2D;
      bool     previousDeviceComTaskH2D;
      size_t   deviceConsecutiveComSDCount; // communication with same direction
      size_t   deviceConsecutiveComCount;
      
      //////////////////////////////////////////////////////////////

      void
      handleFinalDeviceIdleLeave();

      void
      handleDeviceTaskEnter( uint64_t time, DeviceStream* devStream, 
                             bool isCompute = false, bool isH2D = false );

      void
      handleDeviceTaskLeave( uint64_t time, DeviceStream* devStream, 
                             bool isCompute = false  );
     
      void
      processNextEvent( OTF2Event event, OTF2_AttributeList* attributes );

      // needed to get out edges for blame distribution
      Graph* graph;
      
      typedef std::list< Edge* > OpenEdgesList;

      // per location information
      typedef struct
      {
        EventStream::SortedGraphNodeList::iterator currentNodeIter;
        
        EventStream* stream;
        
        bool isFilterOn;
        
        //!< save last written critical path counter to avoid writing of unused counter records
        bool lastWrittenCpValue;
        
        //!< Keep track if process is currently on critical path.
        //!< This is necessary to write counter values correctly.
        bool onCriticalPath;
        
        //!< Keep track of activity stack
        std::stack< OTF2_RegionRef > activityStack;
        
        //!< Store last event time (necessary to calculate metric values correctly)
        uint64_t lastEventTime;
        
        /* Keep track of edges that exist between past and future nodes.
         * Necessary to distribute correct blame to CPU nodes.
         * When processing internal nodes, all out-edges are opened.
         * Blame was assigned to these edges during analysis, now it is
         * distributed to CPU nodes between internal nodes. */
        OpenEdgesList openEdges;
      }StreamStatus;
      
      typedef std::map < uint64_t, StreamStatus > StreamStatusMap;
      StreamStatusMap streamStatusMap;

      typedef std::map< uint64_t, uint64_t > LocationParentMap;
      LocationParentMap locationParentMap;


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

      //<! tell OTF2 what to do after bufferFlush
      OTF2_FlushCallbacks      flush_callbacks;
      
      //<! callbacks to support parallel writing
      OTF2_CollectiveCallbacks coll_callbacks;
  };

 }

}
