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

#include <mpi.h>
#include <cmath>
#include <iostream>
#include <stdlib.h>
#include <inttypes.h>

/* following adjustments necessary to use MPI_Collectives */
#define OTF2_MPI_UINT64_T MPI_UNSIGNED_LONG
#define OTF2_MPI_INT64_T MPI_LONG

#include <otf2/OTF2_MPI_Collectives.h>
#include <boost/filesystem.hpp>

#include "graph/EventNode.hpp"
#include "CounterTable.hpp"
#include "common.hpp"
#include "EventStream.hpp"
#include "FunctionTable.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/OTF2TraceReader.hpp"
#include "otf/OTF2ParallelTraceWriter.hpp"
#include "GraphEngine.hpp"

using namespace casita;
using namespace casita::io;

#define OTF2_CHECK( cmd ) \
  { \
   int _status = cmd; \
   if ( _status ) { \
     throw RTException( "OTF2 command '%s' returned error %d", #cmd, _status );} \
  }

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) { \
      throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
  }

OTF2_FlushType
preFlush( void* userData, OTF2_FileType fileType,
          OTF2_LocationRef location, void* callerData, bool final )
{
  return OTF2_FLUSH;
}

OTF2_TimeStamp
postFlush( void* userData, OTF2_FileType fileType,
           OTF2_LocationRef location )
{
  return 0;
}

static inline size_t
otf2_mpi_type_to_size( OTF2_Type type )
{
  switch ( type )
  {
    case OTF2_TYPE_UINT8:
    case OTF2_TYPE_INT8:
      return 1;
    case OTF2_TYPE_UINT16:
    case OTF2_TYPE_INT16:
      return 2;
    case OTF2_TYPE_UINT32:
    case OTF2_TYPE_INT32:
    case OTF2_TYPE_FLOAT:
      return 4;
    case OTF2_TYPE_UINT64:
    case OTF2_TYPE_INT64:
    case OTF2_TYPE_DOUBLE:
      return 8;
    default:
      return 0;
  }
}

static inline MPI_Datatype
otf2_to_mpi_type( OTF2_Type type )
{

  switch ( type )
  {
    case OTF2_TYPE_UINT8:
    case OTF2_TYPE_INT8:
      return MPI_CHAR;
    case OTF2_TYPE_UINT16:
    case OTF2_TYPE_INT16:
      return MPI_SHORT;
    case OTF2_TYPE_UINT32:
    case OTF2_TYPE_INT32:
      return MPI_INTEGER;
    case OTF2_TYPE_FLOAT:
      return MPI_FLOAT;
    case OTF2_TYPE_UINT64:
      return MPI_UNSIGNED_LONG_LONG;
    case OTF2_TYPE_INT64:
      return MPI_LONG_LONG;
    case OTF2_TYPE_DOUBLE:
      return MPI_DOUBLE;
    default:
      return 0;
  }

}

OTF2ParallelTraceWriter::OTF2ParallelTraceWriter( uint32_t             mpiRank,
                                                  uint32_t             mpiSize,
                                                  const char*          originalFilename,
                                                  bool                 writeToFile,
                                                  std::set< uint32_t > ctrIdSet )
  :
    IParallelTraceWriter( mpiRank, mpiSize ),
    writeToFile( writeToFile ),
    global_def_writer( NULL ),
    processNodes( NULL ),
    iter( NULL ),
    enableWaitStates( false ),
    verbose( false ),
    isFirstProcess( true ),
    graph( NULL ),
    cTable( NULL )
{
  outputFilename.assign( "" );
  pathToFile.assign( "" );
  this->originalFilename.assign( originalFilename );

  this->ctrIdSet = ctrIdSet;

  flush_callbacks.otf2_post_flush = postFlush;
  flush_callbacks.otf2_pre_flush = preFlush;

  commGroup      = MPI_COMM_WORLD;
}

OTF2ParallelTraceWriter::~OTF2ParallelTraceWriter( )
{

}

void
OTF2ParallelTraceWriter::open( const std::string otfFilename, uint32_t maxFiles,
                               uint32_t numStreams )
{
  boost::filesystem::path boost_path     = otfFilename;
  boost::filesystem::path boost_filename = otfFilename;

  outputFilename = boost::filesystem::change_extension(
    boost_filename.filename( ), "" ).string( );
  pathToFile     = boost_path.remove_filename( ).string( );

  UTILS_DBG_MSG( mpiRank == 0, "[%u] FILENAME: '%s' PATH: '%s'",
                 mpiRank, outputFilename.c_str( ), pathToFile.c_str( ) );

  if ( writeToFile )
  {
    /* remove trace dir */
    if ( boost::filesystem::exists( pathToFile + std::string( "/" ) +
                                    outputFilename ) )
    {
      boost::filesystem::remove_all( pathToFile + std::string(
                                       "/" ) + outputFilename );
    }

    /* remove trace files */
    if ( boost::filesystem::exists( otfFilename ) )
    {
      boost::filesystem::remove( otfFilename );
      boost::filesystem::remove(
        boost::filesystem::change_extension( otfFilename, "def" ) );
    }

    /* open new otf2 file */
    archive = OTF2_Archive_Open( pathToFile.c_str( ),
                                 outputFilename.c_str( ),
                                 OTF2_FILEMODE_WRITE, 1024 * 1024, 4 * 1024 *
                                 1024, OTF2_SUBSTRATE_POSIX,
                                 OTF2_COMPRESSION_NONE );

    OTF2_Archive_SetFlushCallbacks( archive, &flush_callbacks, NULL );

    /* set collective callbacks to write trace in parallel */
    OTF2_MPI_Archive_SetCollectiveCallbacks( archive, commGroup, MPI_COMM_NULL );
  }

  timerOffset     = 0;
  timerResolution = 0;
  counterForStringDefinitions = 0;
  counterForMetricInstanceId = 0;

  reader          = OTF2_Reader_Open( originalFilename.c_str( ) );

  OTF2_MPI_Reader_SetCollectiveCallbacks( reader, commGroup );

  if ( !reader )
  {
    throw RTException( "Failed to open OTF2 trace file %s",
                       originalFilename.c_str( ) );
  }

  copyGlobalDefinitions( );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

  /* open event to start creating event files for each location */
  if ( writeToFile )
  {
    OTF2_Archive_OpenEvtFiles( archive );
  }

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
}

void
OTF2ParallelTraceWriter::close( )
{
  /* close all opened event writer */
  if ( writeToFile )
  {
    for ( std::map< uint64_t, OTF2_EvtWriter* >::iterator iter =
            evt_writerMap.begin( );
          iter != evt_writerMap.end( ); iter++ )
    {
      OTF2_Archive_CloseEvtWriter( archive, iter->second );
    }

    if ( mpiRank == 0 )
    {
      OTF2_Archive_CloseDefFiles( archive );
    }

    OTF2_Archive_CloseEvtFiles( archive );

    /* close global writer */
    OTF2_CHECK( OTF2_Archive_Close( archive ) );

    OTF2_Reader_Close( reader );
  }
}

/*
 * Copy definitions from original trace to new one
 */
void
OTF2ParallelTraceWriter::copyGlobalDefinitions( )
{

  if ( mpiRank == 0 && writeToFile )
  {
    global_def_writer = OTF2_Archive_GetGlobalDefWriter( archive );
  }

  OTF2_GlobalDefReader* global_def_reader =
    OTF2_Reader_GetGlobalDefReader(
      reader );

  OTF2_GlobalDefReaderCallbacks* global_def_callbacks =
    OTF2_GlobalDefReaderCallbacks_New( );

  if ( mpiRank == 0 )
  {
    OTF2_GlobalDefReaderCallbacks_SetAttributeCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Attribute );
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_String );
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_ClockProperties );
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Location );
    OTF2_GlobalDefReaderCallbacks_SetGroupCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Group );
    OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_LocationGroup );
    OTF2_GlobalDefReaderCallbacks_SetCommCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Comm );
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Region );
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_SystemTreeNode );
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodePropertyCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty );
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeDomainCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain );
    OTF2_GlobalDefReaderCallbacks_SetRmaWinCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_RmaWin );
  }
  else
  {
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Location );
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_ClockProperties );
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_String );
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
      global_def_callbacks,
      &
      OTF2_GlobalDefReaderCallback_Region );
  }

  /* register callbacks */
  OTF2_Reader_RegisterGlobalDefCallbacks( reader,
                                          global_def_reader,
                                          global_def_callbacks,
                                          this );

  OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );

  uint64_t definitions_read = 0;
  /* read definitions */
  OTF2_Reader_ReadAllGlobalDefinitions( reader,
                                        global_def_reader,
                                        &definitions_read );

  UTILS_DBG_MSG( mpiRank == 0, "[%u] Read and wrote %lu definitions",
                 mpiRank, definitions_read );

}

/*
 * OTF2: create event writer for this process
 */
void
OTF2ParallelTraceWriter::writeDefProcess( uint64_t id, uint64_t parentId,
                                          const char* name, ProcessGroup pg )
{
  /* create writer for this process */
  if ( writeToFile )
  {
    OTF2_EvtWriter* evt_writer = OTF2_Archive_GetEvtWriter(
      archive,
      OTF2_UNDEFINED_LOCATION );
    OTF2_CHECK( OTF2_EvtWriter_SetLocationID( evt_writer, id ) );
    evt_writerMap[id] = evt_writer;
  }

  OTF2_Reader_SelectLocation( reader, id );
}

/*
 * Write self-defined metrics/counter to new trace file
 */
void
OTF2ParallelTraceWriter::writeDefCounter( uint32_t    otfId,
                                          const char* name,
                                          int         properties )
{
  if ( mpiRank == 0 && writeToFile )
  {
    uint32_t id = otfId;

    /* 1) write String definition */
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( global_def_writer,
                                                  counterForStringDefinitions,
                                                  name ) );

    /* 2) Write metrics member and class definition */
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteMetricMember( global_def_writer, id,
                                                        counterForStringDefinitions,
                                                        counterForStringDefinitions,
                                                        OTF2_METRIC_TYPE_USER,
                                                        OTF2_METRIC_ABSOLUTE_POINT,
                                                        OTF2_TYPE_UINT64,
                                                        OTF2_BASE_DECIMAL, 0, 0 ) );

    OTF2_CHECK( OTF2_GlobalDefWriter_WriteMetricClass( global_def_writer, id, 1,
                                                       &id,
                                                       OTF2_METRIC_ASYNCHRONOUS,
                                                       OTF2_RECORDER_KIND_ABSTRACT ) );

    counterForStringDefinitions++;
  }
}

/*
 * Read all events from original trace for this process and combine them with the
 * events and counter values from analysis
 */
void
OTF2ParallelTraceWriter::writeProcess( uint64_t                          processId,
                                       EventStream::SortedGraphNodeList* nodes,
                                       bool                              waitStates,
                                       GraphNode*                        pLastGraphNode,
                                       bool                              verbose,
                                       CounterTable*                     ctrTable,
                                       Graph*                            graph )
{
  assert( nodes );

  processNodes     = nodes;
  enableWaitStates = waitStates;
  iter = processNodes->begin( );
  this->verbose    = verbose;
  this->graph      = graph;
  cTable           = ctrTable;
  processOnCriticalPath[processId] = false;

  UTILS_DBG_MSG( verbose, "[%u] Start writing for process %lu", mpiRank, processId );

  if ( isFirstProcess )
  {
    OTF2_Reader_OpenEvtFiles( reader );
    OTF2_Reader_OpenDefFiles( reader );
    isFirstProcess = false;
  }

  OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, processId );
  uint64_t def_reads         = 0;
  OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
  OTF2_Reader_CloseDefReader( reader, def_reader );

  OTF2_Reader_GetEvtReader( reader, processId );

  OTF2_EvtReader* evt_reader = OTF2_Reader_GetEvtReader(
    reader,
    processId );

  OTF2_EvtReaderCallbacks* event_callbacks = OTF2_EvtReaderCallbacks_New( );
  OTF2_EvtReaderCallbacks_SetEnterCallback( event_callbacks, &otf2CallbackEnter );
  OTF2_EvtReaderCallbacks_SetLeaveCallback( event_callbacks, &otf2CallbackLeave );
  OTF2_EvtReaderCallbacks_SetThreadForkCallback(
    event_callbacks, &OTF2_EvtReaderCallback_ThreadFork );
  OTF2_EvtReaderCallbacks_SetThreadJoinCallback(
    event_callbacks, &OTF2_EvtReaderCallback_ThreadJoin );
  OTF2_EvtReaderCallbacks_SetMpiCollectiveBeginCallback(
    event_callbacks, &otf2CallbackComm_MpiCollectiveBegin );
  OTF2_EvtReaderCallbacks_SetMpiCollectiveEndCallback(
    event_callbacks, &otf2CallbackComm_MpiCollectiveEnd );
  OTF2_EvtReaderCallbacks_SetMpiRecvCallback( event_callbacks,
                                              &otf2Callback_MpiRecv );
  OTF2_EvtReaderCallbacks_SetMpiSendCallback( event_callbacks,
                                              &otf2Callback_MpiSend );
  OTF2_EvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(
    event_callbacks, &otf2CallbackComm_RmaOpCompleteBlocking );
  OTF2_EvtReaderCallbacks_SetRmaWinCreateCallback(
    event_callbacks, &otf2CallbackComm_RmaWinCreate );
  OTF2_EvtReaderCallbacks_SetRmaWinDestroyCallback(
    event_callbacks, &otf2CallbackComm_RmaWinDestroy );
  OTF2_EvtReaderCallbacks_SetRmaGetCallback( event_callbacks,
                                             &otf2CallbackComm_RmaGet );
  OTF2_EvtReaderCallbacks_SetRmaPutCallback( event_callbacks,
                                             &otf2CallbackComm_RmaPut );
  OTF2_EvtReaderCallbacks_SetThreadTeamBeginCallback(
    event_callbacks, &otf2CallbackComm_ThreadTeamBegin );
  OTF2_EvtReaderCallbacks_SetThreadTeamEndCallback(
    event_callbacks, &otf2CallbackComm_ThreadTeamEnd );

  OTF2_Reader_RegisterEvtCallbacks( reader,
                                    evt_reader,
                                    event_callbacks,
                                    this );
  OTF2_EvtReaderCallbacks_Delete( event_callbacks );

  uint64_t events_read = 0;

  /* returns 0 if successfull, >0 otherwise */
  if ( OTF2_Reader_ReadAllLocalEvents( reader, evt_reader, &events_read ) )
  {
    throw RTException( "Failed to read OTF2 events" );
  }

  OTF2_Reader_CloseEvtReader( reader, evt_reader );
}

void
OTF2ParallelTraceWriter::updateActivityGroupMap( OTF2Event event, CounterMap& counters )
{
  /* add function to list if not present yet */
  if ( activityGroupMap.find( event.regionRef ) == activityGroupMap.end( ) )
  {
    activityGroupMap[event.regionRef].functionId   = event.regionRef;
    activityGroupMap[event.regionRef].numInstances = 0;
  }

  /* for each enter event, increase the number of instances found */
  if ( event.type == OTF2_EVT_ENTER )
  {
    activityGroupMap[event.regionRef].numInstances++;
  }

  ActivityStackMap::const_iterator activityIter = activityStack.find( event.location );

  /* add time to current function on stack */
  if ( activityIter != activityStack.end( ) && activityIter->second.size( ) > 0 )
  {
    uint32_t currentActivity = activityIter->second.top( );
    uint64_t timeDiff        = event.time - lastEventTime[event.location];

    activityGroupMap[currentActivity].totalDuration     += timeDiff;

    uint64_t cpValue         = counters[cTable->getCtrId( CTR_CRITICALPATH )];

    activityGroupMap[currentActivity].totalDurationOnCP +=
      ( processOnCriticalPath[event.location] && ( cpValue != 0 ) ) ? timeDiff : 0;

    /* log if this process is currently on the critical path */
    processOnCriticalPath[event.location] = cpValue;

    activityGroupMap[currentActivity].totalBlame        += counters[cTable->getCtrId( CTR_BLAME )];
  }
}

uint64_t
OTF2ParallelTraceWriter::computeCPUEventBlame( OTF2Event event )
{
  uint64_t totalBlame = 0;
  uint64_t timeDiff   = event.time - lastEventTime[event.location];

  /* iterate over all open edges (if any) */
  for ( OpenEdgesList::iterator edgeIter = openEdges.begin( );
        edgeIter != openEdges.end( ); )
  {
    OpenEdgesList::iterator currentIter = edgeIter;
    OpenEdgesList::iterator nextIter    = ++edgeIter;

    Edge* edge = *currentIter;

    if ( ( edge->getDuration( ) > 0 ) &&
         ( edge->getEndNode( )->getTime( ) + timerOffset > event.time ) )
    {
      totalBlame += (double)( edge->getCPUBlame( ) ) *
                    (double)timeDiff /
                    (double)( edge->getDuration( ) );
    }
    else
    {
      /* erase edge if event is after its end node */
      openEdges.erase( currentIter );
    }

    edgeIter = nextIter;
  }

  return totalBlame;
}

void
OTF2ParallelTraceWriter::writeEvent( OTF2Event event, CounterMap& counters )
{
  UTILS_ASSERT( evt_writerMap.find( event.location ) != evt_writerMap.end( ),
                "Could not find OTF2 event writer for location" );

  OTF2_EvtWriter* evt_writer = evt_writerMap[event.location];

  if ( event.type == OTF2_EVT_ENTER )
  {
    OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writer, NULL, event.time, event.regionRef ) );
  }
  else
  {
    OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writer, NULL, event.time, event.regionRef ) );
  }

  for ( CounterMap::const_iterator iter = counters.begin( );
        iter != counters.end( ); ++iter )
  {
    const uint32_t   ctrId = iter->first;
    OTF2_Type        type  = OTF2_TYPE_UINT64;
    OTF2_MetricValue value;
    value.unsigned_int = iter->second;

    OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                       ctrId, 1, &type, &value ) );
  }
}

void
OTF2ParallelTraceWriter::processNextEvent( OTF2Event event )
{
  /* Skip threadFork/Join (also skips first inserted processNode that
   * is not in original trace)
   */
  while ( ( iter != processNodes->end( ) ) && ( *iter )->isOMPParallelRegion( ) )
  {
    UTILS_DBG_MSG( verbose, "[%u] Skipping %s", mpiRank, ( *iter )->getUniqueName( ).c_str( ) );
    iter++;
  }

  /* test if this is an internal node or a CPU event */
  FunctionDescriptor desc;
  const bool isDeviceStream = deviceStreamMap[event.location];
  bool mapsInternalNode     = FunctionTable::getAPIFunctionType(
    idStringMap[regionNameIdList[event.regionRef]], &desc, isDeviceStream, false );

  /* non-internal counter values for this event */
  CounterMap tmpCounters;

  if ( mapsInternalNode )
  {
    GraphNode* currentNode = *iter;

    /* preprocess current internal node */
    if ( graph->hasOutEdges( currentNode ) )
    {
      const Graph::EdgeList& edges = graph->getOutEdges( currentNode );
      for ( Graph::EdgeList::const_iterator edgeIter = edges.begin( );
            edgeIter != edges.end( ); edgeIter++ )
      {
        Edge* edge = *edgeIter;
        if ( edge->getCPUBlame( ) > 0 )
        {
          openEdges.push_back( edge );
        }
      }
    }

    /* copy node counter values to tmp counter map */
    const CounterTable::CtrIdSet& ctrIdSet = cTable->getAllCounterIDs( );
    for ( CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin( );
          ctrIter != ctrIdSet.end( ); ++ctrIter )
    {
      const uint32_t ctrId         = *ctrIter;
      const CtrTableEntry* counter = cTable->getCounter( ctrId );

      if ( !counter->isInternal )
      {
        tmpCounters[ctrId] = currentNode->getCounter( ctrId, NULL );
      }
    }

    ++iter;
  }
  else
  {
    /* compute counters for that event */
    if ( iter != processNodes->end( ) )
    {
      const uint32_t ctrIdBlame     = cTable->getCtrId( CTR_BLAME );
      const uint32_t ctrIdWaitState = cTable->getCtrId( CTR_WAITSTATE );
      const uint32_t ctrIdCritPath  = cTable->getCtrId( CTR_CRITICALPATH );

      /* compute critical path ctr */
      /* event is on critical path if next internal node is, too */
      tmpCounters[ctrIdCritPath]  = ( *iter )->getCounter( ctrIdCritPath, NULL );

      /* compute blame ctr */
      tmpCounters[ctrIdBlame]     = computeCPUEventBlame( event );

      /* compute waiting time ctr */
      tmpCounters[ctrIdWaitState] = 0;
    }
  }

  /* write event with counters */
  if ( writeToFile )
  {
    writeEvent( event, tmpCounters );
  }

  /* update values in activityGroupMap */
  updateActivityGroupMap( event, tmpCounters );

  /* set last event time for all event types */
  lastEventTime[event.location] = event.time;
}

/*
 * /////////////////////// Callbacks to re-write definition records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 */
OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_ClockProperties( void* userData,
                                                                       uint64_t
                                                                       timerResolution,
                                                                       uint64_t
                                                                       globalOffset,
                                                                       uint64_t
                                                                       traceLength )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  tw->timerOffset     = globalOffset;
  tw->timerResolution = timerResolution;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteClockProperties( tw->
                                                           global_def_writer,
                                                           timerResolution,
                                                           globalOffset,
                                                           traceLength ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_LocationGroup( void* userData,
                                                                     OTF2_LocationGroupRef
                                                                     self,
                                                                     OTF2_StringRef
                                                                     name,
                                                                     OTF2_LocationGroupType
                                                                     locationGroupType,
                                                                     OTF2_SystemTreeNodeRef
                                                                     systemTreeParent )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteLocationGroup( tw->global_def_writer,
                                                         self, name,
                                                         locationGroupType,
                                                         systemTreeParent ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Location( void* userData,
                                                                OTF2_LocationRef
                                                                self,
                                                                OTF2_StringRef
                                                                name,
                                                                OTF2_LocationType
                                                                locationType,
                                                                uint64_t
                                                                numberOfEvents,
                                                                OTF2_LocationGroupRef
                                                                locationGroup )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( locationType == OTF2_LOCATION_TYPE_GPU )
  {
    tw->deviceStreamMap[self] = true;
  }
  else
  {
    tw->deviceStreamMap[self] = false;
  }

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteLocation( tw->global_def_writer, self,
                                                    name, locationType,
                                                    numberOfEvents,
                                                    locationGroup ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Group( void*         userData,
                                                             OTF2_GroupRef self,
                                                             OTF2_StringRef
                                                             name,
                                                             OTF2_GroupType
                                                             groupType,
                                                             OTF2_Paradigm
                                                             paradigm,
                                                             OTF2_GroupFlag
                                                             groupFlags,
                                                             uint32_t
                                                             numberOfMembers,
                                                             const uint64_t*
                                                             members )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {

    OTF2_CHECK( OTF2_GlobalDefWriter_WriteGroup( tw->global_def_writer, self,
                                                 name,
                                                 groupType, paradigm, groupFlags,
                                                 numberOfMembers, members ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Comm( void*          userData,
                                                            OTF2_CommRef   self,
                                                            OTF2_StringRef name,
                                                            OTF2_GroupRef  group,
                                                            OTF2_CommRef   parent )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteComm( tw->global_def_writer, self, name,
                                                group, parent ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_String( void* userData,
                                                              OTF2_StringRef
                                                              self,
                                                              const char*
                                                              string )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  tw->counterForStringDefinitions++;

  tw->idStringMap[self] = string;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {

    OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( tw->global_def_writer, self,
                                                  string ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNode( void* userData,
                                                                      OTF2_SystemTreeNodeRef
                                                                      self,
                                                                      OTF2_StringRef
                                                                      name,
                                                                      OTF2_StringRef
                                                                      className,
                                                                      OTF2_SystemTreeNodeRef
                                                                      parent )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNode( tw->global_def_writer,
                                                          self, name, className,
                                                          parent ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty( void* userData,
                                                                              OTF2_SystemTreeNodeRef
                                                                              systemTreeNode,
                                                                              OTF2_StringRef
                                                                              name,
                                                                              OTF2_StringRef
                                                                              value )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty( tw->
                                                                  global_def_writer,
                                                                  systemTreeNode,
                                                                  name, value ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain( void* userData,
                                                                            OTF2_SystemTreeNodeRef
                                                                            systemTreeNode,
                                                                            OTF2_SystemTreeDomain
                                                                            systemTreeDomain )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeDomain( tw->
                                                                global_def_writer,
                                                                systemTreeNode,
                                                                systemTreeDomain ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Region( void* userData,
                                                              OTF2_RegionRef
                                                              self,
                                                              OTF2_StringRef
                                                              name,
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
                                                              endLineNumber )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  tw->regionNameIdList[self] = name;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteRegion( tw->global_def_writer, self,
                                                  name,
                                                  cannonicalName, description,
                                                  regionRole, paradigm,
                                                  regionFlags, sourceFile,
                                                  beginLineNumber,
                                                  endLineNumber ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Attribute( void*     userData,
                                                                 OTF2_AttributeRef
                                                                 self,
                                                                 OTF2_StringRef
                                                                 name,
                                                                 OTF2_StringRef
                                                                 description,
                                                                 OTF2_Type type )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteAttribute( tw->global_def_writer, self,
                                                     name,
                                                     description, type ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_RmaWin( void*        userData,
                                                              OTF2_RmaWinRef
                                                              self,
                                                              OTF2_StringRef
                                                              name,
                                                              OTF2_CommRef comm )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteRmaWin( tw->global_def_writer, self,
                                                  name,
                                                  comm ) );
  }

  return OTF2_CALLBACK_SUCCESS;

}

/*
 * /////////////////////// Callbacks to re-write enter/leave and communication records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 * Enter and leave callbacks call "processNextNode()" to write node with metrics
 */
OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_MpiCollectiveEnd( OTF2_LocationRef locationID,
                                                            OTF2_TimeStamp   time,
                                                            uint64_t
                                                            eventPosition,
                                                            void*            userData,
                                                            OTF2_AttributeList*
                                                            attributeList,
                                                            OTF2_CollectiveOp
                                                            collectiveOp,
                                                            OTF2_CommRef
                                                            communicator,
                                                            uint32_t         root,
                                                            uint64_t         sizeSent,
                                                            uint64_t
                                                            sizeReceived )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiCollectiveEnd( tw->evt_writerMap[locationID],
                                                 attributeList, time,
                                                 collectiveOp, communicator, root,
                                                 sizeSent, sizeReceived ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_MpiCollectiveBegin( OTF2_LocationRef locationID,
                                                              OTF2_TimeStamp
                                                              time,
                                                              uint64_t
                                                              eventPosition,
                                                              void*            userData,
                                                              OTF2_AttributeList
                                                              *                attributeList )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiCollectiveBegin( tw->evt_writerMap[locationID],
                                                   attributeList, time ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaWinCreate( OTF2_LocationRef location,
                                                        OTF2_TimeStamp   time,
                                                        uint64_t         eventPosition,
                                                        void*            userData,
                                                        OTF2_AttributeList*
                                                        attributeList,
                                                        OTF2_RmaWinRef   win )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaWinCreate( tw->evt_writerMap[location],
                                             attributeList, time,
                                             win ) );
  }
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaWinDestroy( OTF2_LocationRef location,
                                                         OTF2_TimeStamp   time,
                                                         uint64_t         eventPosition,
                                                         void*            userData,
                                                         OTF2_AttributeList*
                                                         attributeList,
                                                         OTF2_RmaWinRef   win )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaWinDestroy( tw->evt_writerMap[location],
                                              attributeList, time,
                                              win ) );
  }
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaPut( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  uint64_t         eventPosition,
                                                  void*            userData,
                                                  OTF2_AttributeList*
                                                  attributeList,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaPut( tw->evt_writerMap[location], attributeList,
                                       time,
                                       win, remote, bytes, matchingId ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaOpCompleteBlocking( OTF2_LocationRef location,
                                                                 OTF2_TimeStamp
                                                                 time,
                                                                 uint64_t
                                                                 eventPosition,
                                                                 void*            userData,
                                                                 OTF2_AttributeList
                                                                 *
                                                                 attributeList,
                                                                 OTF2_RmaWinRef
                                                                 win,
                                                                 uint64_t
                                                                 matchingId )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaOpCompleteBlocking( tw->evt_writerMap[location],
                                                      attributeList, time,
                                                      win, matchingId ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaGet( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  uint64_t         eventPosition,
                                                  void*            userData,
                                                  OTF2_AttributeList*
                                                  attributeList,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaGet( tw->evt_writerMap[location], attributeList,
                                       time,
                                       win, remote, bytes, matchingId ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_ThreadTeamBegin( OTF2_LocationRef locationID,
                                                           OTF2_TimeStamp   time,
                                                           uint64_t
                                                           eventPosition,
                                                           void*            userData,
                                                           OTF2_AttributeList*
                                                           attributeList,
                                                           OTF2_CommRef
                                                           threadTeam )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadTeamBegin( tw->evt_writerMap[locationID],
                                                attributeList, time,
                                                threadTeam ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_ThreadTeamEnd( OTF2_LocationRef locationID,
                                                         OTF2_TimeStamp   time,
                                                         uint64_t         eventPosition,
                                                         void*            userData,
                                                         OTF2_AttributeList*
                                                         attributeList,
                                                         OTF2_CommRef
                                                         threadTeam )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadTeamEnd( tw->evt_writerMap[locationID],
                                              attributeList, time,
                                              threadTeam ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackEnter( OTF2_LocationRef    location,
                                            OTF2_TimeStamp      time,
                                            uint64_t            eventPosition,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  /* write next node in List */
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = OTF2_EVT_ENTER;

  tw->processNextEvent( event );

  tw->activityStack[location].push( region );

  return OTF2_CALLBACK_SUCCESS;

}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackLeave( OTF2_LocationRef    location,
                                            OTF2_TimeStamp      time,
                                            uint64_t            eventPosition,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  /* write next node in List */
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = OTF2_EVT_LEAVE;

  tw->processNextEvent( event );

  tw->activityStack[location].pop( );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_EvtReaderCallback_ThreadFork( OTF2_LocationRef locationID,
                                                            OTF2_TimeStamp
                                                            time,
                                                            uint64_t
                                                            eventPosition,
                                                            void*
                                                            userData,
                                                            OTF2_AttributeList
                                                            *
                                                            attributeList,
                                                            OTF2_Paradigm
                                                            paradigm,
                                                            uint32_t
                                                            numberOfRequestedThreads )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadFork( tw->evt_writerMap[locationID],
                                           attributeList, time, paradigm,
                                           numberOfRequestedThreads ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_EvtReaderCallback_ThreadJoin( OTF2_LocationRef locationID,
                                                            OTF2_TimeStamp
                                                            time,
                                                            uint64_t
                                                            eventPosition,
                                                            void*
                                                            userData,
                                                            OTF2_AttributeList
                                                            *
                                                            attributeList,
                                                            OTF2_Paradigm
                                                            paradigm )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadJoin( tw->evt_writerMap[locationID],
                                           attributeList, time, paradigm ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiRecv( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp   time,
                                               uint64_t         eventPosition,
                                               void*            userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               uint32_t         sender,
                                               OTF2_CommRef     communicator,
                                               uint32_t         msgTag,
                                               uint64_t         msgLength )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiRecv( tw->evt_writerMap[locationID],
                                        attributeList, time, sender,
                                        communicator, msgTag, msgLength ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiSend( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp   time,
                                               uint64_t         eventPosition,
                                               void*            userData,
                                               OTF2_AttributeList*
                                               attributeList,
                                               uint32_t         receiver,
                                               OTF2_CommRef     communicator,
                                               uint32_t         msgTag,
                                               uint64_t         msgLength )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiSend( tw->evt_writerMap[locationID],
                                        attributeList, time, receiver,
                                        communicator, msgTag, msgLength ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}