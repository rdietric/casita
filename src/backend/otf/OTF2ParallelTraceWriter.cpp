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

#include <cuda.h>
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

OTF2ParallelTraceWriter::OTF2ParallelTraceWriter( const char*          streamRefKeyName,
                                                  const char*          eventRefKeyName,
                                                  const char*          funcResultKeyName,
                                                  uint32_t             mpiRank,
                                                  uint32_t             mpiSize,
                                                  MPI_Comm             comm,
                                                  const char*          originalFilename,
                                                  std::set< uint32_t > ctrIdSet )
  :
    IParallelTraceWriter( streamRefKeyName, eventRefKeyName, funcResultKeyName,
                          mpiRank, mpiSize ),
    global_def_writer( NULL ),
    processNodes( NULL ),
    enableWaitStates( false ),
    iter( NULL ),
    lastGraphNode( NULL ),
    cTable( NULL ),
    graph( NULL ),
    verbose( false ),
    isFirstProcess( true )
{
  outputFilename.assign( "" );
  pathToFile.assign( "" );
  this->originalFilename.assign( originalFilename );

  this->ctrIdSet = ctrIdSet;

  flush_callbacks.otf2_post_flush = postFlush;
  flush_callbacks.otf2_pre_flush = preFlush;

  commGroup      = comm;

  for ( CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin( );
        ctrIter != ctrIdSet.end( ); ++ctrIter )
  {
    otf2CounterMapping[*ctrIter] = *ctrIter - 1;
  }

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

  printf( "[%u] FILENAME: %s path: %s \n ", mpiRank,
          outputFilename.c_str( ), pathToFile.c_str( ) );

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
  OTF2_Archive_OpenEvtFiles( archive );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
}

void
OTF2ParallelTraceWriter::close( )
{
  /* close all opened event writer */
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
}

/*
 * Copy definitions from original trace to new one
 */
void
OTF2ParallelTraceWriter::copyGlobalDefinitions( )
{

  if ( mpiRank == 0 )
  {
    global_def_writer = OTF2_Archive_GetGlobalDefWriter( archive );
  }

  OTF2_GlobalDefReader* global_def_reader = OTF2_Reader_GetGlobalDefReader(
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

  printf( "Read and wrote %lu definitions\n ", definitions_read );

}

/*
 * OTF2: just create event writer for this process
 * OTF1: create event stream writer and write "begin process" event
 */
void
OTF2ParallelTraceWriter::writeDefProcess( uint64_t id, uint64_t parentId,
                                          const char* name, ProcessGroup pg )
{
  /* create writer for this process */
  OTF2_EvtWriter* evt_writer = OTF2_Archive_GetEvtWriter(
    archive,
    OTF2_UNDEFINED_LOCATION );
  OTF2_CHECK( OTF2_EvtWriter_SetLocationID( evt_writer, id ) );
  evt_writerMap[id] = evt_writer;

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
  if ( mpiRank == 0 )
  {
    /* map to otf2 (otf2 starts with 0 instead of 1) */
    uint32_t id = otf2CounterMapping[otfId];

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
  cpuNodes          = 0;
  currentStackLevel = 0;
  currentlyRunningCPUFunctions.clear( );
  lastTimeOnCriticalPath[processId] = 0;
  lastProcessedNodePerProcess[processId] = new GraphNode( 0,
                                                          processId,
                                                          "START",
                                                          PARADIGM_ALL,
                                                          RECORD_ATOMIC,
                                                          MISC_PROCESS );
  OTF2Event event;
  event.location    = processId;
  event.type        = MISC;
  event.time        = timerOffset;
  lastCPUEventPerProcess[processId] = event;

  if ( verbose )
  {
    std::cout << "[" << mpiRank << "] Start writing for process " <<
    processId << std::endl;
  }

  processNodes      = nodes;
  enableWaitStates  = waitStates;
  iter = processNodes->begin( );
  lastGraphNode     = pLastGraphNode;
  this->verbose     = verbose;
  this->graph       = graph;
  cTable = ctrTable;

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

  OTF2_EvtReader* evt_reader = OTF2_Reader_GetEvtReader( reader, processId );

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

/* processing the next CPU event, calculating blame
 *
 */
bool
OTF2ParallelTraceWriter::processCPUEvent( OTF2Event event )
{
  GraphNode* node;

  node = *iter;

  OTF2Event  bufferedCPUEvent  = lastCPUEventPerProcess[event.location];
  GraphNode* lastProcessedNode =
    lastProcessedNodePerProcess[node->getStreamId( )];
  /* Any buffered CPU-Events? */
  if ( lastProcessedNode &&
       ( bufferedCPUEvent.time >
         ( lastProcessedNode->getTime( ) + timerOffset ) ) )
  {

    if ( bufferedCPUEvent.type != ENTER
         && bufferedCPUEvent.type != LEAVE )
    {
      return true;
    }
    /* write counter for critical path for all pending cpu events */
    OTF2_Type types[1] = { OTF2_TYPE_UINT64 };
    OTF2_MetricValue values[1];
    bool      valid;

    if ( lastProcessedNode &&
         ( lastProcessedNode->getCounter( cTable->getCtrId( CTR_CRITICALPATH ),
                                          &valid ) == 1 ) &&
         ( node->getCounter( cTable->getCtrId( CTR_CRITICALPATH ),
                             &valid ) == 1 )
         && ( lastTimeOnCriticalPath[event.location] != 0 ) )
    {
      values[0].unsigned_int = 1;
      for ( std::list< uint32_t >::iterator fIter =
              currentlyRunningCPUFunctions.begin( );
            fIter != currentlyRunningCPUFunctions.end( ); fIter++ )
      {
        activityGroupMap[*fIter].totalDurationOnCP += event.time
                                                      - std::max(
          activityGroupMap[*fIter].lastEnter,
          lastTimeOnCriticalPath[event.location] );
        /* std::cout << "[" << mpiRank << "] + Time on CP " <<
         * event.time */
        /*        - std::max(activityGroupMap[*fIter].lastEnter,
         * lastTimeOnCriticalPath[event.location]) << " to " */
        /*  << idStringMap[regionNameIdList[*fIter]] << "( " <<
         * activityGroupMap[*fIter].totalDurationOnCP << ") " <<
         * std::endl; */
      }
      lastTimeOnCriticalPath[event.location] = event.time;
    }
    else
    {
      values[0].unsigned_int = 0;
    }

    /* Write buffered CPU-event */
    if ( bufferedCPUEvent.type == ENTER )
    {
      OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writerMap[bufferedCPUEvent.location
                                        ], NULL,
                                        bufferedCPUEvent.time,
                                        bufferedCPUEvent.regionRef ) );
    }
    if ( bufferedCPUEvent.type == LEAVE )
    {
      OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writerMap[bufferedCPUEvent.location
                                        ], NULL,
                                        bufferedCPUEvent.time,
                                        bufferedCPUEvent.regionRef ) );
    }

    OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writerMap[bufferedCPUEvent.location],
                                       NULL, bufferedCPUEvent.time,
                                       otf2CounterMapping[cTable->getCtrId(
                                                            CTR_CRITICALPATH )],
                                       1, types, values ) );

    if ( bufferedCPUEvent.type == LEAVE &&
         ( bufferedCPUEvent.regionRef != event.regionRef ) )
    {
      if ( lastTimeOnCriticalPath[event.location] != 0 )
      {
        if ( ( activityGroupMap[bufferedCPUEvent.regionRef].lastEnter >
               lastProcessedNode->getTime( ) + timerOffset )
              )
        {
          /* std::cout << "[" << mpiRank << "] time on CP from LAST
           * Enter " */
          /*        << (values[0].unsigned_int *
           * (bufferedCPUEvent.time - */
          /*
           *  activityGroupMap[bufferedCPUEvent.regionRef].lastEnter)) */
          /* << " to leave " <<
           * idStringMap[regionNameIdList[bufferedCPUEvent.regionRef]]
           * << std::endl; */
          activityGroupMap[bufferedCPUEvent.regionRef].totalDurationOnCP +=
            ( values[0].unsigned_int * ( bufferedCPUEvent.time -
                                         activityGroupMap[bufferedCPUEvent.
                                                          regionRef].lastEnter ) );
        }
        else
        {
          /* std::cout << "[" << mpiRank << "] time on CP from NODE " */
          /*        << (values[0].unsigned_int *
           * (bufferedCPUEvent.time - */
          /*            (lastProcessedNode->getTime() + timerOffset)))
           * << " buffered: " << bufferedCPUEvent.time << " last: " */
          /*        << lastProcessedNode->getTime() + timerOffset << "
           * value: " << values[0].unsigned_int << " " */
          /*        << lastProcessedNode->getUniqueName() */
          /*          << " to leave " <<
           * idStringMap[regionNameIdList[bufferedCPUEvent.regionRef]]
           * << std::endl; */
          activityGroupMap[bufferedCPUEvent.regionRef].totalDurationOnCP +=
            ( values[0].unsigned_int * ( bufferedCPUEvent.time -
                                         ( lastProcessedNode->getTime( ) +
                                           timerOffset ) ) );
        }
      }
    }

  }

  if ( bufferedCPUEvent.type == ENTER )
  {
    activityGroupMap[bufferedCPUEvent.regionRef].lastEnter =
      bufferedCPUEvent.time;
  }
  else
  {
    if ( bufferedCPUEvent.type == LEAVE )
    {
      activityGroupMap[bufferedCPUEvent.regionRef].totalDuration +=
        bufferedCPUEvent.time -
        activityGroupMap[bufferedCPUEvent.regionRef].lastEnter;
    }
  }

  return true;
}

/*
 * Process next node in list from analysis
 */
bool
OTF2ParallelTraceWriter::processNextNode( OTF2Event event )
{

  if ( activityGroupMap.find( event.regionRef ) == activityGroupMap.end( ) )
  {
    activityGroupMap[event.regionRef].functionId   = event.regionRef;
    activityGroupMap[event.regionRef].numInstances = 0;
  }

  if ( verbose > VERBOSE_ANNOY || true )
  {
    std::cout << "[" << mpiRank << "] process " << event.regionRef << " " <<
    idStringMap[regionNameIdList[event.regionRef]] << " " << event.type
              << " at " <<
    ( (double)event.time - (double)timerOffset ) / (double)timerResolution
              << " on " << event.location
              << std::endl;
  }

  if ( event.type == ENTER )
  {
    activityGroupMap[event.regionRef].numInstances++;
  }

  uint64_t   time   = event.time;
  uint32_t   funcId = event.regionRef;
  GraphNode* node;

  /* Skip threadFork/Join (also skips first inserted processNode that
   * is not in original trace)
   */
  while ( ( iter != processNodes->end( ) ) && ( *iter )->isOMPParallelRegion( ) )
  {
    if ( verbose )
    {
      std::cout << "Skipping " << ( *iter )->getUniqueName( ) << std::endl;
    }
    iter++;
  }

  if ( iter == processNodes->end( ) )
  {
    std::cout << "[" << mpiRank << "] reached end of processNodes at " <<
    event.time << std::endl;
    if ( event.type == ENTER )
    {
      activityGroupMap[event.regionRef].lastEnter = event.time;
      currentlyRunningCPUFunctions.push_back( event.regionRef );
    }
    else
    {
      if ( event.type == LEAVE )
      {
        activityGroupMap[event.regionRef].totalDuration += event.time -
                                                           activityGroupMap[
          event
          .
          regionRef
                                                           ].lastEnter;
        currentlyRunningCPUFunctions.pop_back( );
      }
    }
    return false;
  }

  node = *iter;

  /*
   * CPU events are not in nodes, hence they have to be written and countervalues have to be calculated first.
   * Since we need the time between a node and its successor we buffer one cpu event at each time.
   */
  OTF2Event  bufferedCPUEvent  = lastCPUEventPerProcess[event.location];
  GraphNode* lastProcessedNode =
    lastProcessedNodePerProcess[node->getStreamId( )];

  /* Any buffered CPU-Events to be processed? */
  if ( lastProcessedNode && bufferedCPUEvent.time >
       ( lastProcessedNode->getTime( ) + timerOffset ) )
  {
    processCPUEvent( event );
  }

  /* CPU_nodes are to be buffered */
  FunctionDescriptor desc;
  bool isDeviceStream = deviceStreamMap[event.location];
  if ( !FunctionTable::getAPIFunctionType( idStringMap[regionNameIdList[funcId]
                                           ], &desc, isDeviceStream, false ) )
  {
    if ( verbose )
    {
      std::cout << "[" << mpiRank << "] Buffer: [" << event.location <<
      "] function: " << idStringMap[regionNameIdList[funcId]] << std::endl;
    }

    if ( event.type == ENTER )
    {
      activityGroupMap[event.regionRef].lastEnter = event.time;
    }

    /* Add Blame for area since last event */
    assignBlame( event.time, event.location );

    /* Keep track of cpuFunctions */
    if ( event.type == ENTER )
    {
      currentlyRunningCPUFunctions.push_back( event.regionRef );
    }
    if ( event.type == LEAVE )
    {
      currentlyRunningCPUFunctions.pop_back( );
    }

    cpuNodes++;

    lastCPUEventPerProcess[event.location] = event;

    return true;
  }

  if ( verbose )
  {
    std::cout << "[" << mpiRank << "] processed " << cpuNodes << " cpuNodes" <<
    std::endl;
  }

  cpuNodes = 0;

  if ( time != ( node->getTime( ) + timerOffset ) )
  {
    if ( verbose )
    {
      std::cout <<
      "Node not written from original trace, since there is an inconsistency: TIME: "
                << time << " - " << node->getTime( ) + timerOffset
                << "funcId " << funcId << " - " << node->getFunctionId( ) <<
      std::endl;
    }
    if ( event.type == ENTER )
    {
      activityGroupMap[event.regionRef].lastEnter = event.time;
    }
    else
    {
      if ( event.type == LEAVE )
      {
        activityGroupMap[event.regionRef].totalDuration += event.time -
                                                           activityGroupMap[
          event
          .
          regionRef
                                                           ].lastEnter;
      }
    }
    return false;
  }

  /* find next connected node on critical path */
  GraphNode*      futureCPNode = NULL;
  Graph::EdgeList outEdges;
  if ( graph->hasOutEdges( (GraphNode*)node ) )
  {
    outEdges = graph->getOutEdges( (GraphNode*)node );
  }

  Graph::EdgeList::const_iterator edgeIter = outEdges.begin( );
  uint64_t timeNextCPNode      = 0;
  uint32_t cpCtrId = cTable->getCtrId( CTR_CRITICALPATH );

  while ( edgeIter != outEdges.end( ) && !outEdges.empty( ) )
  {
    GraphNode* edgeEndNode = ( *edgeIter )->getEndNode( );

    if ( ( edgeEndNode->getCounter( cpCtrId, NULL ) == 1 ) &&
         ( ( timeNextCPNode > edgeEndNode->getTime( ) ) || timeNextCPNode == 0 ) )
    {
      futureCPNode   = edgeEndNode;
      timeNextCPNode = futureCPNode->getTime( );
    }
    ++edgeIter;
  }

  if ( futureCPNode == NULL )
  {
    std::cout << "[" << mpiRank << "] futureCP NULL " << std::endl;
  }
  else
  {
    std::cout << "[" << mpiRank << "] futureCP: "
              << futureCPNode->getUniqueName( )
              << std::endl;
  }

  if ( node->isEnter( ) || node->isLeave( ) )
  {
    if ( ( !node->isPureWaitstate( ) ) || enableWaitStates )
    {
      if ( verbose )
      {
        printf( "[%u] [%12lu:%12.8fs] %60s in %8lu (FID %lu)\n",
                mpiRank,
                node->getTime( ),
                (double)( node->getTime( ) ) / (double)timerResolution,
                node->getUniqueName( ).c_str( ),
                node->getStreamId( ),
                node->getFunctionId( ) );
      }

      writeNode( node, *cTable,
                 node == lastGraphNode, futureCPNode );
    }
  }

  lastProcessedNodePerProcess[node->getStreamId( )] = node;
  /* set iterator to next node in sorted list */
  iter++;

  if ( event.type == ENTER )
  {
    activityGroupMap[event.regionRef].lastEnter = event.time;
  }
  else
  {
    if ( event.type == LEAVE )
    {
      activityGroupMap[event.regionRef].totalDuration += event.time -
                                                         activityGroupMap[event
                                                                          .
                                                                          regionRef
                                                         ].lastEnter;
    }
  }

  if ( graph->hasOutEdges( node ) &&
       ( node->getId( ) != lastNodeCheckedForEdgesId ) )
  {
    const Graph::EdgeList& edges = graph->getOutEdges( node );
    for ( Graph::EdgeList::const_iterator edgeIter = edges.begin( );
          edgeIter != edges.end( );
          edgeIter++ )
    {
      if ( ( ( *edgeIter )->getCPUBlame( ) > 0 ) )
      {
        openEdges.push_back( *edgeIter );
      }
    }
    lastNodeCheckedForEdgesId = node->getId( );
  }

  return true;

}

/*
 * Write the nodes that were read at program start, and processed during analysis
 * write corresponding counter values of computed metrics for each node
 */
void
OTF2ParallelTraceWriter::writeNode( GraphNode*       node,
                                    CounterTable&    ctrTable,
                                    bool             lastProcessNode,
                                    const GraphNode* futureNode )
{

  uint64_t processId         = node->getStreamId( );
  uint64_t nodeTime          = node->getTime( ) + timerOffset;

  /* Add Blame for area since last event */
  if ( !( currentStackLevel > currentlyRunningCPUFunctions.size( ) ) &&
       node->isEnter( ) )
  {
    assignBlame( node->getTime( ) + timerOffset, node->getStreamId( ) );
  }

  OTF2_EvtWriter* evt_writer = evt_writerMap[processId];

  if ( node->isEnter( ) || node->isLeave( ) )
  {
    if ( node->isEnter( ) )
    {
      OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writer, NULL, nodeTime,
                                        node->getFunctionId( ) ) );
    }
    else
    {
      OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writer, NULL, nodeTime,
                                        node->getFunctionId( ) ) );
    }

  }

  CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs( );
  for ( CounterTable::CtrIdSet::const_iterator iter = ctrIdSet.begin( );
        iter != ctrIdSet.end( ); ++iter )
  {
    bool     valid         = false;
    uint32_t otf2CtrId     = otf2CounterMapping[*iter];
    uint32_t ctrId         = *iter;
    CtrTableEntry* counter = ctrTable.getCounter( ctrId );
    if ( counter->isInternal )
    {
      continue;
    }

    CounterType ctrType    = counter->type;
    if ( ctrType == CTR_WAITSTATE_LOG10 || ctrType == CTR_BLAME_LOG10 )
    {
      continue;
    }

    uint64_t ctrVal        = node->getCounter( ctrId, &valid );

    if ( valid || counter->hasDefault )
    {
      if ( !valid )
      {
        ctrVal = counter->defaultValue;
      }

      if ( ctrType == CTR_WAITSTATE )
      {
        uint64_t ctrValLog10 = 0;
        if ( ctrVal > 0 )
        {
          ctrValLog10 = std::log10( (double)ctrVal );
        }

        OTF2_Type types[1]   = { OTF2_TYPE_UINT64 };
        OTF2_MetricValue values[1];
        values[0].unsigned_int = ctrValLog10;

        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, nodeTime,
                                           otf2CounterMapping[ctrTable.getCtrId(
                                                                CTR_WAITSTATE_LOG10 )
                                           ],
                                           1, types, values ) );
      }

      if ( ctrType == CTR_BLAME )
      {
        if ( node->isLeave( ) )
        {
          if ( graph->hasOutEdges( node ) )
          {

            const Graph::EdgeList& edges = graph->getOutEdges( node );
            GraphNode* closestNode       = edges.front( )->getEndNode( );
            /* get closest Node */
            for ( Graph::EdgeList::const_iterator edgeIter = edges.begin( );
                  edgeIter != edges.end( ); edgeIter++ )
            {
              if ( closestNode->getTime( ) >
                   ( *edgeIter )->getEndNode( )->getTime( ) )
              {
                closestNode = ( *edgeIter )->getEndNode( );
              }
            }
            /* Add blame to edge with closes Node */
            for ( Graph::EdgeList::const_iterator edgeIter = edges.begin( );
                  edgeIter != edges.end( ); edgeIter++ )
            {
              if ( closestNode == ( *edgeIter )->getEndNode( ) )
              {
                ( *edgeIter )->addCPUBlame( ctrVal );
              }
            }
          }
          continue;
        }

        activityGroupMap[node->getFunctionId( )].totalBlame += ctrVal;
        uint64_t ctrValLog10 = 0;
        if ( ctrVal > 0 )
        {
          ctrValLog10 = std::log10( (double)ctrVal );
        }

        OTF2_Type types[1]   = { OTF2_TYPE_UINT64 };
        OTF2_MetricValue values[1];
        values[0].unsigned_int = ctrValLog10;

        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, nodeTime,
                                           otf2CounterMapping[ctrTable.getCtrId(
                                                                CTR_BLAME_LOG10 )
                                           ],
                                           1, types, values ) );
      }

      if ( ctrType == CTR_CRITICALPATH_TIME )
      {
        /* std::cout << "[" << mpiRank << "] " <<
         * node->getUniqueName() << " Add TOCP " << ctrVal */
        /*              << std::endl; */
        activityGroupMap[node->getFunctionId( )].totalDurationOnCP += ctrVal;

        continue;
        /* We used to write the counter for time on critical path. We don't do this anymore.
      if ( node->isEnter( ) )
      {
        cpTimeCtrStack.push( ctrVal );
      }
      else
      {
        ctrVal = cpTimeCtrStack.top( );
        cpTimeCtrStack.pop( );
      }
         * */
      }

      OTF2_Type types[1] = { OTF2_TYPE_UINT64 };
      OTF2_MetricValue values[1];
      values[0].unsigned_int = ctrVal;

      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, nodeTime,
                                         otf2CtrId, 1, types, values ) );

      if ( ( ctrType == CTR_CRITICALPATH ) && ( ctrVal == 1 ) )
      {
        if ( lastTimeOnCriticalPath[node->getStreamId( )] != 0 )
        {
          for ( std::list< uint32_t >::iterator fIter =
                  currentlyRunningCPUFunctions.begin( );
                fIter != currentlyRunningCPUFunctions.end( ); fIter++ )
          {
            /* std::cout << "[" << mpiRank << "] " <<
             * node->getUniqueName() << " Add TOCP " <<
             * (node->getTime()+ timerOffset */
            /*      - std::max(activityGroupMap[*fIter].lastEnter,
             * lastTimeOnCriticalPath[node->getStreamId()])) */
            /*        << " to " <<
             * idStringMap[regionNameIdList[*fIter]] */
            /*       << std::endl; */

            activityGroupMap[*fIter].totalDurationOnCP += node->getTime( ) +
                                                          timerOffset
                                                          - std::max(
              activityGroupMap[*fIter].lastEnter,
              lastTimeOnCriticalPath[node->getStreamId( )] );
          }
        }

        lastTimeOnCriticalPath[node->getStreamId( )] = node->getTime( ) +
                                                       timerOffset;

        if ( lastProcessNode )
        {
          OTF2_Type types[1] = { OTF2_TYPE_UINT64 };
          OTF2_MetricValue values[1];
          values[0].unsigned_int = 0;

          OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, nodeTime,
                                             otf2CtrId, 1, types, values ) );

          lastTimeOnCriticalPath[node->getStreamId( )] = 0;
        }

        /* make critical path stop in current process if next cp node
         * in different process or if there is no next cp node */
        if ( ( ( futureNode == NULL ) ||
               ( futureNode->getStreamId( ) != processId ) ) )
        {
          OTF2_Type types[1] = { OTF2_TYPE_UINT64 };
          OTF2_MetricValue values[1];
          values[0].unsigned_int = 0;

          OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, nodeTime,
                                             otf2CtrId, 1, types, values ) );

          lastTimeOnCriticalPath[node->getStreamId( )] = 0;
        }
      }

      if ( ( ctrType == CTR_CRITICALPATH ) && ( ctrVal == 0 ) )
      {
        if ( lastTimeOnCriticalPath[node->getStreamId( )] != 0 )
        {
          for ( std::list< uint32_t >::iterator fIter =
                  currentlyRunningCPUFunctions.begin( );
                fIter != currentlyRunningCPUFunctions.end( ); fIter++ )
          {
            activityGroupMap[*fIter].totalDurationOnCP += node->getTime( ) +
                                                          timerOffset
                                                          - std::max(
              activityGroupMap[*fIter].lastEnter,
              lastTimeOnCriticalPath[node->getStreamId( )] );
            /* std::cout << "[" << mpiRank << "] " <<
             * node->getUniqueName() << " + Time on CP " <<
             * (node->getTime() +timerOffset */
            /*        - std::max(activityGroupMap[*fIter].lastEnter,
             * lastTimeOnCriticalPath[node->getStreamId()])) << " to " */
            /*    << idStringMap[regionNameIdList[*fIter]] <<
             * std::endl; */
          }
        }
        lastTimeOnCriticalPath[node->getStreamId( )] = 0;
      }

    }
  }
}

/*
 * Method handling blame distribution to events.
 */
void
OTF2ParallelTraceWriter::assignBlame( uint64_t currentTime,
                                      uint64_t currentStream )
{
  /* Add Blame for area since last event */
  OTF2Event  bufferedCPUEvent  = lastCPUEventPerProcess[currentStream];
  GraphNode* lastProcessedNode = lastProcessedNodePerProcess[currentStream];

  if ( !( bufferedCPUEvent.type == ENTER ) && !( bufferedCPUEvent.type == LEAVE ) )
  {
    return;
  }

  OTF2_Type types[1]           = { OTF2_TYPE_UINT64 };
  OTF2_MetricValue values[1];
  uint32_t  fId = currentlyRunningCPUFunctions.back( );
  uint64_t  totalBlame         = 0;
  uint64_t  edgeBlame          = 0;
  uint64_t  blameAreaStart     = std::max( bufferedCPUEvent.time,
                                           lastProcessedNode->getTime( ) +
                                           timerOffset );
  uint64_t  timeDiff           = currentTime - blameAreaStart;
  for ( Graph::EdgeList::iterator edgeIter = openEdges.begin( );
        edgeIter != openEdges.end( ); )
  {

    Edge* edge = *edgeIter;
    if ( edge->getEndNode( )->getTime( ) > lastProcessedNode->getTime( ) )
    {
      edgeBlame += edge->getCPUBlame( );
      /* std::cout << "[" << mpiRank << "] Blame " <<
       * edge->getCPUBlame() << " from " <<
       * edge->getStartNode()->getUniqueName() */
      /*        << " -> " << edge->getEndNode()->getUniqueName() <<
       * std::endl; */
      if ( ( edge->getDuration( ) > 0 ) &&
           ( edge->getStartNode( )->getTime( ) + timerOffset < currentTime ) )
      {
        totalBlame += (double)( edge->getCPUBlame( ) ) *
                      (double)timeDiff /
                      (double)( edge->getDuration( ) );
      }
      edgeIter++;
    }
    else
    {
      openEdges.erase( edgeIter );
    }
  }

  values[0].unsigned_int            = totalBlame;

  activityGroupMap[fId].totalBlame += totalBlame;

  /* std::cout << "[" << mpiRank << "] write blame " << totalBlame <<
   * " to " << blameAreaStart << " fid " << fId << " " */
  /*        << idStringMap[regionNameIdList[fId]] << " on " <<
   * bufferedCPUEvent.location << " timeDiff " << timeDiff <<
   * std::endl; */

  OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writerMap[bufferedCPUEvent.location],
                                     NULL, blameAreaStart,
                                     otf2CounterMapping[cTable->getCtrId(
                                                          CTR_BLAME )
                                     ],
                                     1, types, values ) );

  if ( totalBlame > 0 )
  {
    totalBlame = std::log10( (double)totalBlame );
  }

  values[0].unsigned_int = totalBlame;
  OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writerMap[bufferedCPUEvent.location],
                                     NULL, blameAreaStart,
                                     otf2CounterMapping[cTable->getCtrId(
                                                          CTR_BLAME_LOG10 )
                                     ],
                                     1, types, values ) );

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

  if ( tw->mpiRank == 0 )
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
  OTF2_CHECK( OTF2_GlobalDefWriter_WriteLocationGroup( tw->global_def_writer,
                                                       self, name,
                                                       locationGroupType,
                                                       systemTreeParent ) );

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

  if ( tw->mpiRank == 0 )
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteGroup( tw->global_def_writer, self,
                                               name,
                                               groupType, paradigm, groupFlags,
                                               numberOfMembers, members ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteComm( tw->global_def_writer, self, name,
                                              group, parent ) );

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

  if ( tw->mpiRank == 0 )
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNode( tw->global_def_writer,
                                                        self, name, className,
                                                        parent ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty( tw->
                                                                global_def_writer,
                                                                systemTreeNode,
                                                                name, value ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeDomain( tw->
                                                              global_def_writer,
                                                              systemTreeNode,
                                                              systemTreeDomain ) );

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

  if ( tw->mpiRank == 0 )
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteAttribute( tw->global_def_writer, self,
                                                   name,
                                                   description, type ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteRmaWin( tw->global_def_writer, self,
                                                name,
                                                comm ) );

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

  OTF2_CHECK( OTF2_EvtWriter_MpiCollectiveEnd( tw->evt_writerMap[locationID],
                                               attributeList, time,
                                               collectiveOp, communicator, root,
                                               sizeSent, sizeReceived ) );

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

  OTF2_CHECK( OTF2_EvtWriter_MpiCollectiveBegin( tw->evt_writerMap[locationID],
                                                 attributeList, time ) );

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

  OTF2_CHECK( OTF2_EvtWriter_RmaWinCreate( tw->evt_writerMap[location],
                                           attributeList, time,
                                           win ) );
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

  OTF2_CHECK( OTF2_EvtWriter_RmaWinDestroy( tw->evt_writerMap[location],
                                            attributeList, time,
                                            win ) );
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

  OTF2_CHECK( OTF2_EvtWriter_RmaPut( tw->evt_writerMap[location], attributeList,
                                     time,
                                     win, remote, bytes, matchingId ) );

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

  OTF2_CHECK( OTF2_EvtWriter_RmaOpCompleteBlocking( tw->evt_writerMap[location],
                                                    attributeList, time,
                                                    win, matchingId ) );

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

  OTF2_CHECK( OTF2_EvtWriter_RmaGet( tw->evt_writerMap[location], attributeList,
                                     time,
                                     win, remote, bytes, matchingId ) );

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

  OTF2_CHECK( OTF2_EvtWriter_ThreadTeamBegin( tw->evt_writerMap[locationID],
                                              attributeList, time,
                                              threadTeam ) );

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

  OTF2_CHECK( OTF2_EvtWriter_ThreadTeamEnd( tw->evt_writerMap[locationID],
                                            attributeList, time,
                                            threadTeam ) );

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
  event.type      = ENTER;

  if ( !tw->processNextNode( event ) )
  {
    OTF2_CHECK( OTF2_EvtWriter_Enter( tw->evt_writerMap[location], attributes,
                                      time,
                                      region ) );
  }

  tw->currentStackLevel++;

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
  event.type      = LEAVE;

  if ( !tw->processNextNode( event ) )
  {
    OTF2_CHECK( OTF2_EvtWriter_Leave( tw->evt_writerMap[location], attributes,
                                      time,
                                      region ) );
  }

  tw->currentStackLevel--;

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
  OTF2ParallelTraceWriter* tw  = (OTF2ParallelTraceWriter*)userData;

  OTF2Event event;
  event.location  = locationID;
  event.regionRef = 0;
  event.time      = time;
  event.type      = MISC;

  OTF2Event  bufferedCPUEvent  = tw->lastCPUEventPerProcess[locationID];
  GraphNode* lastProcessedNode = tw->lastProcessedNodePerProcess[locationID];
  /* Any buffered CPU-Events? */
  if ( lastProcessedNode &&
       bufferedCPUEvent.time >
       ( lastProcessedNode->getTime( ) + tw->timerOffset ) )
  {
    if ( bufferedCPUEvent.type == ENTER
         || bufferedCPUEvent.type == LEAVE )
    {
      tw->assignBlame( time, locationID );
      tw->processCPUEvent( event );
      tw->lastCPUEventPerProcess[locationID] = event;
    }
  }

  OTF2_CHECK( OTF2_EvtWriter_ThreadFork( tw->evt_writerMap[locationID],
                                         attributeList, time, paradigm,
                                         numberOfRequestedThreads ) );

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

  OTF2Event event;
  event.location  = locationID;
  event.regionRef = 0;
  event.time      = time;
  event.type      = MISC;

  tw->lastCPUEventPerProcess[locationID] = event;

  OTF2_CHECK( OTF2_EvtWriter_ThreadJoin( tw->evt_writerMap[locationID],
                                         attributeList, time, paradigm ) );

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

  OTF2_CHECK( OTF2_EvtWriter_MpiRecv( tw->evt_writerMap[locationID],
                                      attributeList, time, sender,
                                      communicator, msgTag, msgLength ) );

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

  OTF2_CHECK( OTF2_EvtWriter_MpiSend( tw->evt_writerMap[locationID],
                                      attributeList, time, receiver,
                                      communicator, msgTag, msgLength ) );

  return OTF2_CALLBACK_SUCCESS;
}
