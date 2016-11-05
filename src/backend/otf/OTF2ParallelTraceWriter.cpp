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
 * What this file does:
 * - read original OTF2 file and immediately write it out again, combined with counter values for blame and CP
 * - open/close an OTF2 archive for file handling
 * - copy definitions from original OTF2 file
 * - compute counter values for CPU events
 * - update statistics for summary for critical blame
 *
 */

#include <mpi.h>
#include <cmath>
#include <iostream>
#include <stdlib.h>

// the following definition and include is needed for the printf PRIu64 macro
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

// following adjustments necessary to use MPI_Collectives with OTF2
#define OTF2_MPI_UINT64_T MPI_UINT64_T
#define OTF2_MPI_INT64_T MPI_INT64_T

#if defined(BOOST_AVAILABLE) 
#include <boost/filesystem.hpp>
#endif

#include <otf2/OTF2_MPI_Collectives.h>
#include <map>

#include "graph/EventNode.hpp"
#include "AnalysisMetric.hpp"
#include "common.hpp"
#include "EventStream.hpp"
#include "FunctionTable.hpp"
#include "otf/OTF2TraceReader.hpp"
#include "otf/OTF2ParallelTraceWriter.hpp"
#include "GraphEngine.hpp"
#include <Parser.hpp>

using namespace casita;
using namespace casita::io;

#define OTF2_CHECK( cmd ) \
  { \
   int _status = cmd; \
   if ( _status ) { throw RTException( "OTF2 command '%s' returned error %d", #cmd, _status );} \
  }

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) { throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
  }

/** Callbacks for OTF2 */
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

/** Necessary for OTF2 to work in MPI-Mode */
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

/** Necessary for OTF2 to work in MPI-Mode */
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
      return MPI_UINT32_T;
    case OTF2_TYPE_INT32:
      return MPI_INT32_T;
    case OTF2_TYPE_FLOAT:
      return MPI_FLOAT;
    case OTF2_TYPE_UINT64:
      return MPI_UINT64_T;
    case OTF2_TYPE_INT64:
      return MPI_INT64_T;
    case OTF2_TYPE_DOUBLE:
      return MPI_DOUBLE;
    default:
      return 0;
  }

}

/**
 *
 *
 * @param mpiRank               MPIrank of this analysis process
 * @param mpiSize               Size of communicator
 * @param originalFilename      Name of original trace file
 * @param writeToFile           Write to new OTF2 file or just analysis
 * @param metrics               Analysis metrics to be written
 * @param ignoreAsyncMpi        Ignore all asynchronous MPI communication in analysis
 */
OTF2ParallelTraceWriter::OTF2ParallelTraceWriter( uint32_t        mpiRank,
                                                  uint32_t        mpiSize,
                                                  const char*     originalFilename,
                                                  bool            writeToFile,
                                                  AnalysisMetric* metrics,
                                                  bool            ignoreAsyncMpi )
  :
    writeToFile( writeToFile ),
    mpiRank( mpiRank ),
    mpiSize( mpiSize ),
    cTable( metrics ),
    ignoreAsyncMpi( ignoreAsyncMpi ),
    ompForkJoinRef( 0 ),
    global_def_writer( NULL ),
    processNodes( NULL ),
    currentNodeIter( NULL ),
    isFirstProcess( true ),
    graph( NULL )
{
  outputFilename.assign( "" );
  pathToFile.assign( "" );
  this->originalFilename.assign( originalFilename );

  flush_callbacks.otf2_post_flush = postFlush;
  flush_callbacks.otf2_pre_flush  = preFlush;

  commGroup = MPI_COMM_WORLD;
}

OTF2ParallelTraceWriter::~OTF2ParallelTraceWriter( )
{

}

void
OTF2ParallelTraceWriter::open( const std::string otfFilename, uint32_t maxFiles )
{
  #if defined(BOOST_AVAILABLE)  
    
  boost::filesystem::path boost_path     = boost::filesystem::system_complete(otfFilename);
  boost::filesystem::path boost_filename = otfFilename;

  outputFilename = boost::filesystem::change_extension(
    boost_filename.filename( ), "" ).string( );
  pathToFile     = boost_path.parent_path().string();

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] PATH: '%s'", pathToFile.c_str( ) );
  
  #else

  outputFilename = Parser::getInstance().getOutArchiveName();
  pathToFile = Parser::getInstance().getPathToFile();
  
  #endif

  
  if ( writeToFile )
  {
    #if defined(BOOST_AVAILABLE)
    if ( mpiRank == 0 )
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
    }
    #endif
    
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    // open new otf2 file
    archive = OTF2_Archive_Open( pathToFile.c_str( ),
                                 outputFilename.c_str( ),
                                 OTF2_FILEMODE_WRITE, 1024 * 1024, 4 * 1024 *
                                 1024, OTF2_SUBSTRATE_POSIX,
                                 OTF2_COMPRESSION_NONE );

    OTF2_Archive_SetFlushCallbacks( archive, &flush_callbacks, NULL );

    // set collective callbacks to write trace in parallel
    OTF2_MPI_Archive_SetCollectiveCallbacks( archive, commGroup, MPI_COMM_NULL );
  }


  timerOffset     = 0;
  timerResolution = 0;
  counterForStringDefinitions = 0;
  //counterForMetricInstanceId = 0;
  counterForAttributeId = 0;

  reader          = OTF2_Reader_Open( originalFilename.c_str( ) );

  OTF2_MPI_Reader_SetCollectiveCallbacks( reader, commGroup );

  if ( !reader )
  {
    throw RTException( "Failed to open OTF2 trace file %s",
                       originalFilename.c_str( ) );
  }

  copyGlobalDefinitions( );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

  // open event files files for each location
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

void
OTF2ParallelTraceWriter::reset()
{
  //\todo: check
  //idStringMap.clear();
  
  processNodes = NULL;
  graph = NULL;
  
  // static metric information, and OTF2 definitions IDs
  // should no change between analysis intervals
  //cTable = NULL; 
  
  //\todo: should be empty ... check that
  //activityStack.clear();
  
  clearOpenEdges();
}

/**
 * Clear the list of open edges and report remaining ones. These out edges are 
 * used to blame regions within the edge interval. Edges have to be intra 
 * stream edges. The list of out edges should be empty after a stream has been 
 * processed.
 */
void
OTF2ParallelTraceWriter::clearOpenEdges( )
{
  if( openEdges.size() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "[%" PRIu64 "] Clear open edge(s)", currentStream->getId() );
    for ( OpenEdgesList::const_iterator edgeIter = openEdges.begin( );
        edgeIter != openEdges.end( ); ++edgeIter)
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "  %s", (*edgeIter)->getName().c_str() );
    }
    openEdges.clear();
  }
}

/**
 * Convert event time to elapsed runtime time (to compare with Vampir times).
 * 
 * @param time OTF2 event timestamp
 * 
 * @return elapsed runtime
 */
double
OTF2ParallelTraceWriter::getRealTime( uint64_t time )
{
  return (double)( time - timerOffset ) / (double)timerResolution;
}

/**
 * Read definitions from original trace.
 * Write them to new one, if new OTF2 file is written.
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
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Attribute );
    
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_String );
    
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_ClockProperties );
    
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Location );
    
    OTF2_GlobalDefReaderCallbacks_SetGroupCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Group );
    
    OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_LocationGroup );
    
    OTF2_GlobalDefReaderCallbacks_SetCommCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Comm );
    
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Region );
    
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNode );
    
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodePropertyCallback(
      global_def_callbacks, 
      &OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty );
    
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeDomainCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain );
    
    OTF2_GlobalDefReaderCallbacks_SetRmaWinCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_RmaWin );
  }
  else
  {
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Location );
    
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_ClockProperties );
    
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_String );
    
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Region );
  }

  // register callbacks
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

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] Read and wrote %"PRIu64" definitions", definitions_read );

  // add fork/join "region" to support internal OMP-fork/join model
  // ( OMP-fork/join is a node in CASITA internally )
  uint32_t stringSize              = idStringMap.size( );
  idStringMap[stringSize]          = OTF2_OMP_FORKJOIN_INTERNAL;
  ompForkJoinRef                   = regionNameIdList.size( );
  regionNameIdList[ompForkJoinRef] = stringSize;

}

/**
 * OTF2: create event writer for this process.
 *
 * @param id            ID of event stream
 * @param parentId      ID of parent event stream
 * @param name          Name of this event stream
 * @param pg            Process group this event stream belongs to
 */
void
OTF2ParallelTraceWriter::writeDefProcess( uint64_t id, uint64_t parentId,
                                          const char* name, ProcessGroup pg )
{
  if ( writeToFile )
  {
    OTF2_EvtWriter* evt_writer = OTF2_Archive_GetEvtWriter(
      archive,
      OTF2_UNDEFINED_LOCATION );
    OTF2_CHECK( OTF2_EvtWriter_SetLocationID( evt_writer, id ) );
    evt_writerMap[id] = evt_writer;
  }

  /* Tell writer to read from this event stream */
  OTF2_Reader_SelectLocation( reader, id );
}

/**
 * Write definitions for self-defined (analysis) metrics to output trace file.
 */
void
OTF2ParallelTraceWriter::writeAnalysisMetricDefinitions( )
{
  for ( size_t i = 0; i < NUM_DEFAULT_METRICS; ++i )
  {
    MetricType metric = (MetricType) i;
    const MetricEntry* entry = cTable->getMetric( metric );

    // ignore internal metrics
    if ( !(entry->isInternal) )
    {
      // only the root rank writes the global definitions
      if ( mpiRank == 0 )
      {
        //UTILS_MSG(true, "Write definition: %s", entry->name );
        
        // write string definition for metric and/or attribute name
        OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( global_def_writer,
                                                      counterForStringDefinitions,
                                                      entry->name ) );
        
        // write string definition for metric and/or attribute description
        OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( global_def_writer,
                                                      counterForStringDefinitions+1,
                                                      entry->description ) );
        
        if( entry->metricMode == ATTRIBUTE )
        {
          uint32_t newAttrId = cTable->newOtf2Id( metric );

          OTF2_CHECK( 
            OTF2_GlobalDefWriter_WriteAttribute( global_def_writer, newAttrId, 
                                                 counterForStringDefinitions,
                                                 counterForStringDefinitions+1,
                                                 OTF2_TYPE_UINT64 ) );
          
        }
        else if( entry->metricMode != METRIC_MODE_UNKNOWN )
        {
          //\todo: this is still wrong as we ignore all metric definitions in the input trace
          uint32_t newCtrMetricId = cTable->newOtf2Id( metric );
          
          // set default OTF2_MetricMode to unknown, which is invalid
          OTF2_MetricMode otf2MetricMode = METRIC_MODE_UNKNOWN;
          
          switch ( entry->metricMode )
          {
            case COUNTER_ABSOLUT_NEXT:
              otf2MetricMode = OTF2_METRIC_ABSOLUTE_NEXT;
              break;
              
            case COUNTER_ABSOLUT_LAST:
              otf2MetricMode = OTF2_METRIC_ABSOLUTE_LAST;
              break;
              
            default:
              otf2MetricMode = METRIC_MODE_UNKNOWN;
          }

          OTF2_CHECK( 
            OTF2_GlobalDefWriter_WriteMetricMember( global_def_writer, newCtrMetricId,
                                                    counterForStringDefinitions,
                                                    counterForStringDefinitions+1,
                                                    OTF2_METRIC_TYPE_USER,
                                                    otf2MetricMode,
                                                    OTF2_TYPE_UINT64,
                                                    OTF2_BASE_DECIMAL, 0, 0 ) );

          OTF2_CHECK( 
            OTF2_GlobalDefWriter_WriteMetricClass( global_def_writer, newCtrMetricId, 1,
                                                   &newCtrMetricId,
                                                   OTF2_METRIC_ASYNCHRONOUS,
                                                   OTF2_RECORDER_KIND_ABSTRACT ) );
        }

        // increase the string definition counter
        counterForStringDefinitions += 2;
      }
      else
      {
        // all processes need to know the OTF2 IDs for the metrics/attributes
        if( entry->metricMode != METRIC_MODE_UNKNOWN )
        {
          cTable->newOtf2Id( metric );
        }
      }
    }
  }
}

void
OTF2ParallelTraceWriter::setupAttributeList( void )
{
  attributes = OTF2_AttributeList_New( );
  
}

/**
 * Read all events from original trace for this process and combine them with the
 * events and counter values from analysis.
 *
 * @param processId  Id of the event stream to be analyzed
 */
void
OTF2ParallelTraceWriter::setupEventReader( uint64_t processId )
{
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_ANNOY, 
             "[%"PRIu32"] Start writing for process %"PRIu64, mpiRank, processId );

  if ( isFirstProcess )
  {
    // \todo: only once!
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC &&  mpiRank == 0, 
               "[0] Write OTF2 trace file with CASITA counters.\n");
    OTF2_Reader_OpenEvtFiles( reader );
    OTF2_Reader_OpenDefFiles( reader );
    isFirstProcess = false;
  }

  // \todo: only once!
  OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, processId );
  uint64_t def_reads         = 0;
  OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
  OTF2_Reader_CloseDefReader( reader, def_reader );

  //OTF2_Reader_GetEvtReader( reader, processId );

  OTF2_EvtReader* evt_reader = OTF2_Reader_GetEvtReader( reader, processId );

  OTF2_EvtReaderCallbacks* event_callbacks = OTF2_EvtReaderCallbacks_New( );
  OTF2_EvtReaderCallbacks_SetEnterCallback( event_callbacks, &otf2CallbackEnter );
  OTF2_EvtReaderCallbacks_SetLeaveCallback( event_callbacks, &otf2CallbackLeave );
  //\todo: write counters from input trace
  //OTF2_EvtReaderCallbacks_SetMetricCallback( event_callbacks, 
  //                                           &otf2Callback_Metric );
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
  OTF2_EvtReaderCallbacks_SetMpiIrecvRequestCallback( event_callbacks, 
                                                      &otf2Callback_MpiIrecvRequest );
  OTF2_EvtReaderCallbacks_SetMpiIrecvCallback( event_callbacks, 
                                               &otf2Callback_MpiIrecv );
  OTF2_EvtReaderCallbacks_SetMpiIsendCallback( event_callbacks, 
                                               &otf2Callback_MpiIsend );
  OTF2_EvtReaderCallbacks_SetMpiIsendCompleteCallback( event_callbacks, 
                                               &otf2Callback_MpiIsendComplete );
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

  OTF2_Reader_RegisterEvtCallbacks( reader, evt_reader, event_callbacks, this );
  OTF2_EvtReaderCallbacks_Delete( event_callbacks );
}


/**
 * Read all events from original trace for this stream and combine them with the
 * events and counter values from analysis.
 * This is done stream by stream.
 *
 * @param stream    event stream to process
 * @param graph     Pointer to internal built graph
 * @param isHost    
 * 
 * @return true, if more events are available for reading. otherwise false
 */
bool
OTF2ParallelTraceWriter::writeStream( EventStream*  stream,
                                      Graph*        graph,
                                      uint64_t*     events_read )
{
  // if the period is still default, there is nothing to write
  if( stream->getPeriod().second == 0 )
    return true;
  
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
             "[%"PRIu32"] Write stream %s", mpiRank, stream->getName() );
  
  //\todo: this prohibits parallelization
  currentStream = stream;
  processNodes  = &(stream->getNodes());
  assert( processNodes );
  
  currentNodeIter = processNodes->begin( );
  
  // check the first nodes on host processes. They might be some artificial 
  // atomic nodes (e.g. global source node or interval start node)
  if ( stream->isHostMasterStream() )
  {
    // the following node is for MPI streams the atomic node of the MPI
    // collective (previously the leave node), which we do not want to write
    // but some special handling, e.g. for the CP is needed
    while( (*currentNodeIter)->isAtomic() )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_ALL, 
                 "[%"PRIu32"] TraceWriter: Skip atomic event: %s", 
                 mpiRank, (*currentNodeIter)->getUniqueName().c_str( ) );
     
      // first part of the condition should be wrong for the global source node
      if( ( (*currentNodeIter)->getCounter( CRITICAL_PATH, NULL ) == 1 ) &&
          ( currentNodeIter + 1 != processNodes->end( ) ) && 
          ( *( currentNodeIter + 1 ) )->getCounter( CRITICAL_PATH, NULL ) == 0 )
      {
        OTF2_Type        type  = OTF2_TYPE_UINT64;
        OTF2_MetricValue value;
        OTF2_EvtWriter*  evt_writer = evt_writerMap[stream->getId()];

        value.unsigned_int = 0;
        lastCounterValues[CRITICAL_PATH] = 0;

        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, lastEventTime[stream->getId()],
                                           cTable->getMetricId( CRITICAL_PATH ), 
                                           1, &type, &value ) );
      }
      
      ++currentNodeIter;
    }
  }

  // store a pointer to the graph as class member
  this->graph = graph;
  
  // reset last counter values before processing the current stream
  const AnalysisMetric::MetricIdSet& ctrIdSet = cTable->getAllCounterIds( );
  for( AnalysisMetric::MetricIdSet::const_iterator ctrIdIter = ctrIdSet.begin();
       ctrIdIter != ctrIdSet.end( ); ++ctrIdIter )
  {
    lastCounterValues[*ctrIdIter] = 0;
  }
  
  // set the initial critical path value for this stream
  if( stream->isFirstCritical() )
  {
    processOnCriticalPath[stream->getId()] = true;
    //UTILS_MSG( true, "Process [%llu] has initial CP", stream->getId());
    
    // after the first interval analysis there is no first critical any more
    stream->isFirstCritical() = false;
  }
  else
  {
    processOnCriticalPath[stream->getId()] = false;
  }
  
  OTF2_EvtReader* evt_reader = OTF2_Reader_GetEvtReader( reader, stream->getId() );
  
  // we only know the number of global events to read, 
  // therefore check for a global collective again

  // returns 0 if successful, >0 otherwise
  OTF2_ErrorCode otf2_error = 
             OTF2_Reader_ReadAllLocalEvents( reader, evt_reader, events_read );
  
  clearOpenEdges( );
  
  if ( OTF2_SUCCESS != otf2_error )
  {
    if( OTF2_ERROR_INTERRUPTED_BY_CALLBACK == otf2_error )
    {
      return true;
    }
    else
      throw RTException( "Failed to read OTF2 events %llu", *events_read );
  }
  
  OTF2_Reader_CloseEvtReader( reader, evt_reader );
  
  return false;
}

/**
 * Returns the name of a region as a string.
 *
 * @param regionRef     ID of region the name is requested for
 * @return              String with Name of the region
 */
std::string
OTF2ParallelTraceWriter::getRegionName( const OTF2_RegionRef regionRef ) const
{
  std::map< uint32_t, OTF2_StringRef >::const_iterator regionNameIter =
    regionNameIdList.find( regionRef );

  UTILS_ASSERT( regionNameIter != regionNameIdList.end( ),
                "Could not find region reference in map" );

  std::map< uint32_t, const char* >::const_iterator idStrIter         =
    idStringMap.find( regionNameIter->second );

  UTILS_ASSERT( idStrIter != idStringMap.end( ),
                "Could not find string reference in map" );

  return idStrIter->second;
}

/**
 * Collect statistical information for activity groups that is used later to 
 * create the profile.
 *
 * @param event         current event that was read from original OTF2 file
 * @param counters      counter values for that event
 */
void
OTF2ParallelTraceWriter::updateActivityGroupMap( OTF2Event event, CounterMap& counters )
{
  // add function to list if not present yet
  if ( activityGroupMap.find( event.regionRef ) == activityGroupMap.end( ) )
  {
    activityGroupMap[event.regionRef].functionId   = event.regionRef;
    activityGroupMap[event.regionRef].numInstances = 0;
    activityGroupMap[event.regionRef].totalBlame = 0;
    
    /*UTILS_MSG( strcmp( getRegionName(event.regionRef).c_str(), "clFinish" ) == 0, 
               "[%u] Add %s to activity group map", 
               mpiRank, getRegionName(event.regionRef).c_str());*/
  }

  // for each enter event, increase the number of instances found
  if ( event.type == RECORD_ENTER )
  {
    activityGroupMap[event.regionRef].numInstances++;
  }

  // get the activity stack for the current event's location
  ActivityStackMap::const_iterator activityIter = activityStack.find( event.location );
  
  bool onCP = false;
  
  // if there are counters (nodes) available
  if( counters.size() != 0 )
  {
    onCP = counters[ CRITICAL_PATH ];
  }
  else
  {
    onCP = processOnCriticalPath[event.location];
  }

  // add duration, CP time and blame to current function on stack
  if ( activityIter != activityStack.end( ) && activityIter->second.size( ) > 0 )
  {
    uint32_t currentActivity = activityIter->second.top( );
    
    // time between the last and the current event
    uint64_t timeDiff = event.time - lastEventTime[event.location];

    activityGroupMap[currentActivity].totalDuration += timeDiff;

    activityGroupMap[currentActivity].totalDurationOnCP += onCP ? timeDiff : 0;

    activityGroupMap[currentActivity].totalBlame += counters[ BLAME ];
    
    /*UTILS_MSG( strcmp( getRegionName(event.regionRef).c_str(), "clFinish" ) == 0,
               "[%u] %s (type %d): on stack %s\t (%d) (Real-time: %lf), onCP: %d, "
               "blame: %llu, total blame: %llu", 
               mpiRank, getRegionName(event.regionRef).c_str(), event.type, 
               getRegionName(currentActivity).c_str(),
               activityStack[event.location].size( ),
               getRealTime( event.time), onCP, 
               counters[ BLAME ], activityGroupMap[currentActivity].totalBlame );*/
  }
}

/**
 * Compute blame for CPU event from blame that is stored in edges. 
 * Use the blame of the out edge of last graph node and distribute it among 
 * the following non-graph (CPU) events according to their duration. 
 * See also the documentation of the variable "openEdges".
 *
 * @param event Current CPU event
 * @return      Blame to assign to this event
 */
uint64_t
OTF2ParallelTraceWriter::computeCPUEventBlame( OTF2Event event )
{
  // collect blame from all open edges
  uint64_t totalBlame = 0;
  
  // time between current and last event on this location
  uint64_t timeDiff  = event.time - lastEventTime[event.location];
  
  // remove timer offset from event time
  uint64_t eventTime = event.time - timerOffset;

  //UTILS_MSG( openEdges.size(), "[%u] Compute blame for %s from %llu open edges", 
  //           mpiRank, getRegionName( event.regionRef ).c_str(), openEdges.size() );
  
  // iterate over all open edges (if any) and calculate total blame
  // \todo: sanity check for open edges
  for ( OpenEdgesList::iterator edgeIter = openEdges.begin( );
        edgeIter != openEdges.end( ); )
  {
    OpenEdgesList::iterator currentIter = edgeIter;
    OpenEdgesList::iterator nextIter    = ++edgeIter;

    Edge* edge = *currentIter;
    
    //std::cerr << getRegionName(event.regionRef) << " between nodes " << edge->getStartNode()->getUniqueName()
    //          << " -> " << edge->getEndNode()->getUniqueName() << std::endl;

    // if edge has duration AND event is in between the edge
    if ( ( edge->getDuration() > 0 ) &&
         ( edge->getEndNode()->getTime() > eventTime ) &&
         ( edge->getStartNode()->getTime() < eventTime ) )
    {
      // blame = blame(edge) * time(cpu_region)/time(edge)
      totalBlame += (double)( edge->getCPUBlame( ) ) * (double)timeDiff 
                  / (double)( edge->getDuration( ) );
    }
    else
    {
      // erase edge if event time is past its end node
      openEdges.erase( currentIter );
    }

    edgeIter = nextIter;
  }
  
  //UTILS_MSG( totalBlame, "[%u] Computed total blame: %llu", mpiRank, totalBlame );

  return totalBlame;
}

/**
 * Write attributes (or metric) values after the given event to new OTF2 file.
 *
 * @param event    event to add attributes
 * @param counters map of metric values
 */
void
OTF2ParallelTraceWriter::writeEventsWithAttributes( OTF2Event event, CounterMap& counters )
{
  UTILS_ASSERT( evt_writerMap.find( event.location ) != evt_writerMap.end( ),
                "Could not find OTF2 event writer for location" );
  
  OTF2_EvtWriter* evt_writer    = evt_writerMap[event.location];
  
  // for all available metrics
  for ( CounterMap::const_iterator iter = counters.begin( );
        iter != counters.end( ); ++iter )
  {
    const MetricType metricType = iter->first;
    
    // skip the critical path attribute, as it is "cheaper" to write a counter, 
    // whenever the critical path changes instead of to every region
    //if( CRITICAL_PATH == metricType )
    
    // ignore all metrics, but attributes
    if( cTable->getMetric( metricType )->metricMode != ATTRIBUTE )
    {
      continue;
    }

    // all metrics are definitely assigned to leave nodes
    if( event.type == RECORD_LEAVE && iter->second != 0 )
    {
      OTF2_CHECK( OTF2_AttributeList_AddUint64( attributes, 
                                                cTable->getMetricId( metricType ), 
                                                iter->second ) );
    }
  }
  
  switch ( event.type )
  {
    case RECORD_ENTER:
      OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writer, attributes, event.time, event.regionRef ) );
      break;

    case RECORD_LEAVE:
      OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writer, attributes, event.time, event.regionRef ) );
      break;

    default:
      /* write only counters for atomic events */
      break;
  }
}

/**
 * Write counter values to new OTF2 file. Write counters only if they have changed.
 * Critical path counter implementation is for OTF2_METRIC_ABSOLUTE_NEXT mode.
 * Waiting time and blame counters are OTF2_METRIC_ABSOLUTE_LAST mode.
 *
 * @param event    event to potentially add counter values
 * @param counters corresponding metric value map
 */
void
OTF2ParallelTraceWriter::writeEventsWithCounters( OTF2Event event, 
                                                  CounterMap& counters,
                                                  bool writeEvents )
{
  UTILS_ASSERT( evt_writerMap.find( event.location ) != evt_writerMap.end( ),
                "Could not find OTF2 event writer for location" );
  
  OTF2_EvtWriter* evt_writer = evt_writerMap[event.location];
  
  /*if( event.type == RECORD_ENTER && 
        strcmp( getRegionName(event.regionRef).c_str(), "BUFFER FLUSH" ) == 0 )
  {
    UTILS_MSG(true, "BUFFER FLUSH enter: '%s' stack %d (time: %llu), #counters: %llu", 
              getRegionName(event.regionRef).c_str(), activityStack[event.location].size( ),
              event.time, counters.size() );
  }*/

  // write enter event before counters
  if( writeEvents && event.type == RECORD_ENTER )
  {
    OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writer, NULL, event.time, event.regionRef ) );
  }

  // if the stream is on the critical path, but no more graph nodes available
  // and therefore the counter table is empty (can happen on host processes)
  if( counters.size() == 0  && processOnCriticalPath[event.location] == true )
  {
    // if we are at a leave event, which is the last on the stack, write '0'
    if( event.type == RECORD_LEAVE && activityStack[event.location].size( ) == 1
        && lastCounterValues[CRITICAL_PATH] != 0 )
    {
      OTF2_Type        type  = OTF2_TYPE_UINT64;
      OTF2_MetricValue value;
      
      value.unsigned_int = 0;
      lastCounterValues[CRITICAL_PATH] = 0;
      
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( CRITICAL_PATH ), 
                                         1, &type, &value ) );
      return;
    }
    
    if( event.type == RECORD_ENTER && activityStack[event.location].size( ) == 0 )
    {
      OTF2_Type        type  = OTF2_TYPE_UINT64;
      OTF2_MetricValue value;
      
      value.unsigned_int = 1;
      lastCounterValues[CRITICAL_PATH] = 1;
      
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( CRITICAL_PATH ), 
                                         1, &type, &value ) );
    }
    
    return;
  }

  // if counters are available
  for ( CounterMap::const_iterator iter = counters.begin( );
        iter != counters.end( ); ++iter )
  {
    const MetricType metricType = iter->first;
    
    MetricMode metricMode = cTable->getMetric( metricType )->metricMode;
    
    OTF2_Type        type = OTF2_TYPE_UINT64;
    OTF2_MetricValue value;
    
    // ignore attributes
    if( metricMode == ATTRIBUTE )
      continue;
    
    // critical path counter, absolute next mode
    if( CRITICAL_PATH == metricType )
    {      
      // set counter to '0' for last leave event on the stack, if last counter 
      // value is not already '0' for this location (applies to CUDA kernels)
      if( event.type == RECORD_LEAVE &&
          activityStack[event.location].size( ) == 1 &&
          lastCounterValues[CRITICAL_PATH] != 0 )
      {
        value.unsigned_int = 0;
        lastCounterValues[CRITICAL_PATH] = 0;
      
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( CRITICAL_PATH ), 
                                           1, &type, &value ) );
        continue;
      }
      
      uint64_t onCP = processOnCriticalPath[event.location];
      //uint64_t onCP = iter->second;
      if( lastCounterValues[CRITICAL_PATH] != onCP )
      {
        value.unsigned_int = onCP;
        lastCounterValues[CRITICAL_PATH] = onCP;
        
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( CRITICAL_PATH ), 
                                           1, &type, &value ) );
      }

      continue;
    }
    // END: critical path counter
    
    /////// other counters (blame and waiting time), absolute last mode ////////
#if defined(BLAME_COUNTER_FALSE)
    if( event.type == RECORD_ENTER )
    {
      if( activityStack[event.location].size( ) > 0 )
      {
        CounterMap* cm = leaveCounterStack[event.location].top();
        
        // if there is a region on the stack that has a counter, set it because
        // we write a new region on the stack
        if( (*cm)[metricType] > 0 )
        {
          value.unsigned_int = (*cm)[metricType];
          lastCounterValues[metricType] = (*cm)[metricType];
          
          OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                             cTable->getMetricId( metricType ), 
                                             1, &type, &value ) );
          
          continue;
        }
      }
      
      // set the enter of this region to '0'
      if( iter->second > 0 )
      {
        value.unsigned_int = 0;
        lastCounterValues[metricType] = 0;
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( metricType ), 
                                           1, &type, &value ) );
      }
    }
    else if( event.type == RECORD_LEAVE )
    {
      // if we have a leave with a counter, write it
      if( iter->second > 0 )
      {
        value.unsigned_int = iter->second;
        lastCounterValues[metricType] = iter->second;
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( metricType ), 
                                           1, &type, &value ) );
      }
      // if we are at a leave and the last region is on the stack, write '0'
      else if( activityStack[event.location].size( ) == 1 && 
               lastCounterValues[metricType] != 0 )
      {
        value.unsigned_int = 0;
        lastCounterValues[metricType] = 0;
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( metricType ), 
                                           1, &type, &value ) );
      }
    }
#endif // BLAME_COUNTER
    
    // The following is currently only for the blame counter
    
    // reset counter if this enter is the first event on the activity stack
    if ( event.type == RECORD_ENTER && activityStack[event.location].size( ) == 0 )
    {
      value.unsigned_int = 0;
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( metricType ), 
                                         1, &type, &value ) );
    }
    else
    {
      value.unsigned_int = iter->second;
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( metricType ), 
                                         1, &type, &value ) );
    }

    // reset counter if this leave is the last event on the activity stack
    if ( event.type == RECORD_LEAVE && activityStack[event.location].size( ) == 1 &&
         value.unsigned_int != 0 )
    {
      value.unsigned_int = 0;
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( metricType ), 
                                         1, &type, &value ) );
    }
  }
  
  // write leave event after counters
  if( writeEvents && event.type == RECORD_LEAVE )
  {
    OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writer, NULL, event.time, event.regionRef ) );
  }
}

/**
 * Process the next event read from original trace file.
 *
 * @param event
 * @param eventName
 * 
 * @return a pointer to the GraphNode object of the given event
 */
void
OTF2ParallelTraceWriter::processNextEvent( OTF2Event event, 
                                           const std::string eventName )
{  
  // test if this is an internal node or a CPU event
  FunctionDescriptor eventDesc;
  // set event type to determine if an internal node is available
  eventDesc.recordType = event.type; 
  const bool mapsInternalNode = FunctionTable::getAPIFunctionType(
    eventName.c_str( ), &eventDesc, deviceStreamMap[event.location], false );  

  //UTILS_MSG( mpiRank == 0, "Event name: '%s' (%d), maps internal: %d", 
  //           eventName.c_str( ), event.type, (int)mapsInternalNode );

  // non-internal counter values for this event
  CounterMap tmpCounters;

  // if this is a node we are using for analysis
  if ( mapsInternalNode )
  {
    // if we are after the end of the node list
    if ( currentNodeIter == processNodes->end( ) )
    {
      UTILS_MSG( true, "[%u] OTF2 writer: More events than nodes! "
                       "(%s (%" PRIu64 "): %s (%d) at %" PRIu64 ")", 
                 mpiRank, currentStream->getName(), event.location, 
                 eventName.c_str(), event.type, getRealTime( event.time ) );
    }
    else
    {
      GraphNode* currentNode = *currentNodeIter;

      UTILS_ASSERT( currentNode->getFunctionId() == event.regionRef,
                    //&& currentNode->getRecordType() == event.type,
                    " [%u] RegionRef doesn't fit for event %s:%d:%" PRIu64 ":%lf"
                    " and internal node %s:%lf, %u != %" PRIu64, mpiRank, 
                    eventName.c_str(), event.type, event.time, getRealTime(event.time),
                    currentNode->getUniqueName().c_str(),
                    (double)currentNode->getTime() / (double)timerResolution,
                    event.regionRef, currentNode->getFunctionId() );

      // model fork/join nodes as the currently running activity
      if ( currentNode->isOMPForkJoinRegion( ) )
      {
        UTILS_ASSERT( event.regionRef == ompForkJoinRef,
                      "ForkJoin must have regionRef %u", ompForkJoinRef );

        UTILS_ASSERT( event.type == RECORD_ATOMIC,
                      "Event %s has unexpected type", eventName.c_str( ) );
        /*
        UTILS_ASSERT( activityStack[event.location].size( ) > 0,
                      "[%u] No current activity for OMP ForkJoin "
                      "(%s, %u)",
                      mpiRank, currentNode->getUniqueName( ).c_str( ), event.regionRef);
        */
        
        if( activityStack[event.location].size( ) > 0 )
        {
          const OTF2_RegionRef newRegionRef = activityStack[event.location].top( );
          currentNode->setFunctionId( newRegionRef );
        
          event.regionRef = newRegionRef;
        }
      }

      // preprocess current internal node (mark open edges to blame CPU events)
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

      // copy node counter values to temporary counter map
      const AnalysisMetric::MetricIdSet& metricIdSet = cTable->getAllMetricIds( );
      for ( AnalysisMetric::MetricIdSet::const_iterator metricIter = metricIdSet.begin( );
            metricIter != metricIdSet.end( ); ++metricIter )
      {
        const MetricType metricType = *metricIter;
        const MetricEntry* metric = cTable->getMetric( metricType );

        if ( !( metric->isInternal ) )
        {
          tmpCounters[metricType] = currentNode->getCounter( metricType, NULL );
          
          // set CP counter to 0, if the next node is NOT on the CP 
          // (because we use counter next mode)
          if( CRITICAL_PATH == metricType )
          {
            processOnCriticalPath[event.location] = (bool) tmpCounters[ CRITICAL_PATH ];
            
            // if next node is NOT on the CP
            if( currentNodeIter + 1 != processNodes->end( ) &&  
                (*( currentNodeIter + 1 ) )->getCounter( CRITICAL_PATH, NULL ) == 0 )
            {
              processOnCriticalPath[event.location] = false;
            }
            
            // if node is leave
            if( currentNode->isLeave() )
            {
              // if node is leave AND enter has zero as CP counter
              if( currentNode->getPartner()->getCounter(CRITICAL_PATH, NULL ) == 0 )
              {
                // zero the counter, as the activity is not on the CP
                tmpCounters[ CRITICAL_PATH ] = 0;

                // Does NOT work for visualization (OTF2)!
                // visualization uses processOnCriticalPath[event.location]
              }

              // if we are at an MPI_Finalize leave and the current stream does 
              // not contain the globally last event
              if( currentNode->isMPIFinalize() && !currentStream->hasLastGlobalEvent( ) )
              {
                processOnCriticalPath[event.location] = false;
              }
            }

            /*UTILS_MSG( ( strcmp( currentNode->getName(), "clFinish") == 0 ), 
                       "[%llu] %s: onCP %llu=?%d, Blame=%llu (EvtType: %d)", 
                       currentNode->getStreamId(), currentNode->getUniqueName().c_str(), 
                       tmpCounters[CRITICAL_PATH], processOnCriticalPath[event.location], 
                       tmpCounters[BLAME], event.type );*/
          }
          
#if defined(BLAME_COUNTER_FALSE)
          /* no special handling for CP counter */
          if( CRITICAL_PATH != metricType )
          {
            // for enter, get counter value of leave node
            if( currentNode->isEnter() )
            {
              tmpCounters[metricType] = currentNode->getPartner()->getCounter( metricType, NULL );
            }
          }
#endif //BLAME_COUNTER
        }
      }
      
      // check in edges to blame the interval between this and the last event
      if ( currentNode->isEnter() && graph->hasInEdges( currentNode ) )
      {
        // duration between last and current event
        uint64_t timeDiff = event.time - lastEventTime[event.location];
        
        // iterate over in edges
        const Graph::EdgeList& edges = graph->getInEdges( currentNode );
        for ( Graph::EdgeList::const_iterator edgeIter = edges.begin( );
              edgeIter != edges.end( ); edgeIter++ )
        {
          Edge* edge = *edgeIter;
          if ( edge->getCPUBlame() > 0 )
          {
            /*
            UTILS_MSG( ( strcmp( currentNode->getName(), "clFinish") == 0 ), 
                       "[%llu] %s (%lf) has in Edge %s with blame %lf", 
                       currentNode->getStreamId(), getRealTime(event.time),
                       currentNode->getName(), edge->getName().c_str(), edge->getCPUBlame() );
            */
            
            // calculate the partial blame for this edge
            double blame = (double)( edge->getCPUBlame( ) ) * (double)timeDiff 
                   / (double)( edge->getDuration( ) );
            
            // increase the blame counter for this event
            tmpCounters[BLAME] += blame;
          }
        }
      }

      // increase iterator over graph nodes
      ++currentNodeIter;
    }
  }
  else
  { // this is a CPU or unknown event
    /*
    UTILS_MSG( strcmp( getRegionName(event.regionRef).c_str(), "cuDevicePrimaryCtxRetain" ) == 0, 
              "[%u] cuDevicePrimaryCtxRetain: '%s' (time: %llu); %d", 
              mpiRank, getRegionName(event.regionRef).c_str(), 
              getRealTime( event.time ), event.type );
    */
    // the currentNodeIter points to a node after the current event
    
    // compute counters for that event, if we still have internal nodes following
    if ( currentNodeIter != processNodes->end( ) )
    {
      //// Compute critical path counter ////
      // Event is on critical path if next internal node is, too
      // BUT: if next event is a leave AND the corresponding enter node is not on the CP
      // the nested regions are not on the CP
      if( ( *currentNodeIter )->isLeave() &&
          ( *currentNodeIter )->getPartner()->getCounter( CRITICAL_PATH, NULL ) == 0 )
      {
        tmpCounters[CRITICAL_PATH] = 0;
      }
      else
      {
        tmpCounters[CRITICAL_PATH] = ( *currentNodeIter )->getCounter( CRITICAL_PATH, NULL );
      }
      
      // compute blame counter
      uint64_t blame = computeCPUEventBlame( event );

      //\todo: validate the following if, which affects only the OTF2 output
      if( blame )
        tmpCounters[BLAME] = blame;

      // non-paradigm events cannot be wait states
      tmpCounters[WAITING_TIME] = 0;
    }
  }

  // write event with counters
  if ( writeToFile )
  {
    writeEventsWithCounters( event, tmpCounters, false );
    writeEventsWithAttributes( event, tmpCounters );    
  }

  // update values in activityGroupMap
  updateActivityGroupMap( event, tmpCounters );

  // set last event time for all event types (CPU and paradigm nodes)
  lastEventTime[event.location] = event.time;

  // update activity stack
  switch ( event.type )
  {
    case RECORD_ENTER:
    {
      activityStack[event.location].push( event.regionRef );
      
#if defined(BLAME_COUNTER_FALSE)
      CounterMap* cm = new CounterMap( tmpCounters );
      leaveCounterStack[event.location].push( cm );
#endif //BLAME_COUNTER
      
      break;
    }
    case RECORD_LEAVE:
    {
      activityStack[event.location].pop( );

#if defined(BLAME_COUNTER_FALSE)
      delete leaveCounterStack[event.location].top( );
      leaveCounterStack[event.location].pop( );
#endif //BLAME_COUNTER
      
      break;
    }
    default:
      break;
  }
}

/*
 * Callbacks to re-write definition records of original trace file.
 * Every callback has the writer object within @var{userData} and writes record 
 * immediately after reading.
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
                                                              OTF2_StringRef self,
                                                              const char* string )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  // keep track how many strings are defined to add definitions for metrics later
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
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty( 
                                        void*                  userData,
                                        OTF2_SystemTreeNodeRef systemTreeNode,
                                        OTF2_StringRef         name,
                                        OTF2_Type              type,
                                        OTF2_AttributeValue    value )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty( tw->global_def_writer,
                                                                  systemTreeNode,
                                                                  name, type,
                                                                  value ) );
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
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Attribute( 
                                                  void*             userData,
                                                  OTF2_AttributeRef self,
                                                  OTF2_StringRef    name,
                                                  OTF2_StringRef    description,
                                                  OTF2_Type         type )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    // do not write the attribute definitions that are only for CASITA in the trace
    if ( strcmp(tw->idStringMap[ name ], SCOREP_CUDA_STREAMREF ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_CUDA_EVENTREF ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_CUDA_CURESULT ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_LOCATIONREF ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_REGION_ID ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_PARENT_REGION_ID ) != 0 )
    {
      OTF2_CHECK( OTF2_GlobalDefWriter_WriteAttribute( tw->global_def_writer, self,
                                                     name, description, type ) );
    }
  }
  
  // increment attribute counter to append CASITA attributes
  tw->counterForAttributeId++;

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_RmaWin( 
                                                        void*          userData,
                                                        OTF2_RmaWinRef self,
                                                        OTF2_StringRef name,
                                                        OTF2_CommRef   comm )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteRmaWin( tw->global_def_writer, self,
                                                  name, comm ) );
  }

  return OTF2_CALLBACK_SUCCESS;

}

/******************************************************************************/
/* Callbacks to re-write enter/leave and communication records of original trace file.
 * Every callback has the writer object within @var{userData} and writes record 
 * immediately after reading. Enter and leave callbacks call "processNextNode()" 
 * to write node with metrics.
 */

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_MpiCollectiveEnd( 
                                              OTF2_LocationRef    locationID,
                                              OTF2_TimeStamp      time,
                                              uint64_t            eventPosition,
                                              void*               userData,
                                              OTF2_AttributeList* attributeList,
                                              OTF2_CollectiveOp   collectiveOp,
                                              OTF2_CommRef        communicator,
                                              uint32_t            root,
                                              uint64_t            sizeSent,
                                              uint64_t            sizeReceived )
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
OTF2ParallelTraceWriter::otf2CallbackComm_RmaOpCompleteBlocking( 
                                           OTF2_LocationRef    location,
                                           OTF2_TimeStamp      time,
                                           uint64_t            eventPosition,
                                           void*               userData,
                                           OTF2_AttributeList* attributeList,
                                           OTF2_RmaWinRef      win,
                                           uint64_t            matchingId )
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

/**
 * Callback for an enter region record.
 * 
 * @param location
 * @param time
 * @param eventPosition
 * @param userData
 * @param attributes
 * @param region
 * @return 
 */
OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackEnter( OTF2_LocationRef    location,
                                            OTF2_TimeStamp      time,
                                            uint64_t            eventPosition,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  /* Define event to write next node in list */
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = RECORD_ENTER;

  //if( tw->currentStream->getPeriod().second >= time - tw->timerOffset )
  if( tw->currentStream->getLastEventTime() >= time - tw->timerOffset )
  {
    //\todo: process attributes
    tw->processNextEvent( event, tw->getRegionName( region ) );
  }
  
  if ( tw->mpiSize > 1 && Parser::getInstance().getProgramOptions().analysisInterval &&
       time - tw->timerOffset == /*tw->currentStream->getPeriod().second*/
                                   tw->currentStream->getLastEventTime() )
  {
    return OTF2_CALLBACK_INTERRUPT;
  }

  return OTF2_CALLBACK_SUCCESS;

}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackLeave( OTF2_LocationRef    location, // streamID
                                            OTF2_TimeStamp      time,
                                            uint64_t            eventPosition,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  /* Define event to write next node in list */
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = RECORD_LEAVE;

  //std::cerr << "TW: Handle leave: " << tw->getRegionName( region ) << std::endl;
  
  //if( tw->currentStream->getPeriod().second >= time - tw->timerOffset )
  if( tw->currentStream->getLastEventTime() >= time - tw->timerOffset )
  {
    //\todo: process attributes
    tw->processNextEvent( event, tw->getRegionName( region ) );
  }
  
  // interrupt reading, if we processed the last read leave event
  if ( tw->mpiSize > 1 && Parser::getInstance().getProgramOptions().analysisInterval &&
       time - tw->timerOffset == /*tw->currentStream->getPeriod().second*/
                                   tw->currentStream->getLastEventTime() )
  {
    return OTF2_CALLBACK_INTERRUPT;
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_EvtReaderCallback_ThreadFork( 
                                  OTF2_LocationRef    locationID,
                                  OTF2_TimeStamp      time,
                                  uint64_t            eventPosition,
                                  void*               userData,
                                  OTF2_AttributeList* attributeList,
                                  OTF2_Paradigm       paradigm,
                                  uint32_t            numberOfRequestedThreads )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  // write next node in List
  OTF2Event event;
  event.location  = locationID;
  
  /* Fork/Join-RegionRef is created when definitions are read.
   * This event is processed because internal it is a node and counters have to
   * be calculated correctly (always happens between internal nodes).
   */
  event.regionRef = tw->ompForkJoinRef;
  event.time      = time;
  
  // mark as atomic to avoid unnecessary operations in processNextEvent())
  event.type      = RECORD_ATOMIC; 

  tw->processNextEvent( event, OTF2_OMP_FORKJOIN_INTERNAL );

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

  /* write next node in List */
  OTF2Event event;
  event.location  = locationID;
  /* Fork/Join-RegionRef is created when definitions are read.
   * This event is processed because internal it is a node and counters have to
   * be calculated correctly (always happens between internal nodes).
   */
  event.regionRef = tw->ompForkJoinRef;
  event.time      = time;
  event.type      = RECORD_ATOMIC;

  tw->processNextEvent( event, OTF2_OMP_FORKJOIN_INTERNAL );

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

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIrecvRequest( 
                                              OTF2_LocationRef    locationID,
                                              OTF2_TimeStamp      time,
                                              uint64_t            eventPosition,
                                              void*               userData,
                                              OTF2_AttributeList* attributeList,
                                              uint64_t            requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiIrecvRequest( tw->evt_writerMap[locationID],
                                                attributeList,
                                                time,
                                                requestID ) );
  }
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIrecv( OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                uint64_t         eventPosition,
                                                void*            userData,
                                                OTF2_AttributeList*
                                                attributeList,
                                                uint32_t         sender,
                                                OTF2_CommRef     communicator,
                                                uint32_t         msgTag,
                                                uint64_t         msgLength,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiIrecv( tw->evt_writerMap[locationID],
                                        attributeList, time, sender,
                                        communicator, msgTag, msgLength,
                                        requestID ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIsend( OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                uint64_t         eventPosition,
                                                void*            userData,
                                                OTF2_AttributeList*
                                                attributeList,
                                                uint32_t         receiver,
                                                OTF2_CommRef     communicator,
                                                uint32_t         msgTag,
                                                uint64_t         msgLength,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiIsend( tw->evt_writerMap[locationID],
                                        attributeList, time, receiver,
                                        communicator, msgTag, msgLength,
                                        requestID ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIsendComplete( 
                                                OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                uint64_t         eventPosition,
                                                void*            userData,
                                                OTF2_AttributeList*
                                                attributeList,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_MpiIsendComplete( tw->evt_writerMap[locationID],
                                                 attributeList, time, requestID ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}
