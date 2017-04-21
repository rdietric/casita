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
 * What this file does:
 * - read original OTF2 file and immediately write it out again, 
 * - add counter values and attributes for blame, CP, etc. to the trace
 * - copy definitions from original OTF2 file with MPI rank 0
 * - compute counter values for CPU events
 * - update statistics the statistics for the final rating
 *
 */

#include <mpi.h>
#include <cmath>
#include <iostream>
#include <stdlib.h>

// the following definition and include is needed for the printf PRIu64 macro
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <map>

#include "otf/OTF2ParallelTraceWriter.hpp"
#include <otf2/OTF2_MPI_Collectives.h>

#include "common.hpp"
#include "FunctionTable.hpp"
#include "Parser.hpp"

using namespace casita;
using namespace casita::io;

#define OTF2_CHECK( cmd ) \
  { \
   int _status = cmd; \
   if ( _status ) \
   { throw RTException( "OTF2 command '%s' returned error %d", #cmd, _status );} \
  }

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) \
    { throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
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

/**
 *
 *
 * @param mpiRank               MPIrank of this analysis process
 * @param mpiSize               Size of communicator
 * @param originalFilename      Name of original trace file
 * @param writeToFile           Write to new OTF2 file or just analysis
 * @param metrics               Analysis metrics to be written
 */
OTF2ParallelTraceWriter::OTF2ParallelTraceWriter( AnalysisEngine* analysis )
  :
    analysis( analysis ),
    mpiRank( analysis->getMPIRank() ),
    mpiSize( analysis->getMPISize() ),
    cTable( &( analysis->getCtrTable() ) ),
    otf2Archive( NULL ),
    otf2GlobalDefWriter( NULL ),
    otf2Reader( NULL ),
    otf2GlobalEventReader( NULL ),
    ompForkJoinRef( 0 ),
    deviceIdleRegRef( 0 ),
    deviceComputeIdleRegRef( 0 ),
    graph( NULL )
{
  flush_callbacks.otf2_post_flush = postFlush;
  flush_callbacks.otf2_pre_flush  = preFlush;

  commGroup = MPI_COMM_WORLD;
  
  // set the device reference counts to invalid
  deviceRefCount = -1;
  deviceComputeRefCount = -1;
  
  // set consecutive device communication count to zero
  deviceConsecutiveComCount = 0;
  //previousDeviceComTaskH2D = true;
  //currentDeviceComTaskH2D = true;
  
  firstOffloadApiEvtTime = UINT64_MAX;
  lastOffloadApiEvtTime = 0;
  
  open();
}

OTF2ParallelTraceWriter::~OTF2ParallelTraceWriter()
{
  
}

void
OTF2ParallelTraceWriter::open()
{
  // initialize OTF2 output members
  std::string outputFilename = Parser::getInstance().getOutArchiveName();
  std::string pathToFile     = Parser::getInstance().getPathToFile();
  
  writeToFile = Parser::getInstance().getProgramOptions().createTraceFile;
  
  // open OTF2 archive for writing and set flush and collective callbacks
  if ( writeToFile )
  {
    //\todo: needed?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    // open new otf2 file
    otf2Archive = OTF2_Archive_Open( pathToFile.c_str(), outputFilename.c_str(),
                                     OTF2_FILEMODE_WRITE, 
                                     1024 * 1024, 4 * 1024 * 1024, 
                                     OTF2_SUBSTRATE_POSIX,
                                     OTF2_COMPRESSION_NONE );

    OTF2_Archive_SetFlushCallbacks( otf2Archive, &flush_callbacks, NULL );

    // set collective callbacks to write trace in parallel
    OTF2_MPI_Archive_SetCollectiveCallbacks( otf2Archive, commGroup, MPI_COMM_NULL );
  }

  // initialize time and string definition members
  timerOffset     = 0;
  timerResolution = 0;
  
  // open OTF2 input trace
  const char* originalFilename = 
    Parser::getInstance().getProgramOptions().filename.c_str();

  otf2Reader = OTF2_Reader_Open( originalFilename );

  OTF2_MPI_Reader_SetCollectiveCallbacks( otf2Reader, commGroup );

  if ( !otf2Reader )
  {
    throw RTException( "Failed to open OTF2 trace file %s",
                        originalFilename );
  }

  // copy global definitions
  copyGlobalDefinitions();

  if( writeToFile )
  {
    //\todo: needed?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    
    writeAnalysisMetricDefinitions();
    
    //\todo: only for CUDA/OpenCL traces
    writeDeviceIdleDefinitions();
  }

  //\todo: needed?
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  setupGlobalEvtReader();
}

void
OTF2ParallelTraceWriter::close()
{
  OTF2_Reader_CloseGlobalEvtReader( otf2Reader, otf2GlobalEventReader );

  if ( writeToFile )
  {
    // close all opened event writer
    for ( std::map< uint64_t, OTF2_EvtWriter* >::iterator iter =
            evt_writerMap.begin();
          iter != evt_writerMap.end(); iter++ )
    {
      OTF2_Archive_CloseEvtWriter( otf2Archive, iter->second );
    }
  }
  
  OTF2_Reader_CloseEvtFiles( otf2Reader );
  
  OTF2_Reader_Close( otf2Reader );

  OTF2_CHECK( OTF2_Archive_Close( otf2Archive ) );
}

void
OTF2ParallelTraceWriter::reset()
{
  graph = NULL;
  
  // static metric information, and OTF2 definitions IDs
  // should no change between analysis intervals
  // keep cTable 
  
  // do not clear the activity stack, as activities might be active (not closed)
  // over interval boundaries
}

void
OTF2ParallelTraceWriter::handleFinalDeviceIdleLeave()
{
  if( lastOffloadApiEvtTime == 0 )
  {
    return;
  }
  
  // add device idle times to statistics
  analysis->getStatistics().addStatValue( OFLD_STAT_IDLE_TIME, 
    lastOffloadApiEvtTime - lastIdleStart );
  analysis->getStatistics().addStatValue( OFLD_STAT_COMPUTE_IDLE_TIME, 
    lastOffloadApiEvtTime - lastComputeIdleStart );
  
  // add offloading time
  if( lastOffloadApiEvtTime > firstOffloadApiEvtTime )
  {
    analysis->getStatistics().addStatValue( OFLD_STAT_OFLD_TIME, 
      lastOffloadApiEvtTime - firstOffloadApiEvtTime );
  }
  
  //\todo: finds the first device stream
  uint64_t streamId = analysis->getStreamGroup().getFirstDeviceStream( -1 )->getId();
  
  // make sure that we do not write an OTF2 event before the last written one
  if( lastOffloadApiEvtTime < streamStatusMap[ streamId ].lastEventTime )
  {
    lastOffloadApiEvtTime = streamStatusMap[ streamId ].lastEventTime;
  }

  if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle & 1 )
  {
    OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writerMap[ streamId ], NULL, 
                                      lastOffloadApiEvtTime, deviceIdleRegRef ) );
  }

  if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle  & (1 << 1) )
  {
    OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writerMap[ streamId ], NULL, 
                                      lastOffloadApiEvtTime, deviceComputeIdleRegRef ) );
  }

  // write idle leave only once
  lastOffloadApiEvtTime = 0;
}

/**
 * Clear the list of open edges and report remaining ones. These out edges are 
 * used to blame regions within the edge interval. Edges have to be intra 
 * stream edges. The list of out edges should be empty after a stream has been 
 * processed.
 */
void
OTF2ParallelTraceWriter::clearOpenEdges()
{
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "[%"PRIu32"] Clear open edges ...", mpiRank );
    
    for( StreamStatusMap::iterator mapIt = streamStatusMap.begin();
         mapIt != streamStatusMap.end(); ++mapIt )
    {
      OpenEdgesList& openEdges = mapIt->second.openEdges;
      if( openEdges.size() )
      {
        UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
                   "  [%"PRIu64"] Clear open edge(s)", mapIt->first );
        for ( OpenEdgesList::const_iterator edgeIter = openEdges.begin();
            edgeIter != openEdges.end(); ++edgeIter)
        {
          UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_ALL, 
                     "    %s", (*edgeIter)->getName().c_str() );
        }
        openEdges.clear();
      }
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
 * Get new OTF2 string reference based on the sorted property of the 
 * stringRefMap.
 * 
 * @param string string to generate a new OTF2 reference for
 * @return new OTF2 string reference
 */
uint32_t
OTF2ParallelTraceWriter::getNewStringRef( const char* string )
{
  uint32_t newStringRef = 1;
  
  if( !stringRefMap.empty() )
  {
    // get the largest string reference and add '1'
    newStringRef += stringRefMap.rbegin()->first;
  }
  
  stringRefMap[ newStringRef ] = string;
  
  return newStringRef;
}

/**
 * Get new OTF2 region reference based on the sorted property of the 
 * regionRefMap.
 * 
 * @param stringRef OTF2 string reference to generate a new OTF2 region reference for
 * @return new OTF2 region reference
 */
uint32_t
OTF2ParallelTraceWriter::getNewRegionRef( const char* string, 
                                          OTF2_Paradigm paradigm )
{
  uint32_t newRegionRef = 1;
  
  if( !regionInfoMap.empty() )
  {
    // get the largest region reference and add '1'
    newRegionRef += regionInfoMap.rbegin()->first;
  }
  
  RegionInfo regInf;
  regInf.name = string;
  regInf.paradigm = paradigm;
  regionInfoMap[ newRegionRef ] = regInf;
  
  return newRegionRef;
}

/**
 * Read definitions from original trace.
 * Write them to new one, if new OTF2 file is written.
 */
void
OTF2ParallelTraceWriter::copyGlobalDefinitions()
{
  if ( mpiRank == 0 && writeToFile )
  {
    otf2GlobalDefWriter = OTF2_Archive_GetGlobalDefWriter( otf2Archive );
  }

  OTF2_GlobalDefReader* global_def_reader =
    OTF2_Reader_GetGlobalDefReader( otf2Reader );

  OTF2_GlobalDefReaderCallbacks* global_def_callbacks =
    OTF2_GlobalDefReaderCallbacks_New();
  
  // these callbacks have to be registered for all processes 
  // (data are needed for analysis and internal processing)
  OTF2_GlobalDefReaderCallbacks_SetStringCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_String );
  OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Location );
  OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_ClockProperties );
  OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Region );
  
  // only needed, if we write an output trace (all processes have to register, 
  // because some information are needed for analysis)
  if( writeToFile )
  {
    OTF2_GlobalDefReaderCallbacks_SetAttributeCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_Attribute );
    
    OTF2_GlobalDefReaderCallbacks_SetMetricMemberCallback(
        global_def_callbacks, &otf2GlobalDefReaderCallback_MetricMember );
    
    OTF2_GlobalDefReaderCallbacks_SetMetricClassCallback(
        global_def_callbacks, &otf2GlobalDefReaderCallback_MetricClass );
    
    OTF2_GlobalDefReaderCallbacks_SetMetricInstanceCallback(
        global_def_callbacks, &otf2GlobalDefReaderCallback_MetricInstance );
    
    // just a write through by the root process
    if ( mpiRank == 0 )
    {
      OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(
          global_def_callbacks, &OTF2_GlobalDefReaderCallback_LocationGroup );

      OTF2_GlobalDefReaderCallbacks_SetGroupCallback(
          global_def_callbacks, &OTF2_GlobalDefReaderCallback_Group );

      OTF2_GlobalDefReaderCallbacks_SetCommCallback(
        global_def_callbacks, &OTF2_GlobalDefReaderCallback_Comm );

      OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeCallback(
        global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNode );

      OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodePropertyCallback(
        global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty );

      OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeDomainCallback(
        global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain );

      OTF2_GlobalDefReaderCallbacks_SetRmaWinCallback(
        global_def_callbacks, &OTF2_GlobalDefReaderCallback_RmaWin );
    }
  }

  // register callbacks
  OTF2_Reader_RegisterGlobalDefCallbacks( otf2Reader,
                                          global_def_reader,
                                          global_def_callbacks,
                                          this );

  OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );

  // read definitions
  uint64_t definitions_read = 0;
  OTF2_Reader_ReadAllGlobalDefinitions( otf2Reader,
                                        global_def_reader,
                                        &definitions_read );

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] Trace writer: Read/wrote %"PRIu64" definitions", 
             definitions_read );

  // add internal fork/join "region", 
  // no string entry needed as respective definition will not be written
  ompForkJoinRef = getNewRegionRef( OTF2_OMP_FORKJOIN_INTERNAL, OTF2_PARADIGM_OPENMP );
}

void
OTF2ParallelTraceWriter::writeDeviceIdleDefinitions()
{
  // get compute region references (needed by all processes)
  deviceIdleRegRef = getNewRegionRef( "deviceIdle", OTF2_PARADIGM_UNKNOWN );
  deviceComputeIdleRegRef = 
    getNewRegionRef( "deviceComputeIdle", OTF2_PARADIGM_UNKNOWN );
  
  if ( mpiRank == 0 )
  {  
    if( Parser::getInstance().getProgramOptions().deviceIdle & 1 )
    {
      uint32_t newStringRef = getNewStringRef( "deviceIdle" );
      
      OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( otf2GlobalDefWriter,
                                                    newStringRef,
                                                    stringRefMap[ newStringRef ] ) );

      OTF2_CHECK( OTF2_GlobalDefWriter_WriteRegion( otf2GlobalDefWriter,
                                      deviceIdleRegRef,
                                      newStringRef,
                                      newStringRef,
                                      OTF2_UNDEFINED_STRING,
                                      OTF2_REGION_ROLE_ARTIFICIAL,
                                      analysis->haveParadigm( PARADIGM_OCL ) ? 
                                        OTF2_PARADIGM_OPENCL : OTF2_PARADIGM_CUDA,
                                      OTF2_REGION_FLAG_NONE,
                                      OTF2_UNDEFINED_STRING,
                                      0, 0 ) );
    }
    
    if( Parser::getInstance().getProgramOptions().deviceIdle & (1 << 1) )
    {
      uint32_t newStringRef = getNewStringRef( "deviceComputeIdle" );
      
      OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( otf2GlobalDefWriter,
                                                    newStringRef,
                                                    stringRefMap[ newStringRef ] ) );

      OTF2_CHECK( OTF2_GlobalDefWriter_WriteRegion( otf2GlobalDefWriter,
                                      deviceComputeIdleRegRef,
                                      newStringRef,
                                      newStringRef,
                                      OTF2_UNDEFINED_STRING,
                                      OTF2_REGION_ROLE_ARTIFICIAL,
                                      analysis->haveParadigm( PARADIGM_OCL ) ? 
                                        OTF2_PARADIGM_OPENCL : OTF2_PARADIGM_CUDA,
                                      OTF2_REGION_FLAG_NONE,
                                      OTF2_UNDEFINED_STRING,
                                      0, 0 ) );
    }
  }
}

/**
 * Write definitions for self-defined (analysis) metrics to output trace file.
 */
void
OTF2ParallelTraceWriter::writeAnalysisMetricDefinitions()
{
  for ( size_t i = 0; i < NUM_OUTPUT_METRICS; ++i )
  {
    MetricType metric = (MetricType) i;
    const MetricEntry* entry = cTable->getMetric( metric );

    // ignore internal metrics
    if ( !(entry->isInternal) )
    {
      // get new string references
      uint32_t newStringRef = 1;
      if( !stringRefMap.empty() )
      {
        // get the largest string reference and add '1'
        newStringRef += stringRefMap.rbegin()->first;
      }
      stringRefMap[ newStringRef ] = entry->name;
      stringRefMap[ newStringRef + 1 ] = entry->description;
      
      // only the root rank writes the global definitions
      if ( mpiRank == 0 )
      {
        //UTILS_MSG(true, "Write definition: %s", entry->name );
        
        // write string definition for metric and/or attribute name
        OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( otf2GlobalDefWriter,
                                                      newStringRef,
                                                      entry->name ) );
        
        
        // write string definition for metric and/or attribute description
        OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( otf2GlobalDefWriter,
                                                      newStringRef+1,
                                                      entry->description ) );
        
        
        if( entry->metricMode == ATTRIBUTE )
        {
          uint32_t newAttrId = cTable->newOtf2Id( metric );

          OTF2_CHECK( 
            OTF2_GlobalDefWriter_WriteAttribute( otf2GlobalDefWriter, newAttrId, 
                                                 newStringRef,
                                                 newStringRef+1,
                                                 OTF2_TYPE_UINT64 ) );
        }
        else if( entry->metricMode != METRIC_MODE_UNKNOWN )
        {
          uint32_t newMetricMemberId = cTable->getNewMetricMemberId();
          uint32_t newMetricClassId = cTable->newOtf2Id( metric );
          
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
            OTF2_GlobalDefWriter_WriteMetricMember( otf2GlobalDefWriter, newMetricMemberId,
                                                    newStringRef,
                                                    newStringRef+1,
                                                    OTF2_METRIC_TYPE_USER,
                                                    otf2MetricMode,
                                                    OTF2_TYPE_UINT64,
                                                    OTF2_BASE_DECIMAL, 0, 0 ) );

          OTF2_CHECK( 
            OTF2_GlobalDefWriter_WriteMetricClass( otf2GlobalDefWriter, newMetricClassId, 1,
                                                   &newMetricMemberId,
                                                   OTF2_METRIC_ASYNCHRONOUS,
                                                   OTF2_RECORDER_KIND_ABSTRACT ) );
        }
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

/**
 * Read all events from original trace in chronological order over all local 
 * processes. Therefore, select all local locations and register event callbacks.
 * 
 * This function should be called only once!
 */
void
OTF2ParallelTraceWriter::setupGlobalEvtReader()
{
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_ANNOY, 
             "[%"PRIu32"] Setup global event reader", mpiRank );
  
  
  for( LocationParentMap::const_iterator it = locationParentMap.begin();
       it != locationParentMap.end(); ++it )
  {
    if ( writeToFile )
    {
      OTF2_EvtWriter* evt_writer = OTF2_Archive_GetEvtWriter(
        otf2Archive, it->first /*OTF2_UNDEFINED_LOCATION*/ );

      //OTF2_CHECK( OTF2_EvtWriter_SetLocationID( evt_writer, processId ) );
      evt_writerMap[ it->first ] = evt_writer;
    }
    
    // Tell writer to read this location
    OTF2_Reader_SelectLocation( otf2Reader, it->first );
  }

  // open selected location files
  bool successful_open_def_files = 
    OTF2_Reader_OpenDefFiles( otf2Reader ) == OTF2_SUCCESS;
  OTF2_Reader_OpenEvtFiles( otf2Reader );
  
  for( LocationParentMap::const_iterator it = locationParentMap.begin();
       it != locationParentMap.end(); ++it )
  {
    if ( successful_open_def_files )
    {
      // read location definitions to set mapping tables and time offsets
      OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( otf2Reader, it->first );
      if ( def_reader )
      {
        uint64_t def_reads = 0;
        OTF2_Reader_ReadAllLocalDefinitions( otf2Reader, def_reader, &def_reads );
        OTF2_Reader_CloseDefReader( otf2Reader, def_reader );
      }
    }
    
    // select event reader
    OTF2_Reader_GetEvtReader( otf2Reader, it->first );
  }
  
  if ( successful_open_def_files )
  {
    OTF2_Reader_CloseDefFiles( otf2Reader );
  }
  
  registerEventCallbacks();
  
  otf2GlobalEventReader = OTF2_Reader_GetGlobalEvtReader( otf2Reader );
}

void
OTF2ParallelTraceWriter::registerEventCallbacks()
{
  // the global event reader contains all previously opened local event readers
  OTF2_GlobalEvtReader* evt_reader = OTF2_Reader_GetGlobalEvtReader( otf2Reader );
  
  OTF2_GlobalEvtReaderCallbacks* event_callbacks = 
    OTF2_GlobalEvtReaderCallbacks_New();
  OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks, 
                                                  &otf2CallbackEnter );
  OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks, 
                                                  &otf2CallbackLeave );
  
  OTF2_GlobalEvtReaderCallbacks_SetThreadForkCallback(
    event_callbacks, &otf2EvtCallbackThreadFork );
  OTF2_GlobalEvtReaderCallbacks_SetThreadJoinCallback(
    event_callbacks, &otf2EvtCallbackThreadJoin );
  
  // the following callback events are just written back to the output trace
  if( writeToFile )
  {
    OTF2_GlobalEvtReaderCallbacks_SetMetricCallback( event_callbacks, 
                                                     &otf2CallbackMetric );
    
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveBeginCallback(
      event_callbacks, &otf2CallbackComm_MpiCollectiveBegin );
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback(
      event_callbacks, &otf2CallbackComm_MpiCollectiveEnd );
    OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback( event_callbacks, 
                                                &otf2Callback_MpiRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback( event_callbacks, 
                                                &otf2Callback_MpiSend );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvRequestCallback( 
      event_callbacks, &otf2Callback_MpiIrecvRequest );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvCallback( event_callbacks, 
                                                 &otf2Callback_MpiIrecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCallback( event_callbacks, 
                                                 &otf2Callback_MpiIsend );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCompleteCallback( 
      event_callbacks, &otf2Callback_MpiIsendComplete );

    OTF2_GlobalEvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(
      event_callbacks, &otf2CallbackComm_RmaOpCompleteBlocking );
    OTF2_GlobalEvtReaderCallbacks_SetRmaWinCreateCallback(
      event_callbacks, &otf2CallbackComm_RmaWinCreate );
    OTF2_GlobalEvtReaderCallbacks_SetRmaWinDestroyCallback(
      event_callbacks, &otf2CallbackComm_RmaWinDestroy );
    OTF2_GlobalEvtReaderCallbacks_SetRmaGetCallback( event_callbacks, 
                                               &otf2CallbackComm_RmaGet );
    OTF2_GlobalEvtReaderCallbacks_SetRmaPutCallback( event_callbacks, 
                                               &otf2CallbackComm_RmaPut );
    OTF2_GlobalEvtReaderCallbacks_SetThreadTeamBeginCallback(
      event_callbacks, &otf2CallbackComm_ThreadTeamBegin );
    OTF2_GlobalEvtReaderCallbacks_SetThreadTeamEndCallback(
      event_callbacks, &otf2CallbackComm_ThreadTeamEnd );
  }

  OTF2_Reader_RegisterGlobalEvtCallbacks( otf2Reader, evt_reader, event_callbacks, this );
  OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );
}

uint64_t
OTF2ParallelTraceWriter::writeLocations( const uint64_t eventsToRead )
{
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
             "[%"PRIu32"] Write streams ...", mpiRank );
  
  // reset "per interval" values in the trace writer
  reset();

  // \todo: needed?
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  // check whether this is the first call of this function (set initial onCP value)
  static bool firstCall = true;
  
  // iterate over all streams that have been analyzed
  const EventStreamGroup::EventStreamList streams = analysis->getStreams();
  for( EventStreamGroup::EventStreamList::const_iterator itStream = streams.begin();
       itStream != streams.end(); ++itStream )
  {
    EventStream* stream = *itStream;
    const uint64_t streamId = stream->getId();
    
    // when called the first time for this stream ID, the map entry is generated
    streamStatusMap[ streamId ].stream = stream;
    StreamStatus& streamState = streamStatusMap[ streamId ];
    
    EventStream::SortedGraphNodeList* processNodes = &( stream->getNodes() );
    UTILS_ASSERT( processNodes, "No nodes for stream %"PRIu64" found!", streamId );

    EventStream::SortedGraphNodeList::iterator currentNodeIter = 
      processNodes->begin();

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
                   "[%"PRIu32"] TraceWriter: Skip atomic node: %s", 
                   mpiRank, (*currentNodeIter)->getUniqueName().c_str() );

        // first part of the condition should be wrong for the global source node
        // if current node is on the CP, but the following is not
        if( ( (*currentNodeIter)->getCounter( CRITICAL_PATH, NULL ) == 1 ) &&
            ( currentNodeIter + 1 != processNodes->end() ) && 
            ( *( currentNodeIter + 1 ) )->getCounter( CRITICAL_PATH, NULL ) == 0 )
        {
          OTF2_Type        type  = OTF2_TYPE_UINT64;
          OTF2_MetricValue value;
          OTF2_EvtWriter*  evt_writer = evt_writerMap[ streamId ];

          value.unsigned_int = 0;

          // we need the metric instance/class ID
          OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, 
                                             streamState.lastEventTime,
                                             cTable->getMetricId( CRITICAL_PATH ), 
                                             1, &type, &value ) );
          streamState.onCriticalPath = false;

          //UTILS_MSG( true, "[%"PRIu32"] Write CP =0 (node %s)", 
          //           mpiRank, (*currentNodeIter)->getUniqueName().c_str() );
        }

        ++currentNodeIter;
      }
    }

    // set the current node iterator for this stream
    streamState.currentNodeIter = currentNodeIter;

    // store a pointer to the graph as class member (global for all local streams)
    this->graph = &( analysis->getGraph() );

    /* reset last counter values before processing the current stream
    const AnalysisMetric::MetricIdSet& ctrIdSet = cTable->getAllCounterIds();
    for( AnalysisMetric::MetricIdSet::const_iterator ctrIdIter = ctrIdSet.begin();
         ctrIdIter != ctrIdSet.end(); ++ctrIdIter )
    {
      streamState.lastMetricValues[ *ctrIdIter ] = 0;
    }*/
    
    // set the initial critical path value if this is the first call of this function
    if( firstCall ) 
    {
      streamState.isFilterOn = false;
      
      // set the initial critical path value for this stream
      // (only in the first call of this function)
      if( stream->isFirstCritical() )
      {
        streamState.onCriticalPath = true;
        //UTILS_MSG( true, "Process [%llu] has initial CP", stream->getId());

        // after the first interval analysis there is no first critical any more
        stream->isFirstCritical() = false;
      }
      else
      {
        //UTILS_WARNING( "[%"PRIu64"] Set CP=0", streamState.stream->getId() );
        streamState.onCriticalPath = false;
      }
    }
    
    //UTILS_WARNING( "%llu:%s on CP %d", streamId, stream->getName(), 
    //                 streamState.onCriticalPath );
    
    // this affects the blame distribution compared to the non-interval run!!!
    if( streamState.openEdges.size() > 0 )
    {
      // clear list of open edges
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
                 " [%"PRIu64"] Clear open edge(s).", 
                 streamState.stream->getId() );
      
      streamState.openEdges.clear();
    }
    
    // hint: the activity stack is preserved!
  }
  
  firstCall = false;
  
  assert( otf2GlobalEventReader );

  // returns 0 if successful, >0 otherwise
  uint64_t events_read = 0;
  OTF2_ErrorCode otf2_error = OTF2_Reader_ReadGlobalEvents( 
    otf2Reader, otf2GlobalEventReader, eventsToRead, &events_read );
  
  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() > VERBOSE_BASIC, 
             "[0] Writer: Read %"PRIu64" / %"PRIu64" events", 
             events_read, eventsToRead );
  
  if ( OTF2_SUCCESS != otf2_error )
  {
    /*if( OTF2_ERROR_INTERRUPTED_BY_CALLBACK == otf2_error )
    {
      UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "[0] Writer interrupted by callback: Read %lu events", 
                 events_read );
      return events_read;
    }
    else*/
    {
      throw RTException( "Failed to read OTF2 events %llu", events_read );
    }
  }  
  
  return events_read;
}

/**
 * Returns the name of a region as a string. (used for every enter/leave event)
 *
 * @param regionRef     ID of region the name is requested for
 * @return              string with Name of the region
 */
RegionInfo&
OTF2ParallelTraceWriter::getRegionInfo( const OTF2_RegionRef regionRef )
{
  UTILS_ASSERT( regionInfoMap.count( regionRef ) > 0,
                "Could not find region reference!" )
  
  return regionInfoMap[ regionRef ];
}

/**
 * Collect statistical information for activity groups that is used later to 
 * create the profile.
 *
 * @param event         current event that was read from original OTF2 file
 * @param counters      counter values for that event
 */
void
OTF2ParallelTraceWriter::updateActivityGroupMap( OTF2Event event, 
                                                 CounterMap& counters )
{
  // add function to list if not present yet
  if ( activityGroupMap.find( event.regionRef ) == activityGroupMap.end() )
  {
    activityGroupMap[ event.regionRef ].functionId        = event.regionRef;
    activityGroupMap[ event.regionRef ].numInstances      = 0;
    activityGroupMap[ event.regionRef ].totalBlame        = 0;
    activityGroupMap[ event.regionRef ].blameOnCP         = 0;
    activityGroupMap[ event.regionRef ].totalDuration     = 0;
    activityGroupMap[ event.regionRef ].totalDurationOnCP = 0;
    
    /*UTILS_MSG( strcmp( getRegionName(event.regionRef).c_str(), "clFinish" ) == 0, 
               "[%u] Add %s to activity group map", 
               mpiRank, getRegionName(event.regionRef).c_str() );*/
  }

  // for each enter event, increase the number of instances found
  if ( event.type == RECORD_ENTER )
  {
    activityGroupMap[ event.regionRef ].numInstances++;
  }

  UTILS_ASSERT( streamStatusMap.count( event.location ) > 0, 
                "Could not find stream status!" );
  
  bool onCP = false;
  
  // if there are counters (nodes) available, use this for the critical path
  if( counters.count( CRITICAL_PATH ) > 0 )
  {
    onCP = counters[ CRITICAL_PATH ];
  }
  else
  {
    onCP = streamStatusMap[ event.location ].onCriticalPath;
  }
  
  // add duration, CP time and blame to current function on stack
  //if ( activityIter != activityStack.end() && activityIter->second.size() > 0 )
  if( streamStatusMap[ event.location ].activityStack.size() > 0 )
  {
    uint32_t currentActivity = 
      streamStatusMap[ event.location ].activityStack.top();
    
    // time between the last and the current event
    uint64_t timeDiff = 
      event.time - streamStatusMap[ event.location ].lastEventTime;
    
    uint64_t blame = 0;
    if( counters.count( BLAME ) )
    {
      blame = counters[ BLAME ];
      activityGroupMap[ currentActivity ].totalBlame += blame;
    }
    
    if( counters.count( WAITING_TIME ) )
    {
      activityGroupMap[ currentActivity ].waitingTime += counters[ WAITING_TIME ];
    }

    activityGroupMap[ currentActivity ].totalDuration += timeDiff;
    
    if( onCP )
    {
      activityGroupMap[ currentActivity ].totalDurationOnCP += timeDiff;
      activityGroupMap[ currentActivity ].blameOnCP         += blame;
    }
    
    /*UTILS_MSG( strcmp( getRegionName(event.regionRef).c_str(), "clFinish" ) == 0,
               "[%u] %s (type %d): on stack %s\t (%d) (Real-time: %lf), onCP: %d, "
               "blame: %llu, total blame: %llu", 
               mpiRank, getRegionName(event.regionRef).c_str(), event.type, 
               getRegionName(currentActivity).c_str(),
               activityStack[event.location].size(),
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

  // iterate over all open edges (if any) and calculate total blame
  if( streamStatusMap.count( event.location ) > 0 )
  {
    StreamStatus& streamState = streamStatusMap[ event.location ];
    
    // time between current and last event on this location
    uint64_t timeDiff = event.time - streamState.lastEventTime;

    // remove timer offset from event time
    uint64_t eventTime = event.time - timerOffset;

    //UTILS_MSG( openEdges.size(), "[%u] Compute blame for %s from %llu open edges", 
    //           mpiRank, getRegionName( event.regionRef ).c_str(), openEdges.size() );
    
    OpenEdgesList& openEdges = streamState.openEdges;
    for ( OpenEdgesList::iterator edgeIter = openEdges.begin();
          edgeIter != openEdges.end(); )
    {
      Edge* edge = *edgeIter;

      //std::cerr << getRegionName(event.regionRef) << " between nodes " << edge->getStartNode()->getUniqueName()
      //          << " -> " << edge->getEndNode()->getUniqueName() << std::endl;

      // if edge has duration AND event is in between the edge
      if ( ( edge->getDuration() > 0 ) &&
           ( edge->getEndNode()->getTime() > eventTime ) &&
           ( edge->getStartNode()->getTime() < eventTime ) )
      {
        // blame = blame(edge) * time(cpu_region)/time(edge)
        totalBlame += (double)( edge->getCPUBlame() ) * (double)timeDiff 
                    / (double)( edge->getDuration() );
      
        ++edgeIter;
      }
      else
      {
        // erase edge if event time is past its end node
        edgeIter = openEdges.erase( edgeIter );
      }
    }
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
OTF2ParallelTraceWriter::writeEventsWithAttributes( OTF2Event event, 
                                                    OTF2_AttributeList* attributes,
                                                    CounterMap& counters )
{
  UTILS_ASSERT( evt_writerMap.find( event.location ) != evt_writerMap.end(),
                "Could not find OTF2 event writer for location" );
  
  OTF2_EvtWriter* evt_writer = evt_writerMap[ event.location ];
  
  // for all available metrics
  for ( CounterMap::const_iterator iter = counters.begin();
        iter != counters.end(); ++iter )
  {
    const MetricType metricType = iter->first;
    
    // skip the critical path attribute, as it is "cheaper" to write a counter, 
    // whenever the critical path changes instead of to every region
    
    // ignore all metrics, but attributes
    if( cTable->getMetric( metricType )->metricMode != ATTRIBUTE )
    {
      continue;
    }

    // all metrics are definitely assigned to leave nodes
    if( event.type == RECORD_LEAVE && iter->second != 0 )
    {
      if( attributes == NULL )
      {
        attributes = OTF2_AttributeList_New();
        UTILS_WARNING( "Create new attribute list, as event does not provide one!" );
      }
      
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
OTF2ParallelTraceWriter::writeCounterMetrics( OTF2Event event, 
                                              CounterMap& counters )
{
  UTILS_ASSERT( evt_writerMap.find( event.location ) != evt_writerMap.end(),
                "Could not find OTF2 event writer for location" );
  
  OTF2_EvtWriter* evt_writer = evt_writerMap[ event.location ];
  /*
  UTILS_MSG( event.type == RECORD_ENTER && 
             strcmp( getRegionName(event.regionRef).c_str(), "BUFFER FLUSH" ) == 0, 
             "BUFFER FLUSH enter: '%s' stack %d (time: %llu), #counters: %llu", 
             getRegionName(event.regionRef).c_str(), 
             activityStack[event.location].size(), event.time, counters.size() );
  */
  UTILS_ASSERT( streamStatusMap.count( event.location ) > 0, 
                "Could not find stream status!" );
  
  StreamStatus& streamState = streamStatusMap[ event.location ];

  // if the stream is on the critical path, but no more graph nodes available
  // and therefore the counter table is empty (can happen on host processes)
  if( counters.size() == 0 && streamState.onCriticalPath == true )
  {
    // if we are at a leave event, which is the last on the stack, write '0'
    if( event.type == RECORD_LEAVE && 
        streamState.activityStack.size() == 1 && 
        streamState.lastMetricValues[ CRITICAL_PATH ] != 0 )
    {
      OTF2_Type type = OTF2_TYPE_UINT64;
      
      OTF2_MetricValue value;
      value.unsigned_int = 0;
      streamState.lastMetricValues[ CRITICAL_PATH ] = 0;
      
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( CRITICAL_PATH ), 
                                         1, &type, &value ) );
      return;
    }
    
    if( event.type == RECORD_ENTER && /*streamState.activityStack.size() == 0*/ 
        streamState.lastMetricValues[ CRITICAL_PATH ] == 0 )
    {
      OTF2_Type type = OTF2_TYPE_UINT64;
      
      OTF2_MetricValue value;
      value.unsigned_int = 1;
      streamState.lastMetricValues[ CRITICAL_PATH ] = 1;
      
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( CRITICAL_PATH ), 
                                         1, &type, &value ) );
    }
    
    return;
  }

  // if counters are available
  for ( CounterMap::const_iterator iter = counters.begin();
        iter != counters.end(); ++iter )
  {
    const MetricType metricType = iter->first;
    
    MetricMode metricMode = cTable->getMetric( metricType )->metricMode;
    
    OTF2_Type        type = OTF2_TYPE_UINT64;
    OTF2_MetricValue value;
    
    // ignore attributes (waiting time)
    if( metricMode == ATTRIBUTE )
      continue;
    
    // critical path counter, absolute next mode
    if( CRITICAL_PATH == metricType )
    {
      // set counter to '0' for last leave event on the stack, if last counter 
      // value is not already '0' for this location (applies to CUDA kernels)
      if( event.type == RECORD_LEAVE && streamState.activityStack.size() == 1 &&
          streamState.lastMetricValues[ CRITICAL_PATH ] != 0 )
      {
        value.unsigned_int = 0;
        streamState.lastMetricValues[ CRITICAL_PATH ] = 0;
      
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( CRITICAL_PATH ), 
                                           1, &type, &value ) );
        continue;
      }
      
      uint64_t onCP = streamState.onCriticalPath;
      if( streamState.lastMetricValues[ CRITICAL_PATH ] != onCP )
      {
        value.unsigned_int = onCP;
        streamState.lastMetricValues[ CRITICAL_PATH ] = onCP;
        
        OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                           cTable->getMetricId( CRITICAL_PATH ), 
                                           1, &type, &value ) );
      }

      continue;
    }
    // END: critical path counter
    
    // The following is currently only for the blame counter
    
    // reset counter if this enter is the first event on the activity stack
    if ( event.type == RECORD_ENTER && streamState.activityStack.size() == 0 )
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
    if ( event.type == RECORD_LEAVE && streamState.activityStack.size() == 1 &&
         value.unsigned_int != 0 )
    {
      value.unsigned_int = 0;
      OTF2_CHECK( OTF2_EvtWriter_Metric( evt_writer, NULL, event.time,
                                         cTable->getMetricId( metricType ), 
                                         1, &type, &value ) );
    }
  }
}

/**
 * Additional handling of device task enter events.
 * 
 * @param time event time
 * @param location event location
 * @param isCompute true, if it is a compute task, otherwise false
 */
void
OTF2ParallelTraceWriter::handleDeviceTaskEnter( uint64_t time, 
                                                OTF2_LocationRef location, 
                                                bool isCompute, bool isH2D )
{
  if( deviceRefCount == 0 )
  {
    // add device idle time to statistics
    analysis->getStatistics().addStatValue( OFLD_STAT_IDLE_TIME, 
                                                     time - lastIdleStart );

    deviceRefCount = 1;
    
    if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle & 1 )
    {
      // something is happening on the device again, leave idle region
      int deviceId = analysis->getStream( location )->getDeviceId();
      EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
      OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writerMap[ stream->getId() ], NULL, 
                                        time, deviceIdleRegRef ) );
    }
  }
  else
  {
    deviceRefCount++;
  }
  
  // if this is a device compute task
  if( isCompute )
  {
    if( deviceComputeRefCount == 0 )
    {
      // add device idle time to statistics
      analysis->getStatistics().addStatValue( OFLD_STAT_COMPUTE_IDLE_TIME, 
                                              time - lastComputeIdleStart );

      deviceComputeRefCount = 1;
      
      if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle  
          & (1 << 1) )
      {
        // something is happening on the device again, leave idle region
        int deviceId = analysis->getStream( location )->getDeviceId();
        EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
        OTF2_CHECK( OTF2_EvtWriter_Leave( evt_writerMap[ stream->getId() ], NULL, 
                                          time, deviceComputeIdleRegRef ) );
      }
    }
    else
    {
      deviceComputeRefCount++;
    }
    
    // reset count
    deviceConsecutiveComCount = 0;
  }
  else
  {
    previousDeviceComTaskH2D   = currentDeviceComTaskH2D;
    currentDeviceComTaskH2D    = isH2D;
    lastDeviceComTaskEnterTime = time;
  }
}

/**
 * Additional handling of device task leave events.
 * 
 * @param time event time
 * @param location event location
 * @param isCompute true, if it is a compute task, otherwise false
 */
void
OTF2ParallelTraceWriter::handleDeviceTaskLeave( uint64_t time, 
                                                OTF2_LocationRef location, 
                                                bool isCompute )
{
  // write the compute idle enter first
  if( isCompute )
  {
    deviceComputeRefCount--;

    if( deviceComputeRefCount == 0 )
    {
      //save time
      lastComputeIdleStart = time;
      
      if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle  
          & (1 << 1) )
      {
        // write OTF2 device compute idle region
        int deviceId = analysis->getStream( location )->getDeviceId();
        EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
        OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writerMap[ stream->getId() ], NULL, 
                                          time, deviceComputeIdleRegRef ) );
      }
    }
    
    deviceConsecutiveComCount = 0;
  }
  else // communication task
  {
    // the previous device communication task has the same direction
    if( currentDeviceComTaskH2D == previousDeviceComTaskH2D )
    {
      // if the previous device task was a communication
      if( deviceConsecutiveComCount > 0 )
      {
        // add this consecutive communication to stats
        analysis->getStatistics().addStatWithCount( 
          OFLD_STAT_MULTIPLE_COM, time - lastDeviceComTaskEnterTime );
      }

      deviceConsecutiveComCount++;
    }
    else
    {
      deviceConsecutiveComCount = 1;
    }
  }
  
  deviceRefCount--;

  if( deviceRefCount == 0 )
  {
    //save time
    lastIdleStart = time;
    
    if( writeToFile && Parser::getInstance().getProgramOptions().deviceIdle & 1 )
    {
      // write OTF2 idle enter
      int deviceId = analysis->getStream( location )->getDeviceId();
      EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
      OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writerMap[ stream->getId() ], NULL, 
                                        time, deviceIdleRegRef ) );
    }
  }
}

/**
 * Process the next event read from original trace file.
 *
 * @param event
 * @param attributeList
 * 
 * @return a pointer to the GraphNode object of the given event
 */
void
OTF2ParallelTraceWriter::processNextEvent( OTF2Event event, 
                                           OTF2_AttributeList* attributeList )
{  
  RegionInfo& regionInfo = getRegionInfo( event.regionRef );  
  const char* eventName = regionInfo.name;
  
  UTILS_ASSERT( streamStatusMap.count( event.location ) > 0, 
                "Could not find stream status!" );
  StreamStatus& streamState = streamStatusMap[ event.location ];
  EventStream* currentStream = streamState.stream;
  
  // test if this is an internal node or a CPU event
  FunctionDescriptor eventDesc;
  // set event type to determine if an internal node is available
  eventDesc.recordType = event.type; 
  const bool mapsInternalNode = FunctionTable::getAPIFunctionType(
    eventName, &eventDesc, currentStream->isDeviceStream(), 
    analysis->haveDeviceNullStreamOnly() );  

  //UTILS_MSG( mpiRank == 0, "Event name: '%s' (%d), maps internal: %d", 
  //           eventName.c_str(), event.type, (int)mapsInternalNode );

  // non-internal counter values for this event
  CounterMap tmpCounters;
  
  EventStream::SortedGraphNodeList::iterator endNodeIter = 
    currentStream->getNodes().end();
  EventStream::SortedGraphNodeList::iterator currentNodeIter = 
    streamState.currentNodeIter;

  // if this is a node we are using for analysis
  if ( mapsInternalNode )
  {
    // if we are after the end of the node list
    if ( currentNodeIter == endNodeIter )
    {
      UTILS_MSG( true, "[%u] OTF2 writer: More events than nodes! "
                       "(%s (%" PRIu64 "): %s (%d) at %" PRIu64 ")", 
                 mpiRank, currentStream->getName(), event.location, 
                 eventName, event.type, getRealTime( event.time ) );
    }
    else
    {
      GraphNode* currentNode = *currentNodeIter;
      
      // special handling for offloading API routines
      if( currentNode->isOffload() )
      {
        // do not write attributes from CUDA and OpenCL nodes
        OTF2_AttributeList_RemoveAllAttributes( attributeList );
        
        // write enter event for device idle regions at first occurrence of 
        // a device task triggering or synchronizing offloading API routine
        if( deviceRefCount < 0 )
        {
          deviceRefCount = 0;
          deviceComputeRefCount = 0;

          lastIdleStart = event.time;
          lastComputeIdleStart = event.time;

          // compute idle has to be written first (includes device idle)
          if( Parser::getInstance().getProgramOptions().deviceIdle & (1 << 1) )
          {
            // write compute idle enter
            //\todo: deviceId will be -1
            int deviceId = analysis->getStream( event.location )->getDeviceId();
            EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
            OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writerMap[ stream->getId() ], NULL, 
                                              event.time, deviceComputeIdleRegRef ) );
          }

          // device idle
          if( Parser::getInstance().getProgramOptions().deviceIdle & 1 )
          {
            // write OTF2 idle enter
            int deviceId = analysis->getStream( event.location )->getDeviceId();
            EventStream* stream = analysis->getStreamGroup().getFirstDeviceStream( deviceId );
            OTF2_CHECK( OTF2_EvtWriter_Enter( evt_writerMap[ stream->getId() ], NULL, 
                                              event.time, deviceIdleRegRef ) );
          }

          firstOffloadApiEvtTime = event.time;
        }

        // remember last event time for offloading API functions (not BUFFER_FLUSH)
        // to write offloading idle leave events
        if( regionInfo.role == OTF2_REGION_ROLE_WRAPPER )
        {
          lastOffloadApiEvtTime = event.time;
        }
        
        // at kernel launch leave when the device is compute idle
        if( currentNode->isLeave() && deviceComputeRefCount == 0 &&
            currentNode->isOffloadEnqueueKernel() )
        {
          GraphNode* launchEnter = currentNode->getGraphPair().first;
          if( launchEnter && launchEnter->getLink() )
          {
            uint64_t knStartTime = launchEnter->getLink()->getTime();
            
            if( knStartTime > currentNode->getTime() )
            {
              analysis->getStatistics().addStatWithCount( OFLD_STAT_KERNEL_START_DELAY, 
                knStartTime - currentNode->getTime() );
            }
          }
        }
      }
      else
      // reset consecutive communication count at MPI leave nodes 
      // (assumes that offloading is used in between MPI operations) 
      if( currentNode->isMPI() && currentNode->isLeave() )
      {
        deviceConsecutiveComCount = 0;
      }

      UTILS_ASSERT( currentNode->getFunctionId() == event.regionRef,
                    //&& currentNode->getRecordType() == event.type,
                    " [%u] RegionRef doesn't fit for event %"PRIu64":%s:%d:%" PRIu64 ":%lf"
                    " and internal node %s:%lf, %u != %" PRIu64, mpiRank, 
                    event.location, eventName, event.type, event.time, getRealTime(event.time),
                    currentNode->getUniqueName(),
                    (double)currentNode->getTime() / (double)timerResolution,
                    event.regionRef, currentNode->getFunctionId() );

      // model fork/join nodes as the currently running activity
      if ( currentNode->isOMPForkJoinRegion() )
      {
        UTILS_ASSERT( event.regionRef == ompForkJoinRef,
                      "ForkJoin must have regionRef %u", ompForkJoinRef );

        UTILS_ASSERT( event.type == RECORD_ATOMIC,
                      "Event %s has unexpected type", eventName );
        
        if( streamStatusMap[event.location].activityStack.size() > 0 )
        {
          const OTF2_RegionRef newRegionRef = 
            streamStatusMap[event.location].activityStack.top();
          
          currentNode->setFunctionId( newRegionRef );
        
          event.regionRef = newRegionRef;
        }
      }

      // preprocess current internal node (mark open edges to blame CPU events)
      if ( graph->hasOutEdges( currentNode ) )
      {
        const Graph::EdgeList& edges = graph->getOutEdges( currentNode );
        
        for ( Graph::EdgeList::const_iterator edgeIter = edges.begin();
              edgeIter != edges.end(); edgeIter++ )
        {
          Edge* edge = *edgeIter;

          if ( edge->getCPUBlame() > 0 )
          {
            streamState.openEdges.push_back( edge );
          }
        }
      }

      // copy node counter values to temporary counter map
      const AnalysisMetric::MetricIdSet& metricIdSet = cTable->getAllMetricIds();
      for ( AnalysisMetric::MetricIdSet::const_iterator metricIter = metricIdSet.begin();
            metricIter != metricIdSet.end(); ++metricIter )
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
            streamState.onCriticalPath = 
              (bool) tmpCounters[ CRITICAL_PATH ];
            
            // if next node is NOT on the CP
            if( currentNodeIter + 1 != endNodeIter &&  
                (*( currentNodeIter + 1 ) )->getCounter( CRITICAL_PATH ) == 0 )
            {
              streamState.onCriticalPath = false;
            }
            
            // if node is leave
            if( currentNode->isLeave() )
            {
              // if node is leave AND enter has zero as CP counter
              if( currentNode->getGraphPair().first->getCounter(CRITICAL_PATH ) == 0 )
              {
                // zero the counter, as the activity is not on the CP
                // affects events that are no internal nodes
                tmpCounters[ CRITICAL_PATH ] = 0;
              }

              // if we are at an MPI_Finalize leave
              if( currentNode->isMPIFinalize() )
              {
                //UTILS_MSG( true, "%s", currentNode->getUniqueName().c_str() );
                // if current stream has globally last event, set on CP to true
                if( currentStream->hasLastGlobalEvent() )
                {
                  //UTILS_WARNING("%llu has last global event", currentStream->getId() );
                  streamState.onCriticalPath = true;
                }
                else
                {
                  //UTILS_WARNING( "[%"PRIu64"] Set CP=0", streamState.currentStream->getId() );
                  streamState.onCriticalPath = false;
                }
              }
            }

            /*UTILS_MSG( ( strcmp( currentNode->getName(), "clFinish") == 0 ), 
                       "[%llu] %s: onCP %llu=?%d, Blame=%llu (EvtType: %d)", 
                       currentNode->getStreamId(), currentNode->getUniqueName().c_str(), 
                       tmpCounters[CRITICAL_PATH], processOnCriticalPath[event.location], 
                       tmpCounters[BLAME], event.type );*/
          }
        }
      }
      
      // check in edges to blame the interval between this and the last event
      if ( currentNode->isEnter() && graph->hasInEdges( currentNode ) )
      {
        // duration between last and current event
        uint64_t timeDiff = event.time - streamState.lastEventTime;
        
        // iterate over in edges
        const Graph::EdgeList& edges = graph->getInEdges( currentNode );
        for ( Graph::EdgeList::const_iterator edgeIter = edges.begin();
              edgeIter != edges.end(); edgeIter++ )
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
            double blame = (double)( edge->getCPUBlame() ) * (double)timeDiff 
                   / (double)( edge->getDuration() );
            
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
    if ( currentNodeIter != endNodeIter )
    {
       GraphNode* currentNode = *currentNodeIter;
     
      //// Compute critical path counter ////
      // Event is on critical path if next internal node is, too
      // BUT: if next event is a leave AND the corresponding enter node is not on the CP
      // the nested regions are not on the CP
      if( currentNode->isLeave() &&
          currentNode->getGraphPair().first->getCounter( CRITICAL_PATH ) == 0 )
      {
        tmpCounters[CRITICAL_PATH] = 0;
      }
      else
      {
        tmpCounters[CRITICAL_PATH] = currentNode->getCounter( CRITICAL_PATH );
      }
      
      // compute blame counter
      uint64_t blame = computeCPUEventBlame( event );
      
      //\todo: validate the following, which affects only the OTF2 output
      if( blame )
      {
        tmpCounters[ BLAME ] = blame;
      }

      // non-paradigm events cannot be wait states
      tmpCounters[ WAITING_TIME ] = 0;
    }
  }
  /*
  if( tmpCounters.count( CRITICAL_PATH ) > 0 )
  { 
    if( tmpCounters[ CRITICAL_PATH ] > 0 )
    {
      streamState.onCriticalPath = true;
    }
    else
    {
      streamState.onCriticalPath = false;
    }
  }
  */
  //UTILS_MSG( tmpCounters.size() == 0, "%"PRIu64":%s (onCP: %d)", 
  //           event.location, eventName.c_str(), streamState.onCriticalPath );
  
  streamState.currentNodeIter = currentNodeIter;

  // write idle leave before the device task enter
  if( currentStream->isDeviceStream() && event.type == RECORD_ENTER )
  {
    handleDeviceTaskEnter( event.time, event.location, true );
  }
  
  // write event with counters
  if ( writeToFile )
  {
    writeCounterMetrics( event, tmpCounters );
    writeEventsWithAttributes( event, attributeList, tmpCounters );    
  }
  
  // write idle enter after the device task leave
  if( currentStream->isDeviceStream() && event.type == RECORD_LEAVE )
  {
    handleDeviceTaskLeave( event.time, event.location, true );
  }

  // update values in activityGroupMap
  updateActivityGroupMap( event, tmpCounters );

  // set last event time for all event types (CPU and paradigm nodes)
  streamState.lastEventTime = event.time;

  // update activity stack
  switch ( event.type )
  {
    case RECORD_ENTER:
    {
      streamState.activityStack.push( event.regionRef );
      
      break;
    }
    case RECORD_LEAVE:
    {
      streamState.activityStack.pop();

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
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_ClockProperties( 
                                                      void*    userData,
                                                      uint64_t timerResolution,
                                                      uint64_t globalOffset,
                                                      uint64_t traceLength )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  tw->timerOffset     = globalOffset;
  tw->timerResolution = timerResolution;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteClockProperties( tw->otf2GlobalDefWriter,
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteLocationGroup( tw->otf2GlobalDefWriter,
                                                       self, name,
                                                       locationGroupType,
                                                       systemTreeParent ) );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Location( 
                                          void*                 userData,
                                          OTF2_LocationRef      self,
                                          OTF2_StringRef        name,
                                          OTF2_LocationType     locationType,
                                          uint64_t              numberOfEvents,
                                          OTF2_LocationGroupRef locationGroup )
{

  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  // \todo: this seems to work referring to OTF2_LocationType description
  if( tw->mpiRank == locationGroup )
  {
    // store all locations with their parent
    tw->locationParentMap[ self ] = locationGroup;
  }
  
  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteLocation( tw->otf2GlobalDefWriter, self,
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteGroup( tw->otf2GlobalDefWriter, self,
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteComm( tw->otf2GlobalDefWriter, self, name,
                                              group, parent ) );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_String( void* userData,
                                                              OTF2_StringRef self,
                                                              const char* string )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  // keep track of string definitions (to add new later on)
  tw->stringRefMap[ self ] = string;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {

    OTF2_CHECK( OTF2_GlobalDefWriter_WriteString( tw->otf2GlobalDefWriter, self,
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNode( tw->otf2GlobalDefWriter,
                                                        self, name, className,
                                                        parent ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty( tw->otf2GlobalDefWriter,
                                                                systemTreeNode,
                                                                name, type,
                                                                value ) );

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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteSystemTreeNodeDomain( tw->otf2GlobalDefWriter,
                                                              systemTreeNode,
                                                              systemTreeDomain ) );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Region( 
                                                void*           userData,
                                                OTF2_RegionRef  self,
                                                OTF2_StringRef  name,
                                                OTF2_StringRef  cannonicalName,
                                                OTF2_StringRef  description,
                                                OTF2_RegionRole regionRole,
                                                OTF2_Paradigm   paradigm,
                                                OTF2_RegionFlag regionFlags,
                                                OTF2_StringRef  sourceFile,
                                                uint32_t        beginLineNumber,
                                                uint32_t        endLineNumber )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  RegionInfo regionInfo;
  if( tw->stringRefMap.count( name ) > 0 )
  {
    regionInfo.name = tw->stringRefMap[ name ];
  }
  else
  {
    UTILS_WARNING( "OTF2TraceWriter: Could no find string reference %u", name );
    regionInfo.name = NULL;
  }
  regionInfo.paradigm = paradigm;
  regionInfo.role     = regionRole;
  tw->regionInfoMap[ self ] = regionInfo;

  if ( tw->mpiRank == 0 && tw->writeToFile )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteRegion( tw->otf2GlobalDefWriter, self,
                                                  name, cannonicalName, 
                                                  description, regionRole, 
                                                  paradigm, regionFlags, 
                                                  sourceFile, beginLineNumber,
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

  if ( tw->mpiRank == 0 )
  {
    // do not write the attribute definitions that have only been written for CASITA
    if ( strcmp(tw->stringRefMap[ name ], SCOREP_CUDA_STREAMREF ) != 0 &&
         strcmp(tw->stringRefMap[ name ], SCOREP_CUDA_EVENTREF ) != 0 &&
         strcmp(tw->stringRefMap[ name ], SCOREP_CUDA_CURESULT ) != 0 &&
         strcmp(tw->stringRefMap[ name ], SCOREP_OPENCL_QUEUEREF ) != 0 /* &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_LOCATIONREF ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_REGION_ID ) != 0 &&
         strcmp(tw->idStringMap[ name ], SCOREP_OMP_TARGET_PARENT_REGION_ID ) != 0*/ )
    {
      OTF2_CHECK( OTF2_GlobalDefWriter_WriteAttribute( tw->otf2GlobalDefWriter, self,
                                                     name, description, type ) );
    }
  }
  
  // increment attribute counter to append CASITA attribute definitions
  tw->cTable->addAttributeId( self );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2GlobalDefReaderCallback_MetricMember( 
                                                 void*                userData,
                                                 OTF2_MetricMemberRef self,
                                                 OTF2_StringRef       name,
                                                 OTF2_StringRef       description,
                                                 OTF2_MetricType      metricType,
                                                 OTF2_MetricMode      metricMode,
                                                 OTF2_Type            valueType,
                                                 OTF2_Base            base,
                                                 int64_t              exponent,
                                                 OTF2_StringRef       unit )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  if( tw->mpiRank == 0 )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteMetricMember( tw->otf2GlobalDefWriter, 
                                                        self, name, description,
                                                        metricType, metricMode,
                                                        valueType, base, exponent,
                                                        unit ) );
  }
  
  //UTILS_WARNING( "[%u] Found metric member %s with definition ID %u", 
  //               tw->mpiRank, tw->idStringMap[name], self );
  
  // increment metric member ID to append CASITA metric member definitions
  tw->cTable->addMetricMemberId( self );
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2GlobalDefReaderCallback_MetricClass(
                                  void*                       userData,
                                  OTF2_MetricRef              self,
                                  uint8_t                     numberOfMetrics,
                                  const OTF2_MetricMemberRef* metricMembers,
                                  OTF2_MetricOccurrence       metricOccurrence,
                                  OTF2_RecorderKind           recorderKind )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  if( tw->mpiRank == 0 )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteMetricClass(  tw->otf2GlobalDefWriter, 
                                                        self, numberOfMetrics,
                                                        metricMembers,
                                                        metricOccurrence,
                                                        recorderKind ) );
  }
  
  // increment metric class ID to append CASITA counter definitions
  tw->cTable->addMetricClassId( self );
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2GlobalDefReaderCallback_MetricInstance(
                                                   void*            userData,
                                                   OTF2_MetricRef   self,
                                                   OTF2_MetricRef   metricClass,
                                                   OTF2_LocationRef recorder,
                                                   OTF2_MetricScope metricScope,
                                                   uint64_t         scope )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  if( tw->mpiRank == 0 )
  {
    OTF2_CHECK( OTF2_GlobalDefWriter_WriteMetricInstance( tw->otf2GlobalDefWriter, 
                  self, metricClass, recorder, metricScope, scope ) );
  }
  
  // increment metric class ID to append CASITA counter definitions
  // metric class and metric instances share the same ID space in OTF2
  tw->cTable->addMetricClassId( self );
  
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

  OTF2_CHECK( OTF2_GlobalDefWriter_WriteRmaWin( tw->otf2GlobalDefWriter, self,
                                                name, comm ) );

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
                                                         void*            userData,
                                                         OTF2_AttributeList*
                                                         attributeList,
                                                         OTF2_RmaWinRef   win )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  if ( tw->writeToFile )
  {
    // handle last device idle leave
    if( tw->lastOffloadApiEvtTime != 0 )
    {
      StreamStatus& streamState = tw->streamStatusMap[ location ];
      EventStream* currentStream = streamState.stream;
      if( currentStream->isDeviceStream() )
      {
        tw->handleFinalDeviceIdleLeave();
      }
    }
    
    OTF2_CHECK( OTF2_EvtWriter_RmaWinDestroy( tw->evt_writerMap[location],
                                              attributeList, time,
                                              win ) );
  }
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaPut( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  void*            userData,
                                                  OTF2_AttributeList*
                                                  attributeList,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  EventStream* stream = tw->analysis->getStream( location );
  
  // communication task on device streams starts
  if( stream->isDeviceStream() )
  {
    // device communication task device-to-host
    tw->handleDeviceTaskEnter( time, location, false, false );
  }

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaPut( tw->evt_writerMap[location], attributeList,
                                       time, win, remote, bytes, matchingId ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaOpCompleteBlocking( 
                                           OTF2_LocationRef    location,
                                           OTF2_TimeStamp      time,
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
  
  EventStream* stream = tw->analysis->getStream( location );
  
  // communication task on device streams ends
  if( stream->isDeviceStream() )
  {
    tw->handleDeviceTaskLeave( time, location, false );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_RmaGet( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  void*            userData,
                                                  OTF2_AttributeList*
                                                  attributeList,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  EventStream* stream = tw->analysis->getStream( location );
  
  // communication task on device streams starts, leave device idle region
  if( stream->isDeviceStream() )
  {
    // device communication task host-to-device
    tw->handleDeviceTaskEnter( time, location, false, true );
  }
  
  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_RmaGet( tw->evt_writerMap[location], attributeList,
                                       time, win, remote, bytes, matchingId ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackComm_ThreadTeamBegin( OTF2_LocationRef locationID,
                                                           OTF2_TimeStamp   time,
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
 * @param location OTF2 location reference (internally used as stream ID)
 * @param time
 * @param userData
 * @param attributes
 * @param region
 * @return 
 */
OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackEnter( OTF2_LocationRef    location,
                                            OTF2_TimeStamp      time,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  if( tw->streamStatusMap[ location ].isFilterOn )
  {
    return OTF2_CALLBACK_SUCCESS;
  }
  
  if( tw->analysis->isFunctionFiltered( region ) )
  {
    tw->streamStatusMap[location].isFilterOn = true;
    return OTF2_CALLBACK_SUCCESS;
  }

  // define event to write next node in list
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = RECORD_ENTER;

  //if( tw->currentStreamMap[location]->getLastEventTime() >= time - tw->timerOffset )
  {
    tw->processNextEvent( event, attributes );
  }

  /*if ( tw->mpiSize > 1 && Parser::getInstance().getProgramOptions().analysisInterval &&
       time - tw->timerOffset == tw->currentStreamMap[location]->getLastEventTime() )
  {
    return OTF2_CALLBACK_INTERRUPT;
  }*/

  return OTF2_CALLBACK_SUCCESS;

}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackLeave( OTF2_LocationRef    location, // streamID
                                            OTF2_TimeStamp      time,
                                            void*               userData,
                                            OTF2_AttributeList* attributes,
                                            OTF2_RegionRef      region )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;
  
  if( tw->analysis->isFunctionFiltered( region ) )
  {
    UTILS_WARNING("asdf3");
    tw->streamStatusMap[location].isFilterOn = false;
    return OTF2_CALLBACK_SUCCESS;
  }
  
  if( tw->streamStatusMap[location].isFilterOn )
  {
    UTILS_WARNING("asdf2");
    return OTF2_CALLBACK_SUCCESS;
  }

  // Define event to write next node in list
  OTF2Event event;
  event.location  = location;
  event.regionRef = region;
  event.time      = time;
  event.type      = RECORD_LEAVE;
  
  //if( tw->currentStreamMap[location]->getLastEventTime() >= time - tw->timerOffset )
  {
    tw->processNextEvent( event, attributes );
  }
  
  // interrupt reading, if we processed the last read leave event
  /*if ( tw->mpiSize > 1 && Parser::getInstance().getProgramOptions().analysisInterval &&
       time - tw->timerOffset == tw->currentStreamMap[location]->getLastEventTime() )
  {
    return OTF2_CALLBACK_INTERRUPT;
  }*/

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2EvtCallbackThreadFork( 
                                  OTF2_LocationRef    locationID,
                                  OTF2_TimeStamp      time,
                                  void*               userData,
                                  OTF2_AttributeList* attributeList,
                                  OTF2_Paradigm       paradigm,
                                  uint32_t            numberOfRequestedThreads )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  // write next node in List
  OTF2Event event;
  event.location = locationID;
  
  /* Fork/Join-RegionRef is created when definitions are read.
   * This event is processed because internal it is a node and counters have to
   * be calculated correctly (always happens between internal nodes).
   */
  event.regionRef = tw->ompForkJoinRef;
  event.time      = time;
  
  // mark as atomic to avoid unnecessary operations in processNextEvent())
  event.type      = RECORD_ATOMIC; 

  tw->processNextEvent( event, attributeList );

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadFork( tw->evt_writerMap[locationID],
                                           attributeList, time, paradigm,
                                           numberOfRequestedThreads ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2EvtCallbackThreadJoin( OTF2_LocationRef    locationID,
                                                    OTF2_TimeStamp      time,
                                                    void*               userData,
                                                    OTF2_AttributeList* attributeList,
                                                    OTF2_Paradigm       paradigm )
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

  tw->processNextEvent( event, attributeList );

  if ( tw->writeToFile )
  {
    OTF2_CHECK( OTF2_EvtWriter_ThreadJoin( tw->evt_writerMap[locationID],
                                           attributeList, time, paradigm ) );
  }

  return OTF2_CALLBACK_SUCCESS;
}

/**
 * This callback reads and writes a metric instance.
 */
OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2CallbackMetric( 
                                     OTF2_LocationRef        location,
                                     OTF2_TimeStamp          time,
                                     void*                   userData,
                                     OTF2_AttributeList*     attributeList,
                                     OTF2_MetricRef          metric,
                                     uint8_t                 numberOfMetrics,
                                     const OTF2_Type*        typeIDs,
                                     const OTF2_MetricValue* metricValues )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  OTF2_CHECK( OTF2_EvtWriter_Metric( tw->evt_writerMap[location],
                attributeList, time, metric, numberOfMetrics, typeIDs, 
                metricValues ) );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiRecv( OTF2_LocationRef locationID,
                                               OTF2_TimeStamp   time,
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
                                               void*            userData,
                                               OTF2_AttributeList* attributeList,
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

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIrecvRequest( 
                                              OTF2_LocationRef    locationID,
                                              OTF2_TimeStamp      time,
                                              void*               userData,
                                              OTF2_AttributeList* attributeList,
                                              uint64_t            requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  OTF2_CHECK( OTF2_EvtWriter_MpiIrecvRequest( tw->evt_writerMap[locationID],
                                              attributeList, time, requestID ) );
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIrecv( OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                void*            userData,
                                                OTF2_AttributeList* attributeList,
                                                uint32_t         sender,
                                                OTF2_CommRef     communicator,
                                                uint32_t         msgTag,
                                                uint64_t         msgLength,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  OTF2_CHECK( OTF2_EvtWriter_MpiIrecv( tw->evt_writerMap[locationID],
                                      attributeList, time, sender,
                                      communicator, msgTag, msgLength,
                                      requestID ) );
    
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIsend( OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                void*            userData,
                                                OTF2_AttributeList* attributeList,
                                                uint32_t         receiver,
                                                OTF2_CommRef     communicator,
                                                uint32_t         msgTag,
                                                uint64_t         msgLength,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  OTF2_CHECK( OTF2_EvtWriter_MpiIsend( tw->evt_writerMap[locationID],
                                      attributeList, time, receiver,
                                      communicator, msgTag, msgLength,
                                      requestID ) );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2ParallelTraceWriter::otf2Callback_MpiIsendComplete( 
                                                OTF2_LocationRef locationID,
                                                OTF2_TimeStamp   time,
                                                void*            userData,
                                                OTF2_AttributeList* attributeList,
                                                uint64_t         requestID )
{
  OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter*)userData;

  OTF2_CHECK( OTF2_EvtWriter_MpiIsendComplete( tw->evt_writerMap[locationID],
                                               attributeList, time, requestID ) );

  return OTF2_CALLBACK_SUCCESS;
}
