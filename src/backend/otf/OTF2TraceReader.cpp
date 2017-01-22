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
 * Provide interactions with OTF2-file for reading definitions and events.
 * - read otf2 file and trigger callbacks to start appropriate actions for read definitions and events like creating the graph, event streams
 *
 */

#include <stdexcept>
#include <stack>
#include <string>
#include <cstring>
#include <iostream>
#include <list>
#include <algorithm>
#include <limits>

#include "common.hpp"
#include "otf/OTF2TraceReader.hpp"
#include "utils/ErrorUtils.hpp"

#define OTF2_CHECK( cmd ) \
  { \
    int _status = cmd; \
    if ( _status ) { throw RTException( "OTF2 command '%s' returned error", #cmd );} \
  }

using namespace casita::io;

OTF2TraceReader::OTF2TraceReader( void*    userData,
                                  uint32_t mpiRank,
                                  uint32_t mpiSize ) :
  handleEnter( NULL ),
  handleLeave( NULL ),
  handleDefProcess( NULL ),
  handleDefFunction( NULL ),
  handleDefAttribute( NULL ),
  handleProcessMPIMapping( NULL ),
  handleMPIComm( NULL ),
  handleMPICommGroup( NULL ),
  handleMPIIrecv( NULL ),
  handleMPIIrecvRequest( NULL ),
  handleMPIIsendComplete( NULL ),
  handleRmaWinDestroy( NULL ),
  userData( userData ),
  mpiRank( mpiRank ),
  mpiSize( mpiSize ),
  mpiProcessId( 1 ),
  reader( NULL ),
  ticksPerSecond( 1 ),
  timerOffset( 0 ),
  traceLength( 0 ),
  ompForkJoinRef( 0 ),
  processingPhase( 0 )
{

}

OTF2TraceReader::~OTF2TraceReader( )
{
  /* delete process groups */
  for ( ProcessGroupMap::iterator iter = processGroupMap.begin( );
        iter != processGroupMap.end( ); ++iter )
  {
    ProcessGroup* pg = iter->second;
    if ( pg->name != NULL )
    {
      delete[]pg->name;
    }
    if ( pg->procs != NULL )
    {
      delete[]pg->procs;
    }
    delete pg;
  }

  /* delete members of mpi-groups */
  for ( GroupIdGroupMap::iterator iter = groupMap.begin( );
        iter != groupMap.end( ); iter++ )
  {
    delete[]iter->second.members;
  }
}

OTF2TraceReader::IdNameTokenMap&
OTF2TraceReader::getProcessNameTokenMap( )
{
  return processNameTokenMap;
}

OTF2TraceReader::TokenTokenMap&
OTF2TraceReader::getFunctionNameTokenMap( )
{
  return functionNameTokenMap;
}

OTF2TraceReader::NameTokenMap&
OTF2TraceReader::getNameKeysMap( )
{
  return nameKeysMap;
}

OTF2TraceReader::TokenNameMap&
OTF2TraceReader::getKeyNameMap( )
{
  return kNameMap;
}

OTF2TraceReader::TokenTokenMap64&
OTF2TraceReader::getProcessFamilyMap( )
{
  return processFamilyMap;
}

/*OTF2TraceReader::IdTokenMap&
OTF2TraceReader::getProcessRankMap( )
{
  return processRankMap;
}*/

OTF2TraceReader::GroupIdGroupMap&
OTF2TraceReader::getGroupMap( )
{
  return groupMap;
}

int
OTF2TraceReader::getProcessingPhase( )
{
  return processingPhase;
}

OTF2KeyValueList&
OTF2TraceReader::getKVList( )
{
  return kvList;
}

uint64_t
OTF2TraceReader::getMPIProcessId( )
{
  return mpiProcessId;
}

void
OTF2TraceReader::setMPIStreamId( uint64_t processId )
{
  mpiProcessId = processId;
}

void
OTF2TraceReader::open( const std::string otfFilename, uint32_t maxFiles )
{
  baseFilename.assign( "" );
  baseFilename.append( otfFilename.c_str( ), otfFilename.length( ) );
  reader = OTF2_Reader_Open( baseFilename.c_str( ) );

  // TODO: why?
  OTF2_Reader_SetSerialCollectiveCallbacks( reader );

  if ( !reader )
  {
    throw RTException( "Failed to open OTF2 trace file %s", baseFilename.c_str( ) );
  }
}

void
OTF2TraceReader::close( )
{
  OTF2_CHECK( OTF2_Reader_Close( reader ) );
}

/**
 * Setup the reader. Read local definitions and register for event callbacks.
 * 
 * @param ignoreAsyncMPI if true, do not register for non-blocking MPI
 */
void
OTF2TraceReader::setupEventReader( bool ignoreAsyncMPI )
{
  // processNameTokenMap is initialized during traceReader->readDefinitions( );
  for ( IdNameTokenMap::const_iterator iter = processNameTokenMap.begin( );
        iter != processNameTokenMap.end( ); ++iter )
  {
    OTF2_Reader_SelectLocation( reader, iter->first );
  }
  
  OTF2_Reader_OpenEvtFiles( reader );
  OTF2_Reader_OpenDefFiles( reader );

  for ( IdNameTokenMap::const_iterator iter = processNameTokenMap.begin( );
        iter != processNameTokenMap.end( ); ++iter )
  {
    OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, iter->first );
    uint64_t def_reads         = 0;
    OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
    OTF2_Reader_CloseDefReader( reader, def_reader );
    OTF2_Reader_GetEvtReader( reader, iter->first );
  }

  OTF2_Reader_CloseDefFiles( reader );

  OTF2_GlobalEvtReader* global_evt_reader        =
    OTF2_Reader_GetGlobalEvtReader( reader );

  OTF2_GlobalEvtReaderCallbacks* event_callbacks =
    OTF2_GlobalEvtReaderCallbacks_New( );
  OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks,
                                                  &otf2CallbackEnter );
  OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks,
                                                  &otf2CallbackLeave );
  OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback(
    event_callbacks,&otf2Callback_MpiCollectiveEnd );
  OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback( event_callbacks,
                                                    &otf2Callback_MpiRecv );
  OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback( event_callbacks,
                                                    &otf2Callback_MpiSend );
  OTF2_GlobalEvtReaderCallbacks_SetThreadForkCallback(
    event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadFork );
  OTF2_GlobalEvtReaderCallbacks_SetThreadJoinCallback(
    event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadJoin );
  
  // 
  OTF2_GlobalEvtReaderCallbacks_SetRmaWinDestroyCallback(
    event_callbacks, &otf2CallbackComm_RmaWinDestroy );
  
  if ( !ignoreAsyncMPI )
  {
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvRequestCallback( event_callbacks, 
                                                &otf2Callback_MpiIRecvRequest );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvCallback( event_callbacks, 
                                                       &otf2Callback_MpiIRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCallback( event_callbacks, 
                                                       &otf2Callback_MpiISend );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCompleteCallback( event_callbacks, 
                                               &otf2Callback_MpiISendComplete );
  }
  
  OTF2_Reader_RegisterGlobalEvtCallbacks( reader,
                                          global_evt_reader,
                                          event_callbacks,
                                          this );
  OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );
}

/**
 * Read OTF2 events.
 * 
 * @return true, if more events are available to read.
 */
bool
OTF2TraceReader::readEvents( uint64_t *events_read )
{
  OTF2_GlobalEvtReader* global_evt_reader        =
    OTF2_Reader_GetGlobalEvtReader( reader );
  
  //uint64_t events_read = 0;

  // returns 0 if successful, >0 otherwise
  OTF2_ErrorCode otf2_error = 
    OTF2_Reader_ReadAllGlobalEvents( reader, global_evt_reader, events_read );
  if ( OTF2_SUCCESS != otf2_error )
  {
    if( OTF2_ERROR_INTERRUPTED_BY_CALLBACK == otf2_error )
    {
      UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_SOME, 
                 "[0] Reader interrupted by callback. Read %" PRIu64 " events", 
                 *events_read );
      
      return true;
    }
    else
      throw RTException( "Failed to read OTF2 events" );
  }

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] Read %" PRIu64 " events", *events_read );

  OTF2_Reader_CloseGlobalEvtReader( reader, global_evt_reader );

  OTF2_Reader_CloseEvtFiles( reader );
  
  return false;
}

/**
 * Unused
 * 
 * @param id
 * @param ignoreAsyncMPI
 */
void
OTF2TraceReader::readEventsForProcess( uint64_t id, bool ignoreAsyncMPI )
{
  OTF2_Reader_SelectLocation( reader, id );

  OTF2_Reader_OpenEvtFiles( reader );

  OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, id );
  uint64_t def_reads         = 0;
  OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
  OTF2_Reader_CloseDefReader( reader, def_reader );

  OTF2_Reader_GetEvtReader( reader, id );

  OTF2_GlobalEvtReader* global_evt_reader        =
    OTF2_Reader_GetGlobalEvtReader(
      reader );  

  OTF2_GlobalEvtReaderCallbacks* event_callbacks =
    OTF2_GlobalEvtReaderCallbacks_New( );
  OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks,
                                                  &otf2CallbackEnter );
  OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks,
                                                  &otf2CallbackLeave );
  OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback(
    event_callbacks,
    &
    otf2Callback_MpiCollectiveEnd );
  OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback( event_callbacks,
                                                    &otf2Callback_MpiRecv );
  OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback( event_callbacks,
                                                    &otf2Callback_MpiSend );
  OTF2_GlobalEvtReaderCallbacks_SetThreadForkCallback(
    event_callbacks,
    &
    OTF2_GlobalEvtReaderCallback_ThreadFork );
  OTF2_GlobalEvtReaderCallbacks_SetThreadJoinCallback(
    event_callbacks,
    &
    OTF2_GlobalEvtReaderCallback_ThreadJoin );
  
  if ( !ignoreAsyncMPI )
  {
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvRequestCallback( event_callbacks, 
                                                &otf2Callback_MpiIRecvRequest );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIrecvCallback( event_callbacks, 
                                                       &otf2Callback_MpiIRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCallback( event_callbacks, 
                                                       &otf2Callback_MpiISend );
    OTF2_GlobalEvtReaderCallbacks_SetMpiIsendCompleteCallback( event_callbacks, 
                                               &otf2Callback_MpiISendComplete );
  }
  
  OTF2_Reader_RegisterGlobalEvtCallbacks( reader,
                                          global_evt_reader,
                                          event_callbacks,
                                          this );
  OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );

  uint64_t events_read = 0;

  /* returns 0 if successful, >0 otherwise */
  if ( OTF2_Reader_ReadAllGlobalEvents( reader, global_evt_reader, &events_read ) )
  {
    throw RTException( "Failed to read OTF2 events" );
  }

  OTF2_Reader_CloseGlobalEvtReader( reader, global_evt_reader );

  OTF2_Reader_CloseEvtFiles( reader );

}

/**
 * Read all global definitions.
 * 
 * @return true, if successful
 */
bool
OTF2TraceReader::readDefinitions( )
{
  OTF2_GlobalDefReader* global_def_reader = 
    OTF2_Reader_GetGlobalDefReader( reader );

  OTF2_GlobalDefReaderCallbacks* global_def_callbacks =
    OTF2_GlobalDefReaderCallbacks_New( );

  processingPhase = 1;

  /* Phase 1 -> read string definitions and Groups */
  OTF2_GlobalDefReaderCallbacks_SetAttributeCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Attribute );
  
  OTF2_GlobalDefReaderCallbacks_SetStringCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_String );
  
  OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_ClockProperties );
  
  OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Location );
/*
  OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(
    global_def_callbacks,
    OTF2_GlobalDefReaderCallback_LocationGroup );
*/
  OTF2_GlobalDefReaderCallbacks_SetGroupCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Group );
  
  OTF2_GlobalDefReaderCallbacks_SetCommCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Comm );
  
  OTF2_GlobalDefReaderCallbacks_SetRegionCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Region );

  // register callbacks
  OTF2_Reader_RegisterGlobalDefCallbacks( reader,
                                          global_def_reader,
                                          global_def_callbacks,
                                          this );

  OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );

  // read definitions
  uint64_t definitions_read = 0;
  OTF2_Reader_ReadAllGlobalDefinitions( reader,
                                        global_def_reader,
                                        &definitions_read );

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] Read %" PRIu64 " definitions in Phase 1", definitions_read );

  close( );
  
  /*if ( ( processRankMap.size() == 0 && mpiSize > 1 ) || 
       ( processRankMap.size() && (mpiSize != processRankMap.size() ) ) )
  {
    UTILS_MSG( true, "[%u] CASITA has to be run with %zu MPI process(es)!",
               mpiRank, processRankMap.size() == 0 ? 1 : processRankMap.size() );*/
  if ( ( rankStreamMap.size() == 0 && mpiSize > 1 ) || 
       ( rankStreamMap.size() && (mpiSize != rankStreamMap.size() ) ) )
  {
    UTILS_MSG( true, "[%u] CASITA has to be run with %zu MPI process(es)!",
               mpiRank, rankStreamMap.size() == 0 ? 1 : rankStreamMap.size() );
    
    return false;
  }

  open( baseFilename.c_str( ), 10 );

  // read definitions (part 2)
  processingPhase      = 2;
  global_def_reader    = OTF2_Reader_GetGlobalDefReader( reader );

  global_def_callbacks = OTF2_GlobalDefReaderCallbacks_New( );

  /* Phase 2 -> read string definitions and Groups */
  OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
    global_def_callbacks,
    &
    OTF2_GlobalDefReaderCallback_Location );

  /* register callbacks */
  OTF2_Reader_RegisterGlobalDefCallbacks( reader,
                                          global_def_reader,
                                          global_def_callbacks,
                                          this );

  OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );

  definitions_read = 0;

  /* read definitions */
  OTF2_Reader_ReadAllGlobalDefinitions( reader,
                                        global_def_reader,
                                        &definitions_read );

  UTILS_MSG( mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[0] Read %" PRIu64 " definitions in Phase 2", definitions_read );

  // add forkjoin "region" to support internal OMP-fork/join model
  uint32_t stringSize = definitionTokenStringMap.size( );
  definitionTokenStringMap[stringSize] = OTF2_OMP_FORKJOIN_INTERNAL;
  ompForkJoinRef = functionNameTokenMap.size( );
  functionNameTokenMap[ompForkJoinRef] = stringSize;
  
  /* check OTF2 location reference, MPI rank map
  IdTokenMap& processRankMap = this->getProcessRankMap( );
  IdTokenMap::iterator iter = processRankMap.begin();

  UTILS_MSG(true, "[%u] OTF2 location reference, MPI rank map", mpiRank )
  for( ; iter != processRankMap.end( ); ++iter )
  {
    UTILS_MSG(true, "[%u] %llu, %u", mpiRank, iter->first, iter->second )
  }*/
  
  return true;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_ClockProperties( 
                                                       void*    userData,
                                                       uint64_t timerResolution,
                                                       uint64_t globalOffset,
                                                       uint64_t traceLength )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  tr->setTimerOffset( globalOffset );
  tr->setTimerResolution( timerResolution );
  tr->setTraceLength( traceLength );

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_Location( 
                                          void*                 userData,
                                          OTF2_LocationRef      self,
                                          OTF2_StringRef        name,
                                          OTF2_LocationType     locationType,
                                          uint64_t              numberOfEvents,
                                          OTF2_LocationGroupRef locationGroup )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  int phase           = tr->getProcessingPhase();

  if ( phase == 1 )
  {
    tr->getProcessFamilyMap()[self] = locationGroup;
  }
  
  // ignore metric or unknown locations
  if( locationType == OTF2_LOCATION_TYPE_METRIC || 
      locationType == OTF2_LOCATION_TYPE_UNKNOWN )
  {
    if ( tr->isChildOf( self, tr->getMPIProcessId() ) )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "Ignore metric/unknown location %s", 
                 tr->getStringRef( name ).c_str() );
    }
    
    return OTF2_CALLBACK_SUCCESS;
  }

  if ( phase == 2 )
  {
    if ( tr->handleProcessMPIMapping )
    {
      tr->handleProcessMPIMapping( tr, self, locationGroup );
    }

    // skip all processes but the mapping MPI master process and its children
    if ( tr->getMPISize() > 1 )
    {
      if ( self != tr->getMPIProcessId() &&
           ( !tr->isChildOf( self, tr->getMPIProcessId() ) ) )
      {
        return OTF2_CALLBACK_SUCCESS;
      }
    }

    // Locations are processes
    tr->getProcessNameTokenMap()[self] = name;

    if ( tr->handleDefProcess )
    {
      tr->handleDefProcess( tr, self, locationGroup, 
                            tr->getStringRef( name ).c_str(), NULL, 
                            locationType == OTF2_LOCATION_TYPE_GPU ? true : false );
    }

  }

  return OTF2_CALLBACK_SUCCESS;
}
/*
OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_LocationGroup( 
                                void*                  userData,
                                OTF2_LocationGroupRef  self,
                                OTF2_StringRef         name,
                                OTF2_LocationGroupType locationGroupType,
                                OTF2_SystemTreeNodeRef systemTreeParent)
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  int phase           = tr->getProcessingPhase( );

  if ( phase == 1 && locationGroupType == OTF2_LOCATION_GROUP_TYPE_PROCESS )
  {
    //tr->
  }

  return OTF2_CALLBACK_SUCCESS;
}
*/

/**
 * Callback for OTF2 group definitions (such as OPENCL, PTHREAD, etc. ).
 * 
 * @param userData
 * @param self unique identifier of this group reference
 * @param name
 * @param groupType
 * @param paradigm
 * @param groupFlags
 * @param numberOfMembers
 * @param members identifiers of the group members
 * 
 * @return 
 */
OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_Group( void*           userData,
                                                     OTF2_GroupRef   self,
                                                     OTF2_StringRef  name,
                                                     OTF2_GroupType  groupType,
                                                     OTF2_Paradigm   paradigm,
                                                     OTF2_GroupFlag  groupFlags,
                                                     uint32_t        numberOfMembers,
                                                     const uint64_t* members )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  UTILS_MSG( tr->mpiRank == 0 && Parser::getVerboseLevel() > VERBOSE_BASIC && 
             name != OTF2_UNDEFINED_STRING, 
             "[0] Read OTF2 group definition: %s",  
             tr->getDefinitionTokenStringMap( )[name].c_str() );

  uint64_t* myMembers = new uint64_t[numberOfMembers];
  for ( uint32_t i = 0; i < numberOfMembers; i++ )
  {
    myMembers[i] = members[i];
  }
  OTF2Group myGroup;
  myGroup.groupId          = self;
  myGroup.members          = myMembers;
  myGroup.numberOfMembers  = numberOfMembers;
  myGroup.paradigm         = paradigm;
  myGroup.stringRef        = name;
  myGroup.groupType        = groupType;
  myGroup.groupFlag        = groupFlags;

  tr->getGroupMap( )[self] = myGroup;

  if ( ( groupType == OTF2_GROUP_TYPE_COMM_LOCATIONS ) &&
       ( paradigm == OTF2_PARADIGM_MPI ) )
  {
    uint32_t mpiRank = tr->getMPIRank( );

    if ( numberOfMembers <= tr->getMPIRank( ) )
    {
      throw RTException(
          "Process group MPI_COMM_WORLD has no process for this MPI rank (%u)",
          mpiRank );
    }

    //IdTokenMap& processRankMap = tr->getProcessRankMap( );
    
    // set the trace reader's stream ID
    tr->setMPIStreamId( members[mpiRank] );
    for ( uint32_t i = 0; i < numberOfMembers; ++i )
    {
      //processRankMap[ members[i] ] = i;
      tr->rankStreamMap[i] = members[i];
    }
    if ( tr->handleMPICommGroup )
    {
      tr->handleMPICommGroup( tr, 0, myGroup.numberOfMembers, myGroup.members );
    }

  }

  return OTF2_CALLBACK_SUCCESS;
}

/**
 * Callback for communicator definition.
 * 
 * @param userData
 * @param self 
 * @param name name of the communicator
 * @param group
 * @param parent
 * @return 
 */
OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_Comm( void*          userData,
                                                    OTF2_CommRef   self,
                                                    OTF2_StringRef name,
                                                    OTF2_GroupRef  group,
                                                    OTF2_CommRef   parent )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  UTILS_MSG( tr->mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC && 
             name != OTF2_UNDEFINED_STRING, 
             "[0] Read OTF2 communicator definition: %s",  
             tr->getDefinitionTokenStringMap( )[name].c_str() );

  GroupIdGroupMap::const_iterator iter = tr->getGroupMap( ).find( group );
  UTILS_ASSERT( iter != tr->getGroupMap( ).end( ), "Group not found" );
  OTF2Group myGroup   = iter->second;

  if ( myGroup.paradigm == OTF2_PARADIGM_MPI )
  {
    if ( tr->handleMPICommGroup )
    {
      tr->handleMPICommGroup( tr,
                              self,
                              myGroup.numberOfMembers,
                              myGroup.members );
    }
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_String( void*          userData,
                                                      OTF2_StringRef self,
                                                      const char*    string )
{

  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  uint32_t max_length = 1000;
  std::string str( string, strnlen( string, max_length ) );
  tr->getDefinitionTokenStringMap( )[self] = str;

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_Region( void*          userData,
                                                      OTF2_RegionRef self,
                                                      OTF2_StringRef name,
                                                      OTF2_StringRef cannonicalName,
                                                      OTF2_StringRef description,
                                                      OTF2_RegionRole regionRole,
                                                      OTF2_Paradigm  paradigm,
                                                      OTF2_RegionFlag regionFlags,
                                                      OTF2_StringRef sourceFile,
                                                      uint32_t       beginLineNumber,
                                                      uint32_t       endLineNumber )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  // locations are processes
  tr->getFunctionNameTokenMap( )[self] = name;

  if ( tr->handleDefFunction )
  {
    tr->handleDefFunction( tr, 0, self, tr->getFunctionName( self ).c_str(), 
                           paradigm );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_Attribute( void*             userData,
                                                         OTF2_AttributeRef self,
                                                         OTF2_StringRef    name,
                                                         OTF2_StringRef    description,
                                                         OTF2_Type         type )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  std::string      s  = tr->getKeyName( name );
  tr->getNameKeysMap().insert( std::make_pair( s, self ) );
  tr->getKeyNameMap().insert( std::make_pair( self, s ) );

  if ( tr->handleDefAttribute )
  {
    tr->handleDefAttribute( tr, 0, self, s.c_str() );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackEnter( OTF2_LocationRef    location,
                                    OTF2_TimeStamp      time,
                                    void*               userData,
                                    OTF2_AttributeList* attributes,
                                    OTF2_RegionRef      region )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleEnter )
  {
    OTF2KeyValueList& kvList = tr->getKVList( );
    kvList.setList( attributes );

    tr->handleEnter(tr, time - tr->getTimerOffset(), region, location, &kvList);
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackLeave( OTF2_LocationRef    location,
                                    OTF2_TimeStamp      time,
                                    void*               userData,
                                    OTF2_AttributeList* attributes,
                                    OTF2_RegionRef      region )
{

  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleLeave )
  {
    OTF2KeyValueList& kvList = tr->getKVList( );
    kvList.setList( attributes );

    bool interrupt = tr->handleLeave( tr, time - tr->getTimerOffset( ), region, 
                                      location, &kvList );
    
    if ( interrupt )
      return OTF2_CALLBACK_INTERRUPT;
  }

  return OTF2_CALLBACK_SUCCESS;
}

/**
 * 
 * @param locationID    OTF2 location where this event happened
 * @param time
 * @param userData
 * @param attributeList
 * @param collectiveOp
 * @param communicator  Communicator references a definition and will be mapped 
 *                      to the global definition
 * @param root          MPI rank of root in communicator.
 * @param sizeSent
 * @param sizeReceived
 * @return 
 */
OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiCollectiveEnd( OTF2_LocationRef  locationID,
                                                OTF2_TimeStamp    time,
                                                void*             userData,
                                                OTF2_AttributeList*
                                                attributeList,
                                                OTF2_CollectiveOp collectiveOp,
                                                OTF2_CommRef      communicator,
                                                uint32_t          root,
                                                uint64_t          sizeSent,
                                                uint64_t          sizeReceived )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIComm )
  {
    io::MPIType mpiType = io::MPI_COLLECTIVE;
    switch ( collectiveOp )
    {
      case OTF2_COLLECTIVE_OP_BCAST:
      case OTF2_COLLECTIVE_OP_GATHER:
      case OTF2_COLLECTIVE_OP_GATHERV:
      case OTF2_COLLECTIVE_OP_REDUCE:
      case OTF2_COLLECTIVE_OP_SCATTER:
      case OTF2_COLLECTIVE_OP_SCATTERV:
        mpiType = io::MPI_ONEANDALL;
        break;
    }
    
    uint64_t rootStreamId = std::numeric_limits< uint64_t >::max( );
    
    if( mpiType == io::MPI_ONEANDALL )
    {
      RankStreamIdMap::iterator it = tr->rankStreamMap.find( root );
      
      if ( it != tr->rankStreamMap.end( ) )
      {
        rootStreamId = tr->rankStreamMap[root];
      }
      else
      {
        throw RTException( "Could not find stream ID for root rank (%u)", root );
        rootStreamId = root; // use root rank instead
      }
    }

    tr->handleMPIComm( tr, mpiType, locationID, communicator, /*root*/rootStreamId, 0 );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiRecv( OTF2_LocationRef    locationID,
                                       OTF2_TimeStamp      time,
                                       void*               userData,
                                       OTF2_AttributeList* attributeList,
                                       uint32_t            sender,
                                       OTF2_CommRef        communicator,
                                       uint32_t            msgTag,
                                       uint64_t            msgLength )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIComm )
  {
    tr->handleMPIComm( tr, MPI_RECV, locationID, sender, 0, msgTag );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiSend( OTF2_LocationRef    locationID,
                                       OTF2_TimeStamp      time,
                                       void*               userData,
                                       OTF2_AttributeList* attributeList,
                                       uint32_t            receiver,
                                       OTF2_CommRef        communicator,
                                       uint32_t            msgTag,
                                       uint64_t            msgLength )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIComm )
  {
    tr->handleMPIComm( tr, MPI_SEND, locationID, receiver, 0, msgTag );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiISend( OTF2_LocationRef    locationID,
                                        OTF2_TimeStamp      time,
                                        void*               userData,
                                        OTF2_AttributeList* attributeList,
                                        uint32_t            receiver,
                                        OTF2_CommRef        communicator,
                                        uint32_t            msgTag,
                                        uint64_t            msgLength,
                                        uint64_t            requestID )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIIsend )
  {
    tr->handleMPIIsend( tr, locationID, receiver, requestID );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiISendComplete( OTF2_LocationRef    locationID,
                                                OTF2_TimeStamp      time,
                                                void*               userData,
                                                OTF2_AttributeList* attributeList,
                                                uint64_t            requestID )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIIsendComplete )
  {
    tr->handleMPIIsendComplete( tr, locationID, requestID );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiIRecvRequest( OTF2_LocationRef    locationID,
                                               OTF2_TimeStamp      time,
                                               void*               userData,
                                               OTF2_AttributeList* attributeList,
                                               uint64_t            requestID )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIIrecvRequest )
  {
    tr->handleMPIIrecvRequest( tr, locationID, requestID );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2Callback_MpiIRecv( OTF2_LocationRef    locationID,
                                        OTF2_TimeStamp      time,
                                        void*               userData,
                                        OTF2_AttributeList* attributeList,
                                        uint32_t            sender,
                                        OTF2_CommRef        communicator,
                                        uint32_t            msgTag,
                                        uint64_t            msgLength,
                                        uint64_t            requestID )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleMPIIrecv )
  {
    tr->handleMPIIrecv( tr, locationID, sender, requestID );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalEvtReaderCallback_ThreadFork( 
                                  OTF2_LocationRef    locationID,
                                  OTF2_TimeStamp      time,
                                  void*               userData,
                                  OTF2_AttributeList* attributeList,
                                  OTF2_Paradigm       paradigm,
                                  uint32_t            numberOfRequestedThreads )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  //\todo: handle numberOfRequestedThreads
  return OTF2TraceReader::otf2CallbackEnter( locationID, time, userData,
                                             attributeList,
                                             tr->getOmpForkJoinRef() );
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalEvtReaderCallback_ThreadJoin( OTF2_LocationRef locationID,
                                                          OTF2_TimeStamp   time,
                                                          void*            userData,
                                                          OTF2_AttributeList*
                                                          attributeList,
                                                          OTF2_Paradigm
                                                          paradigm )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  return OTF2TraceReader::otf2CallbackLeave( locationID, time, userData,
                                             attributeList,
                                             tr->getOmpForkJoinRef( ) );
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackComm_RmaWinDestroy( 
                                            OTF2_LocationRef    location,
                                            OTF2_TimeStamp      time,
                                            void*               userData,
                                            OTF2_AttributeList* attributeList,
                                            OTF2_RmaWinRef      win )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleRmaWinDestroy )
  {
    tr->handleRmaWinDestroy( tr, time - tr->getTimerOffset() , location );
  }
  
  return OTF2_CALLBACK_SUCCESS;
}

std::string
OTF2TraceReader::getStringRef( Token t )
{
  return getKeyName( t );
}

/*
 *  Just reading communication events to place them again into the output trace
 */
void
OTF2TraceReader::readCommunication( )
{

}

OTF2TraceReader::TokenNameMap&
OTF2TraceReader::getDefinitionTokenStringMap( )
{
  return definitionTokenStringMap;
}

uint64_t
OTF2TraceReader::getTimerResolution( )
{
  return ticksPerSecond;
}

void
OTF2TraceReader::setTimerResolution( uint64_t ticksPerSecond )
{
  this->ticksPerSecond = ticksPerSecond;
}

uint64_t
OTF2TraceReader::getTimerOffset( )
{
  return timerOffset;
}

void
OTF2TraceReader::setTimerOffset( uint64_t offset )
{
  this->timerOffset = offset;
}

uint64_t
OTF2TraceReader::getTraceLength( )
{
  return traceLength;
}

void
OTF2TraceReader::setTraceLength( uint64_t length )
{
  this->traceLength = length;
}

uint32_t
OTF2TraceReader::getOmpForkJoinRef( )
{
  return ompForkJoinRef;
}

uint32_t
OTF2TraceReader::getMPIRank( )
{
  return mpiRank;
}

uint32_t
OTF2TraceReader::getMPISize( )
{
  return mpiSize;
}

/**
 * Translate the OTF2 string reference to a string object.
 * 
 * @param id OTF2 string reference
 * @return string object the OTF2 string reference refers to
 */
std::string
OTF2TraceReader::getKeyName( uint32_t id )
{
  TokenNameMap& nm = getDefinitionTokenStringMap( );
  TokenNameMap::iterator iter = nm.find( id );
  if ( iter != nm.end( ) )
  {
    return iter->second;
  }
  else
  {
    UTILS_MSG( true, "Could not translate OTF2 string reference %u to string "
                     "object!", id );
    
    return "(unknown)";
  }
}

/**
* Get the name of the function by its OTF2 region id (reference).
* 
* @param id OTF2 region ID (reference)
* @return string object containing the name of the function
*/
std::string
OTF2TraceReader::getFunctionName( uint32_t id )
{
  // use the OTF2 region reference/ID to get the OTF2 string reference and 
  // translate it to a string with getKeyName  
  return getKeyName( getFunctionNameTokenMap( )[id] );
}

OTF2TraceReader::ProcessGroupMap&
OTF2TraceReader::getProcGoupMap( )
{
  return processGroupMap;
}

/*std::string
OTF2TraceReader::getProcessName( uint64_t id )
{
  return getKeyName( id );
}*/

std::vector< uint32_t >
OTF2TraceReader::getKeys( const std::string keyName )
{
  std::vector< uint32_t > keys;

  std::pair< NameTokenMap::iterator, NameTokenMap::iterator > range;
  range = nameKeysMap.equal_range( keyName );
  for ( NameTokenMap::iterator iter = range.first; iter != range.second; ++iter )
  {
    keys.push_back( iter->second );
  }
  return keys;
}

int32_t
OTF2TraceReader::getFirstKey( const std::string keyName )
{
  std::pair< NameTokenMap::iterator, NameTokenMap::iterator > range;
  range = nameKeysMap.equal_range( keyName );

  if ( range.first != range.second )
  {
    return range.first->second;
  }
  else
  {
    return -1;
  }
}

void*
OTF2TraceReader::getUserData( )
{
  return userData;
}

bool
OTF2TraceReader::isChildOf( uint64_t child, uint64_t parent )
{
  TokenTokenMap64::const_iterator iter = processFamilyMap.find( child );
  if ( iter == processFamilyMap.end( ) )
  {
    throw RTException( "Requesting parent of unknown child process" );
  }

  bool myOwnParent = false;

  iter = processFamilyMap.find( child );

  while ( !myOwnParent )
  {
    uint64_t directParent = iter->second;

    if ( directParent == parent )
    {
      return true;
    }

    iter = processFamilyMap.find( directParent );
    if ( iter == processFamilyMap.end( ) )
    {
      return false;
    }

    if ( directParent == iter->second )
    {
      myOwnParent = true;
    }
  }

  return false;
}
