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

#if defined(SCOREP_USER_ENABLE)
#include "scorep/SCOREP_User.h"
#endif

#define OTF2_CHECK( cmd ) \
  { \
    int _status = cmd; \
    if ( _status ) { throw RTException( "OTF2 command '%s' returned error", #cmd );} \
  }

using namespace casita::io;

OTF2TraceReader::OTF2TraceReader( void*                  userData,
                                  OTF2DefinitionHandler* defHandler,
                                  uint32_t               mpiRank,
                                  uint32_t               mpiSize ) :
  handleEnter( NULL ),
  handleLeave( NULL ),
  handleDefProcess( NULL ),
  handleLocationProperty( NULL ),
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
  defHandler( defHandler ),
  mpiRank( mpiRank ),
  mpiSize( mpiSize ),
  reader( NULL )
{

}

OTF2TraceReader::~OTF2TraceReader()
{
  /* delete members of mpi-groups */
  for ( CommGroupMap::iterator iter = groupMap.begin();
        iter != groupMap.end(); iter++ )
  {
    delete[]iter->second.members;
  }
}

OTF2TraceReader::NameTokenMap&
OTF2TraceReader::getNameKeysMap()
{
  return nameKeysMap;
}

OTF2KeyValueList&
OTF2TraceReader::getKVList()
{
  return kvList;
}

void
OTF2TraceReader::open( const std::string otfFilename, uint32_t maxFiles )
{
  baseFilename.assign( "" );
  baseFilename.append( otfFilename.c_str(), otfFilename.length() );
  reader = OTF2_Reader_Open( baseFilename.c_str() );

  // TODO: why?
  OTF2_Reader_SetSerialCollectiveCallbacks( reader );

  if ( !reader )
  {
    throw RTException( "Failed to open OTF2 trace file %s", baseFilename.c_str() );
  }
}

void
OTF2TraceReader::close()
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
  // processNameTokenMap is initialized during traceReader->readDefinitions();
  for ( LocationStringRefMap::const_iterator iter = locationStringRefMap.begin();
        iter != locationStringRefMap.end(); ++iter )
  {
    OTF2_Reader_SelectLocation( reader, iter->first );
  }
  
  OTF2_Reader_OpenEvtFiles( reader );
  OTF2_Reader_OpenDefFiles( reader );

  for ( LocationStringRefMap::const_iterator iter = locationStringRefMap.begin();
        iter != locationStringRefMap.end(); ++iter )
  {
    OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, iter->first );
    uint64_t def_reads         = 0;
    OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
    OTF2_Reader_CloseDefReader( reader, def_reader );
    OTF2_Reader_GetEvtReader( reader, iter->first );
  }

  OTF2_Reader_CloseDefFiles( reader );

  OTF2_GlobalEvtReader* global_evt_reader = OTF2_Reader_GetGlobalEvtReader( reader );

  OTF2_GlobalEvtReaderCallbacks* event_callbacks =
    OTF2_GlobalEvtReaderCallbacks_New();
  OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks,
                                                  &otf2CallbackEnter );
  OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks,
                                                  &otf2CallbackLeave );
  
  // if only a single process is used, MPI events can be ignored
  if( mpiSize > 1 )
  {
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback(
    event_callbacks,&otf2Callback_MpiCollectiveEnd );
    OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback( event_callbacks,
                                                      &otf2Callback_MpiRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback( event_callbacks,
                                                      &otf2Callback_MpiSend );
  }
  
  OTF2_GlobalEvtReaderCallbacks_SetThreadForkCallback(
    event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadFork );
  OTF2_GlobalEvtReaderCallbacks_SetThreadJoinCallback(
    event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadJoin );
  
  // explicit for OpenMP
  //OTF2_GlobalEvtReaderCallbacks_SetOmpForkCallback(
  //  event_callbacks,OTF2_GlobalEvtReaderCallback_ThreadFork );
  
  // registered because this might be the last event in a stream
  OTF2_GlobalEvtReaderCallbacks_SetRmaWinDestroyCallback(
    event_callbacks, &otf2CallbackComm_RmaWinDestroy );
  
  /* register RMA communication callbacks */
  OTF2_GlobalEvtReaderCallbacks_SetRmaGetCallback(
    event_callbacks, &otf2CallbackComm_RmaGet );
  OTF2_GlobalEvtReaderCallbacks_SetRmaPutCallback(
    event_callbacks, &otf2CallbackComm_RmaPut );
  OTF2_GlobalEvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(
    event_callbacks, &otf2CallbackComm_RmaOpCompleteBlocking );
  
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
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION( "readEvents", SCOREP_USER_REGION_TYPE_FUNCTION )
#endif
    
  OTF2_GlobalEvtReader* global_evt_reader =
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
             "[0] ... %" PRIu64 " events read", *events_read );

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
    OTF2_GlobalEvtReaderCallbacks_New();
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
OTF2TraceReader::readDefinitions()
{
  OTF2_GlobalDefReader* global_def_reader = 
    OTF2_Reader_GetGlobalDefReader( reader );

  OTF2_GlobalDefReaderCallbacks* global_def_callbacks =
    OTF2_GlobalDefReaderCallbacks_New();

  // set definition callbacks
  OTF2_GlobalDefReaderCallbacks_SetAttributeCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Attribute );
  
  OTF2_GlobalDefReaderCallbacks_SetStringCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_String );
  
  OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_ClockProperties );
  
  OTF2_GlobalDefReaderCallbacks_SetLocationCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_Location );
  
  // register for location properties, e.g. to detect CUDA null stream
  OTF2_GlobalDefReaderCallbacks_SetLocationPropertyCallback(
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_LocationProperty );
  
  // only root rank evaluates these information
  if( mpiRank == 0 )
  {
    OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback( 
    global_def_callbacks, &OTF2_GlobalDefReaderCallback_LocationGroup );
  
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeCallback(
      global_def_callbacks, &OTF2_GlobalDefReaderCallback_SystemTreeNode );
  }
  
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
             "  ... %" PRIu64 " OTF2 definitions read", definitions_read );

  close();  
  
  if ( ( rankStreamMap.size() == 0 && mpiSize > 1 ) || 
       ( rankStreamMap.size() && (mpiSize != rankStreamMap.size() ) ) )
  {
    UTILS_OUT( "[%u] CASITA has to be run with %zu MPI process(es)!",
               mpiRank, rankStreamMap.size() == 0 ? 1 : rankStreamMap.size() );
    
    return false;
  }

  open( baseFilename.c_str(), 10 );
  
  // add forkJoin "region" internally to support OMP-fork/join model  
  // and create wait state region
  defHandler->setInternalRegions();

  /* check OTF2 location reference, MPI rank map 
  if( mpiRank == 0 && mpiSize > 1 )
  {
    TokenTokenMap64::iterator iter = processFamilyMap.begin();
    for( ; iter != processFamilyMap.end(); ++iter )
    {
      UTILS_OUT( "[0] stream %" PRIu64 " -> rank %" PRIu64 "", 
                 iter->first, iter->second );
    }
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

  tr->defHandler->setTimerResolution( timerResolution );
  tr->defHandler->setTimerOffset( globalOffset );
  tr->defHandler->setTraceLength( traceLength );

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
  
  OTF2_SystemTreeNodeRef nodeRef = tr->locationGrpSysNodeRefMap[ locationGroup ];
  OTF2_StringRef nodeStringRef = tr->sysNodeStringRefMap[ nodeRef ];
  
  // used for statistics output
  if( tr->mpiRank == 0 && tr->defHandler->haveStringRef( nodeStringRef ) &&
      self == locationGroup )
  {
    const char* nodeName = tr->defHandler->getName( nodeStringRef );
    //UTILS_OUT( "Location %" PRIu64 " on node %s", self, nodeName );
    
    tr->defHandler->addLocationInfo( self, nodeName ); 
  }
  
  // generate mapping of stream IDs to MPI ranks for all processes/streams
  if ( tr->handleProcessMPIMapping )
  {
    tr->handleProcessMPIMapping( tr, self, locationGroup );
  }
  
  // for all locations of this MPI rank (this analysis process)
  if( tr->mpiRank == locationGroup )
  {
    tr->locationStringRefMap[ self ] = name;
    
    if ( tr->handleDefProcess )
    {
      tr->handleDefProcess( tr, self, locationGroup, 
                            tr->defHandler->getName( name ), NULL, 
                            locationType );
    }
  }
  
  /* ignore unknown locations
  if( locationType == OTF2_LOCATION_TYPE_METRIC || 
      locationType == OTF2_LOCATION_TYPE_UNKNOWN )
  {
    if ( tr->isChildOf( self, tr->getMPIProcessId() ) )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "Ignore unknown location %s", 
                 tr->getStringRef( name ).c_str() );
    }
    
    return OTF2_CALLBACK_SUCCESS;
  }*/

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_LocationProperty( 
                                                  void*               userData,
                                                  OTF2_LocationRef    location,
                                                  OTF2_StringRef      name,
                                                  OTF2_Type           type,
                                                  OTF2_AttributeValue value )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  if( tr->defHandler->haveStringRef( name ) )
  {
    UTILS_MSG( Parser::getInstance().getVerboseLevel() >= VERBOSE_BASIC, 
               "[%" PRIu64 "] Found location property %s", 
               location, tr->defHandler->getName( name ) );
    
    // location strings are only stored for locations of this MPI rank
    if( tr->locationStringRefMap.count( location ) > 0 )
    {
      tr->handleLocationProperty(
        tr, location, tr->defHandler->getName( name ), type, value );
    }
  }
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_LocationGroup( 
                                void*                  userData,
                                OTF2_LocationGroupRef  self,
                                OTF2_StringRef         name,
                                OTF2_LocationGroupType locationGroupType,
                                OTF2_SystemTreeNodeRef systemTreeParent)
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  // if this is a process  
  if ( locationGroupType == OTF2_LOCATION_GROUP_TYPE_PROCESS )
  {
    tr->locationGrpSysNodeRefMap[ self ] = systemTreeParent;
    
//    if( tr->defHandler->haveStringRef( name ) )
//    {
//      UTILS_OUT( "Location group: %s", tr->defHandler->getName( name ) );
//    }
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_SystemTreeNode( 
                                              void*                  userData,
                                              OTF2_SystemTreeNodeRef self,
                                              OTF2_StringRef         name,
                                              OTF2_StringRef         className,
                                              OTF2_SystemTreeNodeRef parent )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  if( tr->defHandler->haveStringRef( name ) )
  {
    //UTILS_OUT( "System tree node: %s", tr->defHandler->getName( name ) );
    tr->sysNodeStringRefMap[ self ] = name;
  }
  
//  if( tr->defHandler->haveStringRef( className ) )
//  {
//    UTILS_OUT( "System tree node class: %s", tr->defHandler->getName( className ) );
//  }

  return OTF2_CALLBACK_SUCCESS;
}


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
 * @param members identifiers of the group members (OTF2 location references)
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
  
  // store groups with paradigm MPI (MPI communication groups)
  // also store empty groups which seem to be used in some rare cases (e.g. ug4)
  if( paradigm == OTF2_PARADIGM_MPI /*&& numberOfMembers > 0*/ )
  {
    UTILS_MSG_NOBR( tr->mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC && 
                    name != OTF2_UNDEFINED_STRING, 
                    "  [0] OTF2 MPI group definition %u: %s (",  
                    self, tr->defHandler->getName( name ) );

    // store group members
    uint32_t* myMembers = new uint32_t[ numberOfMembers ];
    for ( uint32_t i = 0; i < numberOfMembers; i++ )
    {
      myMembers[ i ] = members[ i ];
      UTILS_MSG_NOBR( tr->mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                      " %" PRIu64, members[ i ] );
    }
    UTILS_MSG( tr->mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC, " )" );

    OTF2Group myGroup;
    myGroup.groupId         = self;
    myGroup.members         = myMembers;
    myGroup.numberOfMembers = numberOfMembers;
    myGroup.paradigm        = paradigm;
    myGroup.stringRef       = name;
    myGroup.groupType       = groupType;
    myGroup.groupFlag       = groupFlags;

    // store the group in a map
    tr->groupMap[ self ] = myGroup;
  
    // all MPI locations (to map  global ranks to OTF2 location IDs)
    if( groupType == OTF2_GROUP_TYPE_COMM_LOCATIONS )
    {
      // the number of MPI members has to be more than the rank number
      if ( numberOfMembers <= tr->mpiRank )
      {
        throw RTException(
          "Process group MPI_COMM_WORLD has no process for this MPI rank (%u)",
          tr->mpiRank );
      }

      // save all members of the global MPI group with their rank
      for ( uint32_t i = 0; i < numberOfMembers; ++i )
      {
        tr->rankStreamMap[ i ] = members[ i ];
      }
      
      // handle the global group of all MPI ranks (replaces MPI_COMM_WORLD)
      if ( tr->handleMPICommGroup )
      {
        tr->handleMPICommGroup( tr, 0, myGroup.numberOfMembers, myGroup.members );
      }
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

  // get the associated communication group
  CommGroupMap::iterator iter = tr->groupMap.find( group );
  
  // if group was not found, it is not of paradigm MPI
  // \todo: not empty
  if( iter == tr->groupMap.end() )
  {
    return OTF2_CALLBACK_SUCCESS;
  }
  
  OTF2Group& myGroup = iter->second;
  
  UTILS_MSG( tr->mpiRank == 0 && Parser::getVerboseLevel() >= VERBOSE_BASIC && 
             name != OTF2_UNDEFINED_STRING, 
             "  [0] OTF2 communicator definition %u: %s (%u) (group %u)",  
             self, tr->defHandler->getName( name ), name, myGroup.groupId );
  
  if ( myGroup.paradigm == OTF2_PARADIGM_MPI ) // only MPI is stored
  {
    // make sure that no other definition of MPI_COMM_WORLD is written
    if( self == 0 && myGroup.numberOfMembers != tr->mpiSize )
    {
      UTILS_WARNING( "OTF2 MPI communicator 0 already set!" );
      return OTF2_CALLBACK_SUCCESS;
    }
    
    if ( tr->handleMPICommGroup )
    {
      tr->handleMPICommGroup( tr, self, myGroup.numberOfMembers,
                              myGroup.members );
    }
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::OTF2_GlobalDefReaderCallback_String( void*          userData,
                                                      OTF2_StringRef self,
                                                      const char*    name )
{

  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  tr->defHandler->storeString( self, name );
  
  //UTILS_MSG( tr->mpiRank == 0, "Read string definition for %s (%u)", 
  //           name, self );

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

  tr->defHandler->addRegion( self, paradigm, regionRole, name );

  if ( tr->handleDefFunction )
  {
    tr->handleDefFunction( tr, self, tr->defHandler->getRegionName( self ), 
                           paradigm, regionRole );
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
  std::string      s  = tr->defHandler->getName( name );
  tr->getNameKeysMap().insert( std::make_pair( s, self ) );

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
  OTF2TraceReader* tr = ( OTF2TraceReader* )userData;

  if ( tr->handleEnter )
  {
    OTF2KeyValueList& kvList = tr->getKVList();
    kvList.setList( attributes );

    tr->handleEnter( tr, time - tr->defHandler->getTimerOffset(), region, 
                     location, &kvList );
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
    OTF2KeyValueList& kvList = tr->getKVList();
    kvList.setList( attributes );

    bool interrupt = tr->handleLeave( tr, time - tr->defHandler->getTimerOffset(), 
                                      region, location, &kvList );
    
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
    
    uint64_t rootStreamId = std::numeric_limits< uint64_t >::max();
    
    if( mpiType == io::MPI_ONEANDALL )
    {
      RankStreamIdMap::iterator it = tr->rankStreamMap.find( root );
      
      if ( it != tr->rankStreamMap.end() )
      {
        rootStreamId = tr->rankStreamMap[root];
      }
      else
      {
        throw RTException( "Could not find stream ID for root rank (%u)", root );
        rootStreamId = root; // use root rank instead
      }
    }

    tr->handleMPIComm( tr, mpiType, locationID, communicator, rootStreamId, 0 );
  }

  return OTF2_CALLBACK_SUCCESS;
}

/**
 * 
 * @param locationID
 * @param time
 * @param userData
 * @param attributeList
 * @param sender MPI rank of sender in communicator.
 * @param communicator
 * @param msgTag
 * @param msgLength
 * @return 
 */
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
    tr->handleMPIComm( tr, MPI_RECV, locationID, sender, communicator, msgTag );
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
    tr->handleMPIComm( tr, MPI_SEND, locationID, receiver, communicator, msgTag );
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
    tr->handleMPIIsend( tr, locationID, receiver, communicator, msgTag, requestID );
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
    tr->handleMPIIrecv( tr, locationID, sender, communicator, msgTag, requestID );
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

  // handle fork node as enter event
  OTF2_CallbackCode ret = 
    OTF2TraceReader::otf2CallbackEnter( locationID, time, userData,
                                        attributeList,
                                        tr->defHandler->getForkJoinRegionId() );
  
  // add the requested threads to the enter event
  if ( tr->handleThreadFork )
  {
    tr->handleThreadFork( tr, locationID, numberOfRequestedThreads );
  }
  
  return ret;;
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
                                             tr->defHandler->getForkJoinRegionId() );
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
    tr->handleRmaWinDestroy( tr, time - tr->defHandler->getTimerOffset() , location );
  }
  
  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackComm_RmaPut( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  void*            userData,
                                                  OTF2_AttributeList* attributes,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleRmaPut )
  {
    tr->handleRmaPut( tr, time - tr->defHandler->getTimerOffset() , location );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackComm_RmaOpCompleteBlocking( 
                                           OTF2_LocationRef    location,
                                           OTF2_TimeStamp      time,
                                           void*               userData,
                                           OTF2_AttributeList* attributeList,
                                           OTF2_RmaWinRef      win,
                                           uint64_t            matchingId )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;
  
  if ( tr->handleRmaOpCompleteBlocking )
  {
    tr->handleRmaOpCompleteBlocking( tr, time - tr->defHandler->getTimerOffset() , location );
  }

  return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode
OTF2TraceReader::otf2CallbackComm_RmaGet( OTF2_LocationRef location,
                                                  OTF2_TimeStamp   time,
                                                  void*            userData,
                                                  OTF2_AttributeList*
                                                  attributeList,
                                                  OTF2_RmaWinRef   win,
                                                  uint32_t         remote,
                                                  uint64_t         bytes,
                                                  uint64_t         matchingId )
{
  OTF2TraceReader* tr = (OTF2TraceReader*)userData;

  if ( tr->handleRmaGet )
  {
    tr->handleRmaGet( tr, time - tr->defHandler->getTimerOffset() , location );
  }
  
  return OTF2_CALLBACK_SUCCESS;
}


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
OTF2TraceReader::getUserData()
{
  return userData;
}
