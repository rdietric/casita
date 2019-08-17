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
 */

#include "CallbackHandler.hpp"

#include "omp/AnalysisParadigmOMP.hpp"

#define CUDA_SUCCESS 0

using namespace casita;
using namespace casita::io;
using namespace casita::omp;

CallbackHandler::CallbackHandler( AnalysisEngine& analysis ) :
  analysis( analysis ),
  mpiRank( analysis.getMPIRank() )
{
  
}

AnalysisEngine&
CallbackHandler::getAnalysis()
{
  return analysis;
}

void
CallbackHandler::setDefinitionHandler( OTF2DefinitionHandler* defHandler )
{
  this->defHandler = defHandler;
}

void
CallbackHandler::printNode( GraphNode* node, EventStream* stream )
{
  if ( ( Parser::getInstance().getVerboseLevel() >= VERBOSE_ALL ) ||
       ( ( Parser::getInstance().getVerboseLevel() > VERBOSE_BASIC ) &&
         ( !node->isEventNode() ||
           ( ( (EventNode*)node )->
             getFunctionResult() ==
             EventNode::FR_SUCCESS ) ) ) )
  {
    fprintf( stderr, " [%u]", mpiRank );
    if ( node->isEnter() )
    {
      fprintf( stderr, " E " );
    }
    else if( node->isLeave()  )
    {
      fprintf( stderr, " L " );
    }
    else if( node->isAtomic() )
    {
      fprintf( stderr, " A " );
    }
    else
    {
      fprintf( stderr, " S " );
    }

    fprintf( stderr,
             "[%12" PRIu64 "(%12.8fs):%10" PRIu64 ",%5" PRIu64 "] [%20.20s] on "
             "[%15s:%11" PRIu64 "], [%s]",
             node->getTime(), analysis.getRealTime( node->getTime() ),
             node->getId(), node->getFunctionId(),
             node->getName(),
             stream->getName(), stream->getId(),
             Node::typeToStr( node->getParadigm(), node->getType() ).c_str() );

    uint64_t refProcess = node->getReferencedStreamId();
    if ( refProcess )
    {
      fprintf( stderr, ", ref = %lu", refProcess );
    }

    if ( node->isLeave() && node->isEventNode() )
    {
      fprintf( stderr, ", event = %" PRIu64 ", result = %u",
               ( (EventNode*)node )->getEventId(),
               ( (EventNode*)node )->getFunctionResult() );
    }

    fprintf( stderr, "\n" );
    fflush( stderr );
  }
}

uint32_t
CallbackHandler::readAttributeUint32( OTF2TraceReader*  reader,
                                      const char*       keyName,
                                      OTF2KeyValueList* list )
{
  uint32_t keyVal = 0;
  int32_t  key    = reader->getFirstKey( keyName );
  
  if ( key > -1 && list && list->getSize() > 0 && 
       list->testAttribute( (uint32_t)key ) )
  {
    list->getUInt32( (uint32_t)key, &keyVal );
  }
  else
  {
    CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
    AnalysisEngine&  analysis = handler->getAnalysis();
    UTILS_WARNING( "[%" PRIu32 "] No value for key %s found", 
                   analysis.getMPIRank(), keyName );
  }

  return keyVal;
}

uint64_t
CallbackHandler::readAttributeUint64( OTF2TraceReader* reader,
                                      const char*       keyName,
                                      OTF2KeyValueList* list )
{
  uint64_t keyVal = 0;
  int32_t  key    = reader->getFirstKey( keyName );
  if ( key > -1 && list )
  {
    list->getUInt64( (uint32_t)key, &keyVal );
  }

  return keyVal;
}

void
CallbackHandler::handleProcessMPIMapping( OTF2TraceReader* reader,
                                          uint64_t         streamId,
                                          uint32_t         mpiRank )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  handler->getAnalysis().getMPIAnalysis().setMPIRank( streamId, mpiRank );
}

void
CallbackHandler::handleDefProcess( OTF2TraceReader*  reader,
                                   uint64_t          streamId,
                                   uint64_t          parentId, //location group
                                   const char*       name,
                                   OTF2KeyValueList* list,
                                   OTF2_LocationType locationType )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();
  
  bool isGPU = locationType == OTF2_LOCATION_TYPE_GPU ? true : false;
  
  EventStream::EventStreamType streamType = EventStream::ES_HOST;
  
  //\todo: check whether that is always true
  if( streamId == parentId )
  {
    streamType = EventStream::ES_MPI;
  }
  else
  {
    streamType = EventStream::ES_OPENMP;
  }

  if ( isGPU )
  {
      streamType = EventStream::ES_DEVICE;
  }
    
  if ( strstr( name, "MIC" ) || strstr( name, "target" ) )
  {
    streamType = EventStream::ES_DEVICE;

    analysis.addDetectedParadigm( PARADIGM_OMP );
    analysis.addDetectedParadigm( PARADIGM_OMP_TARGET );
  }
  else if ( strstr( name, "OMP thread" ) )
  {
    analysis.addDetectedParadigm( PARADIGM_OMP );
  }
  
  if ( locationType == OTF2_LOCATION_TYPE_METRIC )
  {
    UTILS_OUT( "  [%u] Ignore metric stream '%s' (%" PRIu64 
               ") with type %u, parent %" PRIu64,
               analysis.getMPIRank(), name, streamId, streamType, parentId );
    
    return;
  }

  UTILS_MSG( Parser::getInstance().getVerboseLevel() >= VERBOSE_BASIC,
             "  [%u] Found stream '%s' (%" PRIu64 ") with type %u, parent %" PRIu64,
             analysis.getMPIRank(), name, streamId, streamType, parentId );

  analysis.newEventStream( streamId, parentId, name, streamType );
}

void
CallbackHandler::handleLocationProperty( OTF2TraceReader*    reader,
                                         uint64_t            streamId,
                                         const char*         name,
                                         OTF2_Type           type,
                                         OTF2_AttributeValue value )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();
  
  if( strcmp ( name, SCOREP_CUDA_NULL_STREAM ) == 0 )
  {
    //UTILS_MSG( Parser::getInstance().getVerboseLevel() >= VERBOSE_BASIC, 
    //           "[%"PRIu64"] Found CUDA null stream", streamId );
    if( strcmp ( handler->defHandler->getName( value.stringRef ), "yes" ) == 0 )
    {
      //UTILS_OUT( "Found CUDA null stream == yes" );
      DeviceStream* stream = 
        analysis.getStreamGroup().getDeviceStream( streamId );
      analysis.getStreamGroup().setDeviceNullStream( stream );
    }
  }
}

void
CallbackHandler::handleDefRegion( OTF2TraceReader* reader,
                                  uint32_t         regionId,
                                  const char*      name,
                                  OTF2_Paradigm    paradigm,
                                  OTF2_RegionRole  regionRole )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();
  
  // add 
  if( strcmp( name, "cuEventRecord" ) == 0 && paradigm == OTF2_PARADIGM_CUDA )
  {
    analysis.addAnalysisFeature( CUDA_EVENTS );
  }
  else if( 0 == strcmp(
          Parser::getInstance().getProgramOptions().predictionFilter.c_str(), 
                                                            name ) )
  {
    UTILS_WARNING( "Found definition of filtered function: %s", name );
    analysis.addFilteredRegion( regionId );
  }
  
  //\todo: check for MPI paradigm
  //if( paradigm == OTF2_PARADIGM_MPI )
}

void
CallbackHandler::handleDefAttribute( OTF2TraceReader* reader,
                                     uint64_t         streamId,
                                     uint32_t         attributeId, 
                                     const char*      name )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();
  
  // add attribute ID
  analysis.getCtrTable().addAttributeId( attributeId );
  
  // Check the defined OTF2 attributes for the predefined CUDA_STREAM_REF 
  // If not defined, do not perform CUDA analysis!
  if( strcmp( name, SCOREP_CUDA_STREAMREF ) == 0 )
  {
    analysis.addDetectedParadigm( PARADIGM_CUDA );
  }
  
  // Check the defined OTF2 attributes for the predefined SCOREP_OPENCL_QUEUEREF 
  // If not defined, do not perform OpenCL analysis!
  if( strcmp( name, SCOREP_OPENCL_QUEUEREF ) == 0 )
  {
    analysis.addDetectedParadigm( PARADIGM_OCL );
  }
  
  // if we found the definition of an OMPT parallel ID
  if( strcmp( name, SCOREP_OMPT_PARALLEL_ID ) == 0 )
  {
    analysis.addDetectedParadigm( PARADIGM_OMPT );
    UTILS_MSG( Parser::getInstance().getVerboseLevel() >= VERBOSE_BASIC &&
               analysis.getMPIRank() == 0,
               "[OpenMP] Using OMPT analysis flavor." );
  }
}

void
CallbackHandler::handleEnter( OTF2TraceReader*  reader,
                              uint64_t          time,
                              uint32_t          functionId,
                              uint64_t          streamId,
                              OTF2KeyValueList* list )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();

  EventStream* stream = analysis.getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Process %lu not found.", streamId );
  }
  
  // save the time stamp of the first enter event
  if( stream->getPeriod().first > time )
  {
    stream->getPeriod().first = time;
  }
  
  // save the time stamp of the last enter event
  if( stream->getPeriod().second < time )
  {
    stream->getPeriod().second = time;
  }

  const RegionInfo& regionInfo = handler->defHandler->getRegionInfo( functionId );
  const char* funcName   = regionInfo.name;
  
  if( stream->isFilterOn() )
  {
    UTILS_OUT( "Filtering nested region %s", funcName );
    
    return;
  }
  
  if( analysis.isRegionFiltered( functionId ) )
  {
    if( stream->isFilterOn() )
    {
      UTILS_WARNING( "Region %s is nested into already filtered function.", 
                     funcName );
      
      return;
    }
    else
    {
      UTILS_OUT( "Enable filter for %s (%u)", funcName, functionId );
    
      // set the filter to on (ignore nested regions)
      stream->setFilter( true, time );
    }
  }

  FunctionDescriptor functionDesc;
  functionDesc.recordType = RECORD_ENTER; // needed to determine correct function type
  bool generateNode = 
    FunctionTable::getAPIFunctionType( &functionDesc, funcName, regionInfo.paradigm,
    stream->isDeviceStream(), analysis.getStreamGroup().deviceWithNullStreamOnly(), 
    analysis.getMPISize() == 1 );

  // do not create nodes for CPU events and MPI events in 1-Process-Programs
  if( !generateNode )
  {    
    //UTILS_OUT( "CPU event: %s", funcName );
//    analysis.addCPUEvent( time, streamId, false );
    return;
  }

  GraphNode* enterNode = NULL;
  if ( Node::isCUDAEventType( functionDesc.paradigm, functionDesc.functionType ) )
  {
    enterNode = analysis.addNewEventNode( time,
                                          0,
                                          EventNode::FR_UNKNOWN,
                                          stream,
                                          funcName,
                                          &functionDesc );
  }
  else
  {
    enterNode = analysis.addNewGraphNode( time,
                                          stream,
                                          funcName,
                                          &functionDesc );
  }

  enterNode->setFunctionId( functionId );

  analysis.handleKeyValuesEnter( reader, enterNode, list );
  analysis.handlePostEnter( enterNode );

  handler->printNode( enterNode, stream );
}

/**
 * 
 * @param reader
 * @param time
 * @param functionId
 * @param streamId OTF2 location ID / reference
 * @param list
 * 
 * @return true, if it is a global collective leave event
 */
bool
CallbackHandler::handleLeave( OTF2TraceReader*  reader,
                              uint64_t          time,
                              uint32_t          functionId,
                              uint64_t          streamId,
                              OTF2KeyValueList* list )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();

  EventStream* stream = analysis.getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Stream %" PRIu64 " not found!", streamId );
  }
  
  // save the time stamp of the last leave event
  if( stream->getPeriod().second < time )
  {
    stream->getPeriod().second = time;
  }

  const RegionInfo& regionInfo = handler->defHandler->getRegionInfo( functionId );
  const char* funcName   = regionInfo.name;
  
  if( analysis.isRegionFiltered( functionId ) )
  {
    UTILS_OUT( "Disable filter for %s", funcName );

    stream->setFilter( false, time );
  }
  
  if( stream->isFilterOn() )
  {
    return false;
  }

  FunctionDescriptor regionDesc;
  regionDesc.recordType = RECORD_LEAVE; // needed to determine correct function type
  bool generateNode = 
    FunctionTable::getAPIFunctionType( &regionDesc, funcName, regionInfo.paradigm,
    stream->isDeviceStream(), analysis.getStreamGroup().deviceWithNullStreamOnly(),
    analysis.getMPISize() == 1 );

  // do not create nodes for CPU events and MPI events in 1-Process-Programs
  if( !generateNode )
  {
    //std::cout << " skipping " << funcName << std::endl;
//    analysis.addCPUEvent( time, streamId, true );
    return false;
  }

  GraphNode* leaveNode = NULL;
  if ( Node::isCUDAEventType( regionDesc.paradigm, regionDesc.functionType ) )
  {
    uint64_t eventId = readAttributeUint64( reader, SCOREP_CUDA_EVENTREF, list );
    
    if ( eventId == 0 )
    {
      UTILS_WARNING( "No eventId for event found" );
      return false;
    }

    EventNode::FunctionResultType fResult = EventNode::FR_UNKNOWN;
    
    // get the function result cuEventQuery
    if( regionDesc.functionType & OFLD_QUERY_EVT )
    {
      uint32_t cuResult = readAttributeUint32( reader, SCOREP_CUDA_CURESULT, list );
      if ( cuResult == CUDA_SUCCESS )
      {
        fResult = EventNode::FR_SUCCESS;
      }
    }
    
    leaveNode = handler->getAnalysis().addNewEventNode( time,
                                                        eventId,
                                                        fResult,
                                                        stream,
                                                        funcName,
                                                        &regionDesc );
  }
  else
  {
    leaveNode = analysis.addNewGraphNode( time,
                                          stream,
                                          funcName,
                                          &regionDesc );
  }

  leaveNode->setFunctionId( functionId );

  GraphNode* enterNode = leaveNode->getGraphPair().first;
  
  // applied for offloading paradigms only
  analysis.handleKeyValuesLeave( reader, leaveNode, enterNode, list );
  
  // additional handling for special nodes (e.g. MPI communication and OpenMP)
  analysis.handlePostLeave( leaveNode );

  // for debugging
  handler->printNode( leaveNode, stream );
  
  //UTILS_MSG( functionType.paradigm == PARADIGM_CUDA, 
  //           "[%"PRIu64"] Adding CUDA event %s", 
  //           streamId, leaveNode->getUniqueName().c_str() );
  
  // if analysis should be run in intervals (between global collectives)
  if ( analysis.getMPISize() > 1 && 
       Parser::getOptions().analysisInterval &&
      // if we have read a global blocking collective, we can start the analysis
       ( leaveNode->isMPICollective() /*|| leaveNode->isMPIAllToOne() || leaveNode->isMPIOneToAll()*/ ) &&
       !( leaveNode->isMPIInit() ) && !( leaveNode->isMPIFinalize() ) )
  {
    const uint32_t mpiGroupId = leaveNode->getReferencedStreamId();
    const MPIAnalysis::MPICommGroup& mpiCommGroup =
      analysis.getMPIAnalysis().getMPICommGroup( mpiGroupId ); 

    // if the collective is global (collective group size == number of analysis ranks)
    if ( mpiCommGroup.procs.size() == analysis.getMPISize() )
    {
      // mark as global operation over all processes
      leaveNode->addType( MPI_ALLRANKS );
      
      analysis.getMPIAnalysis().globalCollectiveCounter++;
      
      UTILS_MSG( Parser::getInstance().getVerboseLevel() >= VERBOSE_ANNOY, 
                 "[%u] Global collective: %s", 
                 streamId, leaveNode->getUniqueName().c_str() );
      
      return true;
    }
  }
  
  return false;
}

/**
 * Add the requested threads to the fork node. Assumes that the respective 
 * enter node is the last created node.
 * 
 * @param reader the trace reader object
 * @param streamId the OTF2 location ID, which is internally used as stream ID
 * @param requestedThreads the number of requested threads
 */
void
CallbackHandler::handleThreadFork( OTF2TraceReader* reader,
                                   uint64_t         streamId,
                                   uint32_t         requestedThreads )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  EventStream*     stream  = handler->getAnalysis().getStream( streamId );
  
  if ( !stream )
  {
    throw RTException( "Stream %" PRIu64 " not found!", streamId );
  }
  
  GraphNode* forkNode = stream->getLastNode();
  if( forkNode && forkNode->isOMPForkJoin() )
  {
    forkNode->setReferencedStreamId( requestedThreads );
  }
  else
  {
    UTILS_WARNING( "Could not handle fork node!" );
  }
}

/**
 * Handle RMA window destroy events as they are most often the last events in a 
 * stream.
 * 
 * @param location
 * @param time
 * @param userData
 * @param attributeList
 * @param win
 * @return 
 */
void
CallbackHandler::handleRmaWinDestroy( OTF2TraceReader* reader,
                                      uint64_t         time,
                                      uint64_t         streamId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  EventStream*     stream  = handler->getAnalysis().getStream( streamId );
  
  if ( !stream )
  {
    throw RTException( "Stream %" PRIu64 " not found!", streamId );
  }
  
  // save the time stamp of the last leave event
  if( stream->getLastEventTime() < time )
  {
    stream->setLastEventTime( time );
  }
}

void
CallbackHandler::handleRmaPut( OTF2TraceReader* reader,
                               uint64_t         time,
                               uint64_t         streamId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  EventStream*     stream  = handler->getAnalysis().getStream( streamId );
  
  // handle host-device transfers as device transfer tasks
  if( stream->isDeviceStream() )
  {
    handler->handleEnter( reader, time, handler->defHandler->getD2HRegionId(), 
                          streamId, NULL );
  }
}

void
CallbackHandler::handleRmaGet( OTF2TraceReader* reader,
                               uint64_t         time,
                               uint64_t         streamId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  EventStream*     stream  = handler->getAnalysis().getStream( streamId );
  
  // handle host-device transfers as device transfer tasks
  if( stream->isDeviceStream() )
  {
    handler->handleEnter( reader, time, handler->defHandler->getH2DRegionId(), 
                          streamId, NULL );
  }
}

void
CallbackHandler::handleRmaOpCompleteBlocking( OTF2TraceReader* reader,
                                              uint64_t         time,
                                              uint64_t         streamId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  EventStream*     stream  = handler->getAnalysis().getStream( streamId );
  
  if( stream->isDeviceStream() )
  {
    uint32_t matchingRegionId = handler->defHandler->getH2DRegionId();
    
    handler->handleLeave( reader, time, matchingRegionId, streamId, NULL );
  }
}


/**
 * Handle blocking MPI communication.
 * 
 * @param reader
 * @param mpiType type of MPI communication
 * @param streamId OTF2 location ID / reference
 * @param partnerId MPI rank of communication partner in communicator "root_comm"
 * @param root_comm MPI rank of root process in a collective or 
 *                  OTF2 communicator reference in MPI_Send/MPI_Recv
 * @param tag MPI message tag
 */
void
CallbackHandler::handleMPIComm( OTF2TraceReader* reader,
                                MPIType          mpiType,
                                uint64_t         streamId,
                                uint32_t         partnerId,
                                uint32_t         root_comm,
                                uint32_t         tag )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData() );
  AnalysisEngine&  analysis = handler->getAnalysis();
  MpiStream*       stream   = analysis.getStreamGroup().getMpiStream( streamId );

  MpiStream::MPIType pMPIType;

  switch ( mpiType )
  {
    case io::MPI_COLLECTIVE:
      pMPIType = MpiStream::MPI_COLLECTIVE;
      break;
    case io::MPI_RECV:
      pMPIType = MpiStream::MPI_RECV;
      break;
    case io::MPI_SEND:
      pMPIType = MpiStream::MPI_SEND;
      break;
    case io::MPI_ONEANDALL:
      //pMPIType = EventStream::MPI_ONEANDALL;
      pMPIType = MpiStream::MPI_COLLECTIVE;
      break;
    default: throw RTException( "Unknown io::MPIType %u", mpiType );
  }

  UTILS_MSG( Parser::getInstance().getVerboseLevel() > VERBOSE_ALL,
             " [%u] mpi record, [%lu > %lu], type %u, tag %u",
             analysis.getMPIRank(),
             streamId, partnerId,
             pMPIType, tag );

  stream->setPendingMPIRecord( pMPIType, partnerId, root_comm, tag );
  
  // trace statistics
  //analysis.blockingMPI
}

/**
 * 
 * @param reader
 * @param group OTF2 communicator reference
 * @param numProcs number of member processes
 * @param procs 
 */
void
CallbackHandler::handleMPICommGroup( OTF2TraceReader* reader, 
                                     OTF2_CommRef communicator,
                                     uint32_t numProcs, const uint32_t* procs )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );

  handler->getAnalysis().getMPIAnalysis().addMPICommGroup( communicator, 
                                                           numProcs,
                                                           procs );
}

/**
 * MPI_Isend communication record.
 * 
 * @param reader pointer to the internal OTF2 trace reader
 * @param streamId ID of the active stream
 * @param receiver ID of the communication partner stream
 * @param requestId OTF2 ID of the request handle
 */
void
CallbackHandler::handleMPIIsend( OTF2TraceReader* reader, 
                                 uint64_t         streamId,
                                 uint64_t         receiver,
                                 OTF2_CommRef     communicator,
                                 uint32_t         msgTag,
                                 uint64_t         requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  MpiStream*       stream  = 
    handler->getAnalysis().getStreamGroup().getMpiStream( streamId );
  
  stream->handleMPIIsendEventData( requestId, receiver, communicator, msgTag );
}

/**
 * MPI_Irecv communication record.
 * 
 * Fourth callback that appears for an MPI_Irecv operation. It brings the sender 
 * (communication partner) in the game. The request ID is used to get the
 * matching MPI_Irecv leave node of the stream it occurred on. It is the third
 * record in the MPI_Irecv chain.
 * 
 * @param reader pointer to the internal OTF2 trace reader
 * @param streamId ID of the active stream
 * @param sender ID of the communication partner stream
 * @param request OTF2 ID of the MPI_Irecv request handle
 */
void
CallbackHandler::handleMPIIrecv( OTF2TraceReader* reader, 
                                 uint64_t         streamId,
                                 uint64_t         sender,
                                 OTF2_CommRef     communicator,
                                 uint32_t         msgTag,
                                 uint64_t         requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  MpiStream*       stream  = 
    handler->getAnalysis().getStreamGroup().getMpiStream( streamId );
  
  stream->handleMPIIrecvEventData( requestId, sender, communicator, msgTag );
}

/**
 * MPI_Irecv request record. 
 * 
 * Is enclosed in the MPI_Irecv enter and leave record. It is the first record
 * in the MPI_Irecv chain. Adds a pending MPI_Irecv request in the active stream. 
 * 
 * @param reader
 * @param streamId
 * @param request
 */
void
CallbackHandler::handleMPIIrecvRequest( OTF2TraceReader* reader, 
                                        uint64_t streamId, 
                                        uint64_t requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  MpiStream*       stream  = 
    handler->getAnalysis().getStreamGroup().getMpiStream( streamId );
  
  stream->saveMPIIrecvRequest( requestId );
}

void
CallbackHandler::handleMPIIsendComplete( OTF2TraceReader* reader, 
                                         uint64_t streamId, 
                                         uint64_t requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData() );
  MpiStream*       stream  = 
    handler->getAnalysis().getStreamGroup().getMpiStream( streamId );
  
  stream->saveMPIIsendRequest( requestId );
}
