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

#include "CallbackHandler.hpp"

#include "omp/AnalysisParadigmOMP.hpp"

#define CUDA_SUCCESS 0

using namespace casita;
using namespace casita::io;
using namespace casita::omp;

CallbackHandler::CallbackHandler( ProgramOptions& options,
                                  AnalysisEngine& analysis ) :
  options( options ),
  analysis( analysis ),
  mpiRank( analysis.getMPIRank( ) )
{

}

ProgramOptions&
CallbackHandler::getOptions( )
{
  return options;
}

AnalysisEngine&
CallbackHandler::getAnalysis( )
{
  return analysis;
}

void
CallbackHandler::printNode( GraphNode* node, EventStream* stream )
{
  if ( ( options.verbose >= VERBOSE_ALL ) ||
       ( ( options.verbose > VERBOSE_BASIC ) &&
         ( !node->isEventNode( ) ||
           ( ( (EventNode*)node )->
             getFunctionResult( ) ==
             EventNode::FR_SUCCESS ) ) ) )
  {
    fprintf( stderr, " [%u]", mpiRank );
    if ( node->isEnter( ) )
    {
      fprintf( stderr, " E " );
    }
    else
    {
      fprintf( stderr, " L " );
    }

    fprintf( stderr,
             "[%12lu:%12.8fs:%10u,%5lu] [%20.20s] proc [%15s], pid [%11lu], [%s]",
             node->getTime( ),
             (double)( node->getTime( ) ) / (double)analysis.getTimerResolution( ),
             node->getId( ),
             node->getFunctionId( ),
             node->getName( ),
             stream->getName( ),
             stream->getId( ),
             Node::typeToStr( node->getParadigm( ), node->getType( ) ).c_str( ) );

    uint64_t refProcess = node->getReferencedStreamId( );
    if ( refProcess )
    {
      fprintf( stderr, ", ref = %lu", refProcess );
    }

    if ( node->isLeave( ) && node->isEventNode( ) )
    {
      fprintf( stderr, ", event = %u, result = %u",
               ( (EventNode*)node )->getEventId( ),
               ( (EventNode*)node )->getFunctionResult( ) );
    }

    fprintf( stderr, "\n" );
    fflush( stderr );
  }
}

uint32_t
CallbackHandler::readKeyVal( ITraceReader*  reader,
                             const char*    keyName,
                             IKeyValueList* list )
{
  uint32_t keyVal = 0;
  int32_t  key    = reader->getFirstKey( keyName );
  if ( key > -1 && list )
  {
    list->getUInt32( (uint32_t)key, &keyVal );
  }

  return keyVal;
}

void
CallbackHandler::handleProcessMPIMapping( ITraceReader* reader,
                                          uint64_t      streamId,
                                          uint32_t      mpiRank )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  handler->getAnalysis( ).getMPIAnalysis( ).setMPIRank( streamId, mpiRank );
}

void
CallbackHandler::handleDefProcess( ITraceReader*  reader,
                                   uint32_t       stream,
                                   uint64_t       streamId,
                                   uint64_t       parentId,
                                   const char*    name,
                                   IKeyValueList* list,
                                   bool           isCUDA,
                                   bool           isCUDANull )
{
  CallbackHandler* handler  =
    (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );

  EventStream::EventStreamType streamType = EventStream::ES_HOST;

  if ( isCUDANull )
  {
    streamType = EventStream::ES_DEVICE_NULL;
  }
  else
  {
    if ( isCUDA )
    {
      streamType = EventStream::ES_DEVICE;
    }

    if ( strstr( name, "MIC" ) )
    {
      streamType = EventStream::ES_DEVICE;
    }
  }

  UTILS_DBG_MSG( handler->getOptions( ).verbose >= VERBOSE_BASIC,
                 "  [%u] Found stream %s (%lu) with type %u, stream %u",
                 analysis.getMPIRank( ), name, streamId, streamType, stream );

  analysis.newEventStream( streamId, parentId, name, streamType, PARADIGM_CUDA );
}

void
CallbackHandler::handleDefFunction( ITraceReader* reader,
                                    uint64_t      streamId,
                                    uint32_t      functionId,
                                    const char*   name,
                                    uint32_t      functionGroupId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  handler->getAnalysis( ).addFunction( functionId, name );
}

void
CallbackHandler::handleEnter( ITraceReader*  reader,
                              uint64_t       time,
                              uint32_t       functionId,
                              uint64_t       streamId,
                              IKeyValueList* list )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  ProgramOptions&  options  = handler->getOptions( );

  EventStream*     stream   = analysis.getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Process %lu not found.", streamId );
  }

  const char* funcName      = reader->getFunctionName( functionId ).c_str( );

  FunctionDescriptor functionType;
  AnalysisEngine::getFunctionType( functionId, funcName, stream, &functionType,
                                   handler->getOptions( ).ignoreAsyncMpi );

  // for CPU functions no graph node is created
  // only start time, end time and number of CPU events between nodes is stored
  if ( functionType.paradigm == PARADIGM_CPU )
  {
    analysis.addCPUEvent( time, streamId );
    return;
  }

  GraphNode* enterNode = NULL;
  if ( Node::isCUDAEventType( functionType.paradigm, functionType.type ) )
  {
    enterNode = analysis.addNewEventNode( time,
                                          0,
                                          EventNode::FR_UNKNOWN,
                                          stream,
                                          funcName,
                                          functionType.paradigm,
                                          RECORD_ENTER,
                                          functionType.type );
  }
  else
  {
    enterNode = analysis.addNewGraphNode( time,
                                          stream,
                                          funcName,
                                          functionType.paradigm,
                                          RECORD_ENTER,
                                          functionType.type );
  }

  enterNode->setFunctionId( functionId );

  analysis.handleKeyValuesEnter( reader, enterNode, list );
  analysis.handlePostEnter( enterNode );

  handler->printNode( enterNode, stream );
  options.eventsProcessed++;
}

void
CallbackHandler::handleLeave( ITraceReader*  reader,
                              uint64_t       time,
                              uint32_t       functionId,
                              uint64_t       streamId,
                              IKeyValueList* list )
{
  CallbackHandler* handler  = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  ProgramOptions&  options  = handler->getOptions( );

  EventStream*     stream   = handler->getAnalysis( ).getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Process %lu not found", streamId );
  }

  const char* funcName      = reader->getFunctionName( functionId ).c_str( );

  FunctionDescriptor functionType;
  AnalysisEngine::getFunctionType( functionId, funcName, stream, &functionType,
                                   handler->getOptions( ).ignoreAsyncMpi );

  if ( functionType.paradigm == PARADIGM_CPU )
  {
    /* std::cout << " skipping " << funcName << std::endl; */
    analysis.addCPUEvent( time, streamId );
    return;
  }

  GraphNode* leaveNode = NULL;
  if ( Node::isCUDAEventType( functionType.paradigm, functionType.type ) )
  {
    uint32_t eventId  = readKeyVal(
      reader,
      SCOREP_CUPTI_CUDA_EVENTREF_KEY,
      list );
    uint32_t cuResult = readKeyVal(
      reader,
      SCOREP_CUPTI_CUDA_CURESULT_KEY,
      list );
    EventNode::FunctionResultType fResult = EventNode::FR_UNKNOWN;
    if ( cuResult == CUDA_SUCCESS )
    {
      fResult = EventNode::FR_SUCCESS;
    }

    leaveNode = handler->getAnalysis( ).addNewEventNode( time,
                                                         eventId,
                                                         fResult,
                                                         stream,
                                                         funcName,
                                                         functionType.paradigm,
                                                         RECORD_LEAVE,
                                                         functionType.type );

    if ( eventId == 0 )
    {
      throw RTException( "No eventId for event node %s found",
                         leaveNode->getUniqueName( ).c_str( ) );
    }
  }
  else
  {
    leaveNode = analysis.addNewGraphNode( time,
                                          stream,
                                          funcName,
                                          functionType.paradigm,
                                          RECORD_LEAVE,
                                          functionType.type );
  }

  leaveNode->setFunctionId( functionId );

  analysis.handleKeyValuesLeave( reader, leaveNode, leaveNode->getGraphPair( ).first, list );
  
  // additional handling for special nodes (e.g. MPI communication)
  analysis.handlePostLeave( leaveNode );

  handler->printNode( leaveNode, stream );
  options.eventsProcessed++;
}

/**
 * Handle blocking MPI communication.
 * 
 * @param reader
 * @param mpiType
 * @param streamId
 * @param partnerId
 * @param root
 * @param tag
 */
void
CallbackHandler::handleMPIComm( ITraceReader* reader,
                                MPIType       mpiType,
                                uint64_t      streamId,
                                uint64_t      partnerId,
                                uint32_t      root,
                                uint32_t      tag )
{
  CallbackHandler*     handler  = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&      analysis = handler->getAnalysis( );

  EventStream*         stream   = analysis.getStream( streamId );

  EventStream::MPIType pMPIType;

  switch ( mpiType )
  {
    case io::MPI_COLLECTIVE:
      pMPIType = EventStream::MPI_COLLECTIVE;
      break;
    case io::MPI_RECV:
      pMPIType = EventStream::MPI_RECV;
      break;
    case io::MPI_SEND:
      pMPIType = EventStream::MPI_SEND;
      break;
    case io::MPI_ONEANDALL:
      pMPIType = EventStream::MPI_ONEANDALL;
      break;
    default: throw RTException( "Unknown cdm::io::MPIType %u", mpiType );
  }

  UTILS_DBG_MSG( handler->getOptions( ).verbose > VERBOSE_ALL,
                 " [%u] mpi record, [%lu > %lu], type %u, tag %u",
                 analysis.getMPIRank( ),
                 streamId, partnerId,
                 pMPIType, tag );

  stream->setPendingMPIRecord( pMPIType, partnerId, root );
}

void
CallbackHandler::handleMPICommGroup( ITraceReader* reader, uint32_t group,
                                     uint32_t numProcs, const uint64_t* procs )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );

  handler->getAnalysis( ).getMPIAnalysis( ).setMPICommGroupMap( group,
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
CallbackHandler::handleMPIIsend( ITraceReader* reader, 
                                 uint64_t      streamId,
                                 uint64_t      receiver,
                                 uint64_t      requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  EventStream*     stream   = analysis.getStream( streamId );
  
  stream->handleMPIIsendEventData( requestId, receiver );
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
CallbackHandler::handleMPIIrecv( ITraceReader* reader, 
                                 uint64_t      streamId,
                                 uint64_t      sender,
                                 uint64_t      requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  EventStream*     stream   = analysis.getStream( streamId );
  
  stream->handleMPIIrecvEventData( requestId, sender );
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
CallbackHandler::handleMPIIrecvRequest( ITraceReader* reader, 
                                        uint64_t streamId, 
                                        uint64_t requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  EventStream*     stream   = analysis.getStream( streamId );
  
  stream->saveMPIIrecvRequest( requestId );
}

void
CallbackHandler::handleMPIIsendComplete( ITraceReader* reader, 
                                         uint64_t streamId, 
                                         uint64_t requestId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine&  analysis = handler->getAnalysis( );
  EventStream*     stream   = analysis.getStream( streamId );
  
  stream->saveMPIIsendRequest( requestId );
}