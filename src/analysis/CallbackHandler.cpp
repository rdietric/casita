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

#include <cuda.h>

#include "CallbackHandler.hpp"

using namespace casita;
using namespace casita::io;

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
       ( ( options.verbose >= VERBOSE_BASIC ) &&
         ( !node->isEventNode( ) ||
           ( ( (EventNode*)node )->
             getFunctionResult( ) ==
             EventNode::FR_SUCCESS ) ) ) )
  {
    printf( " [%u]", mpiRank );
    if ( node->isEnter( ) )
    {
      printf( " E " );
    }
    else
    {
      printf( " L " );
    }

    printf(
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
      printf( ", ref = %lu", refProcess );
    }

    if ( node->isLeave( ) && node->isEventNode( ) )
    {
      printf( ", event = %u, result = %u",
              ( (EventNode*)node )->getEventId( ),
              ( (EventNode*)node )->getFunctionResult( ) );
    }

    printf( "\n" );
  }
}

void
CallbackHandler::applyStreamRefsEnter( ITraceReader* reader,
                                       GraphNode* node,
                                       IKeyValueList* list,
                                       Paradigm paradigm )
{
  uint64_t refValue = 0;
  int32_t streamRefKey = -1;

  switch ( paradigm )
  {
    case PARADIGM_CUDA:
      streamRefKey = reader->getFirstKey( VT_CUPTI_CUDA_STREAMREF_KEY );
      /* give it another try, maybe it was scorep, not vt */
      if ( streamRefKey < 0 )
      {
        streamRefKey = reader->getFirstKey( SCOREP_CUPTI_CUDA_STREAMREF_KEY );
      }

      if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
           list->getLocationRef( (uint32_t)streamRefKey, &refValue ) == 0 )
      {
        node->setReferencedStreamId( refValue );
      }
      break;

    case PARADIGM_OMP:
      if ( this->analysis.getStream( node->getStreamId( ) )->getStreamType( )
           == EventStream::ES_DEVICE )
      {
        uint64_t key_value = 0;
        streamRefKey = reader->getFirstKey( SCOREP_POMP_TARGET_REGION_ID_KEY );
        if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
             list->getUInt64( (uint32_t)streamRefKey, &key_value ) == 0 )
        {
          this->analysis.findOmpTargetParentRegion( node, key_value );
        }

        streamRefKey = reader->getFirstKey(
          SCOREP_POMP_TARGET_PARENT_REGION_ID_KEY );
        if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
             list->getUInt64( (uint32_t)streamRefKey, &key_value ) == 0 )
        {
          this->analysis.pushOmpTargetRegion( node, key_value );

          if ( node->isOMPSync( ) )
          {
            uint32_t ompParentCtrId = this->analysis.getCtrTable( ).getCtrId( CTR_OMP_PARENT_REGION_ID );
            std::cout << node->getUniqueName( ) << " has parent id " << key_value << std::endl;
            node->setCounter( ompParentCtrId, key_value );
          }
        }
      }

    default:
      return;
  }
}

void
CallbackHandler::applyStreamRefsLeave( ITraceReader* reader,
                                       GraphNode* node,
                                       GraphNode* oldNode,
                                       IKeyValueList* list,
                                       Paradigm paradigm )
{
  uint64_t refValue = 0;
  int32_t streamRefKey = -1;

  switch ( paradigm )
  {
    case PARADIGM_CUDA:
      streamRefKey = reader->getFirstKey( VT_CUPTI_CUDA_STREAMREF_KEY );
      /* give it another try, maybe it was scorep, not vt */
      if ( streamRefKey < 0 )
      {
        streamRefKey = reader->getFirstKey( SCOREP_CUPTI_CUDA_STREAMREF_KEY );
      }

      if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
           list->getLocationRef( (uint32_t)streamRefKey, &refValue ) == 0 )
      {
        node->setReferencedStreamId( refValue );
        oldNode->setReferencedStreamId( refValue );
      }
      break;

    case PARADIGM_OMP:
      streamRefKey = reader->getFirstKey( SCOREP_POMP_TARGET_LOCATIONREF_KEY );

      if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
           list->getLocationRef( (uint32_t)streamRefKey, &refValue ) == 0 )
      {
        node->setReferencedStreamId( refValue );
      }
      break;

    default:
      return;
  }
}

uint32_t
CallbackHandler::readKeyVal( ITraceReader* reader,
                             const char* keyName,
                             IKeyValueList* list )
{
  uint32_t keyVal = 0;
  int32_t key = reader->getFirstKey( keyName );
  if ( key > -1 && list )
  {
    list->getUInt32( (uint32_t)key, &keyVal );
  }

  return keyVal;
}

void
CallbackHandler::handleProcessMPIMapping( ITraceReader* reader,
                                          uint64_t streamId,
                                          uint32_t mpiRank )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  handler->getAnalysis( ).getMPIAnalysis( ).setMPIRank( streamId, mpiRank );
}

void
CallbackHandler::handleDefProcess( ITraceReader* reader,
                                   uint32_t stream,
                                   uint64_t streamId,
                                   uint64_t parentId,
                                   const char* name,
                                   IKeyValueList* list,
                                   bool isCUDA,
                                   bool isCUDANull )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine& analysis = handler->getAnalysis( );

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

  if ( handler->getOptions( ).verbose >= VERBOSE_BASIC )
  {
    printf( "  [%u] Found stream %s (%lu) with type %u, stream %u\n",
            analysis.getMPIRank( ), name, streamId, streamType, stream );
  }

  analysis.newEventStream( streamId, parentId, name, streamType, PARADIGM_CUDA );
}

void
CallbackHandler::handleDefFunction( ITraceReader* reader,
                                    uint64_t streamId,
                                    uint32_t functionId,
                                    const char* name,
                                    uint32_t functionGroupId )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  handler->getAnalysis( ).addFunction( functionId, name );
}

void
CallbackHandler::handleEnter( ITraceReader* reader,
                              uint64_t time,
                              uint32_t functionId,
                              uint64_t streamId,
                              IKeyValueList* list )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine& analysis = handler->getAnalysis( );
  ProgramOptions& options = handler->getOptions( );

  EventStream* stream = analysis.getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Process %lu not found.", streamId );
  }

  const char* funcName = reader->getFunctionName( functionId ).c_str( );

  FunctionDescriptor functionType;
  AnalysisEngine::getFunctionType( functionId, funcName, stream, &functionType );

  if ( functionType.paradigm == PARADIGM_VT )
  {
    return;
  }
  
  if ( functionType.paradigm == PARADIGM_CPU )
  {
      //std::cout << " skipping " << funcName << std::endl;
      analysis.addCPUEvent(time, streamId);
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

  /* CUDA specific*/
  if ( enterNode->isCUDAKernelLaunch( ) )
  {
    handler->applyStreamRefsEnter( reader,
                                   enterNode,
                                   list,
                                   PARADIGM_CUDA );
    analysis.addPendingKernelLaunch( enterNode );
  }

  /* OMP specific */
  if ( enterNode->isOMP( ) )
  {
    handler->applyStreamRefsEnter( reader,
                                   enterNode,
                                   list,
                                   PARADIGM_OMP );
  }

  handler->printNode( enterNode, stream );
  options.eventsProcessed++;
}

void
CallbackHandler::handleLeave( ITraceReader* reader,
                              uint64_t time,
                              uint32_t functionId,
                              uint64_t streamId,
                              IKeyValueList* list )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine& analysis = handler->getAnalysis( );
  ProgramOptions& options = handler->getOptions( );

  EventStream* stream = handler->getAnalysis( ).getStream( streamId );
  if ( !stream )
  {
    throw RTException( "Process %lu not found", streamId );
  }

  const char* funcName = reader->getFunctionName( functionId ).c_str( );

  FunctionDescriptor functionType;
  AnalysisEngine::getFunctionType( functionId, funcName, stream, &functionType );

  if ( functionType.paradigm == PARADIGM_VT )
  {
    return;
  }
  
  if ( functionType.paradigm == PARADIGM_CPU )
  {
      //std::cout << " skipping " << funcName << std::endl;
      analysis.addCPUEvent(time, streamId);
        return;
  }

  GraphNode* leaveNode = NULL;
  if ( Node::isCUDAEventType( functionType.paradigm, functionType.type ) )
  {
    /**\todo implement for Score-P keys */
    uint32_t eventId = readKeyVal( reader, VT_CUPTI_CUDA_EVENTREF_KEY, list );
    CUresult cuResult = (CUresult)readKeyVal( reader,
                                              VT_CUPTI_CUDA_CURESULT_KEY,
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

  //CallbackHandler::applyStreamRefsLeave( reader, leaveNode,
    //                                     leaveNode->getGraphPair( ).first, list );
  
    /* MPI specific */
  if ( leaveNode->isMPI( ) )
  {
    EventStream::MPICommRecordList mpiCommRecords =
      stream->getPendingMPIRecords( );
    for ( EventStream::MPICommRecordList::const_iterator iter =
            mpiCommRecords.begin( );
          iter != mpiCommRecords.end( ); ++iter )
    {
      uint64_t* tmpId = NULL;

      switch ( iter->mpiType )
      {
        case EventStream::MPI_RECV:
          leaveNode->setReferencedStreamId( iter->partnerId );
          break;

        case EventStream::MPI_COLLECTIVE:
          leaveNode->setReferencedStreamId( iter->partnerId );
          break;

        case EventStream::MPI_ONEANDALL:
          leaveNode->setReferencedStreamId( iter->partnerId );
          tmpId = new uint64_t;
          *tmpId = iter->rootId;
          leaveNode->setData( tmpId );
          break;

        case EventStream::MPI_SEND:
          tmpId = new uint64_t;
          *tmpId = iter->partnerId;
          leaveNode->setData( tmpId );
          break;

        default:
          throw RTException( "Not a valid MPICommRecord type here" );
      }
    }
  }

  /* CUDA specific */
  if ( leaveNode->isCUDA( ) )
  {
    handler->applyStreamRefsLeave( reader,
                                   leaveNode,
                                   leaveNode->getGraphPair( ).first,
                                   list,
                                   PARADIGM_CUDA );

    if ( leaveNode->isCUDAKernelLaunch( ) )
    {
      analysis.addPendingKernelLaunch( leaveNode );
    }
  }

  /* OMP specific */
  if ( leaveNode->isOMP( ) )
  {
    if ( leaveNode->isOMPTargetFlush( ) )
    {
      handler->applyStreamRefsLeave( reader,
                                     leaveNode,
                                     leaveNode->getGraphPair( ).first,
                                     list,
                                     PARADIGM_OMP );
    }

    if ( leaveNode->isOMPParallelRegion( ) && ( stream->getStreamType( ) ==
                                                EventStream::ES_DEVICE ) )
    {
      analysis.popOmpTargetRegion( leaveNode );
    }
  }
  
  handler->printNode( leaveNode, stream );
  options.eventsProcessed++;
}

void
CallbackHandler::handleMPIComm( ITraceReader* reader,
                                MPIType mpiType,
                                uint64_t streamId,
                                uint64_t partnerId,
                                uint32_t root,
                                uint32_t tag )
{
  CallbackHandler* handler = (CallbackHandler*)( reader->getUserData( ) );
  AnalysisEngine& analysis = handler->getAnalysis( );

  EventStream* stream = analysis.getStream( streamId );

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
    default:
      throw RTException( "Unknown cdm::io::MPIType %u", mpiType );
  }

  if ( handler->getOptions( ).verbose >= VERBOSE_ALL )
  {
    printf( " [%u] mpi record, [%lu > %lu], type %u, tag %u\n",
            analysis.getMPIRank( ),
            streamId, partnerId,
            pMPIType, tag );
  }

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
