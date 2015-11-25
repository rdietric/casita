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
 * What this file does:
 * Basic functions to perform analysis of the read events, apply rules
 * - contains the different analysis classes for the different paradigms
 * - helper functions (getFunctionName, getter, addGraphNode,
 * - prepare and trigger writing new OTF2 file
 *
 */

#include <stdio.h>
#include <mpi.h>
#include <list>
#include <stack>

#include "IAnalysisParadigm.hpp"
#include "AnalysisEngine.hpp"
#include "common.hpp"

#include "cuda/AnalysisParadigmCUDA.hpp"
#include "mpi/AnalysisParadigmMPI.hpp"
#include "omp/AnalysisParadigmOMP.hpp"

#include "otf/OTF2ParallelTraceWriter.hpp"

using namespace casita;
using namespace casita::io;

AnalysisEngine::AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize ) :
  mpiAnalysis( mpiRank, mpiSize ),
  writer( NULL ),
  maxFunctionId( 0 ),
  //pendingMPICommForWaitAll( 0 ),
  waitStateFuncId( 0 ),
  foundCUDA( false ),
  foundOMP( false )
{
  // add analysis paradigms
  // \todo: only if paradigm found
  // \todo: Where deleted?
  addAnalysisParadigm( new cuda::AnalysisParadigmCUDA( this ) );
  addAnalysisParadigm( new omp::AnalysisParadigmOMP( this ) );
  addAnalysisParadigm( new mpi::AnalysisParadigmMPI( this, mpiRank, mpiSize ) );
}

AnalysisEngine::~AnalysisEngine( )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    delete iter->second;
  }
  if ( writer != NULL )
  {
    writer->close( );
    delete writer;
  }
}

uint32_t
AnalysisEngine::getMPIRank( )
{
  return mpiAnalysis.getMPIRank( );
}

uint32_t
AnalysisEngine::getMPISize( )
{
  return mpiAnalysis.getMPISize( );
}

MPIAnalysis&
AnalysisEngine::getMPIAnalysis( )
{
  return mpiAnalysis;
}

//\todo: not implemented for OMP and MPI
void 
AnalysisEngine::setParadigmFound( Paradigm paradigm )
{
  if ( paradigm == PARADIGM_CUDA )
  {
    foundCUDA = true;
  }
  else if( paradigm == PARADIGM_OMP )
  {
    foundOMP = true;
  }
}

bool 
AnalysisEngine::haveParadigm( Paradigm paradigm )
{
  //\todo: check not implemented
  if( paradigm == PARADIGM_MPI )
  {
    return true;
  }
  else if( paradigm == PARADIGM_OMP )
  {
    return foundOMP;
  }
  else if ( paradigm == PARADIGM_CUDA )
  {
    return foundCUDA;
  }
  else
  {
    return false;
  }
}

bool
AnalysisEngine::getFunctionType( uint32_t            id,
                                 const char*         name,
                                 EventStream*        stream,
                                 FunctionDescriptor* descr,
                                 bool                ignoreAsyncMpi )
{
  assert( name );
  assert( descr );
  assert( stream );

  return FunctionTable::getAPIFunctionType( name, descr, stream->isDeviceStream( ),
                                            stream->isDeviceNullStream( ), ignoreAsyncMpi );
}

void
AnalysisEngine::addFunction( uint32_t funcId, const char* name )
{
  maxFunctionId       = std::max( maxFunctionId, funcId );
  functionMap[funcId] = name;
}

uint32_t
AnalysisEngine::getNewFunctionId( )
{
  return ++maxFunctionId;
}

void
AnalysisEngine::setWaitStateFunctionId( uint32_t id )
{
  waitStateFuncId = id;
  functionMap[waitStateFuncId] = "WaitState";
}

const char*
AnalysisEngine::getFunctionName( uint32_t id )
{
  std::map< uint32_t, std::string >::const_iterator iter =
    functionMap.find( id );
  if ( iter != functionMap.end( ) )
  {
    return iter->second.c_str( );
  }
  else
  {
    return NULL;
  }
}

bool
AnalysisEngine::applyRules( GraphNode* node, Paradigm paradigm, bool verbose )
{
  AnalysisParadigmsMap::const_iterator iter = analysisParadigms.find( paradigm );
  if ( iter == analysisParadigms.end( ) )
  {
    return false;
  }
  else
  {
    return iter->second->applyRules( node, verbose );
  }
}

void
AnalysisEngine::addAnalysisParadigm( IAnalysisParadigm* paradigm )
{
  assert( paradigm );
  if ( paradigm )
  {
    analysisParadigms[paradigm->getParadigm( )] = paradigm;
  }
}

IAnalysisParadigm*
AnalysisEngine::getAnalysisParadigm( Paradigm paradigm )
{
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( paradigm );
  if ( iter == analysisParadigms.end( ) )
  {
    return NULL;
  }
  else
  {
    return iter->second;
  }
}

void
AnalysisEngine::handlePostEnter( GraphNode* node )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handlePostEnter( node );
  }
}

void
AnalysisEngine::handlePostLeave( GraphNode* node )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handlePostLeave( node );
  }
}

void
AnalysisEngine::handleKeyValuesEnter( ITraceReader*  reader,
                                      GraphNode*     node,
                                      IKeyValueList* list )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handleKeyValuesEnter( reader, node, list );
  }
}

void
AnalysisEngine::handleKeyValuesLeave( ITraceReader*  reader,
                                      GraphNode*     node,
                                      GraphNode*     oldNode,
                                      IKeyValueList* list )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handleKeyValuesLeave( reader, node, oldNode, list );
  }
}

EventStream*
AnalysisEngine::getNullStream( ) const
{
  return streamGroup.getNullStream( );
}

GraphNode*
AnalysisEngine::getLastLeave( uint64_t timestamp, uint64_t streamId ) const
{
  /* find last leave record on stream streamId before timestamp */
  EventStream* stream = getStream( streamId );
  if ( !stream )
  {
    return NULL;
  }

  EventStream::SortedGraphNodeList& nodes = stream->getNodes( );
  for ( EventStream::SortedGraphNodeList::const_reverse_iterator rIter =
          nodes.rbegin( );
        rIter != nodes.rend( ); ++rIter )
  {
    GraphNode* node = *rIter;
    if ( !node->isLeave( ) || node->isMPI( ) )
    {
      continue;
    }

    if ( node->getTime( ) <= timestamp )
    {
      return node;
    }
  }

  return NULL;
}

GraphNode*
AnalysisEngine::newGraphNode( uint64_t          time,
                              uint64_t          streamId,
                              const std::string name,
                              Paradigm          paradigm,
                              NodeRecordType    recordType,
                              int               nodeType )
{
  GraphNode* node = GraphEngine::newGraphNode( time, streamId, name,
                                               paradigm, recordType, nodeType );

  if ( node->isWaitstate( ) )
  {
    node->setFunctionId( waitStateFuncId );
  }

  return node;
}

GraphNode*
AnalysisEngine::addNewGraphNode( uint64_t       time,
                                 EventStream*   stream,
                                 const char*    name,
                                 Paradigm       paradigm,
                                 NodeRecordType recordType,
                                 int            nodeType )
{
  GraphNode* node = GraphEngine::addNewGraphNode( time, stream, name, paradigm,
                                                  recordType, nodeType );

  if ( node->isWaitstate( ) )
  {
    node->setFunctionId( waitStateFuncId );
  }

  return node;
}

void
AnalysisEngine::reset( )
{
  GraphEngine::reset( );
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->reset( );
  }
}

size_t
AnalysisEngine::getNumAllDeviceStreams( )
{
  return streamGroup.getNumStreams( ) - streamGroup.getNumHostStreams( );
}

io::IParallelTraceWriter::ActivityGroupMap*
AnalysisEngine::getActivityGroupMap( )
{
  if ( !writer )
  {
    UTILS_MSG( true, "Writer is NULL!!!" );
    return NULL;
  }
  else
  {
    return writer->getActivityGroupMap( );
  }
}

double
AnalysisEngine::getRealTime( uint64_t t )
{
  return (double)t / (double)getTimerResolution( );
}

/**
 * Sort the streams by stream id, but with host streams first.
 */
static bool
streamSort( EventStream* p1, EventStream* p2 )
{
  if ( p1->isDeviceStream( ) && p2->isHostStream( ) )
  {
    return false;
  }

  if ( p2->isDeviceStream( ) && p1->isHostStream( ) )
  {
    return true;
  }

  return p1->getId( ) <= p2->getId( );
}

/**
 * Write OTF2 output trace definitions. Should be called only once per rank.
 * 
 * @param filename
 * @param origFilename
 * @param writeToFile
 * @param ignoreAsyncMpi
 */
void
AnalysisEngine::writeOTF2Definitions( std::string filename,
                                      std::string origFilename,
                                      bool        writeToFile,
                                      bool        ignoreAsyncMpi,
                                      int         verbose)
{
  EventStreamGroup::EventStreamList allStreams;
  
  // \todo why not getLocalStreams( allStreams ) ?
  getStreams( allStreams );

  std::sort( allStreams.begin( ), allStreams.end( ), streamSort );

  writer = NULL;
  if ( strstr( origFilename.c_str( ), ".otf2" ) != NULL )
  {
    // \todo: only the first time
    writer = new OTF2ParallelTraceWriter(
      mpiAnalysis.getMPIRank( ),
      mpiAnalysis.getMPISize( ),
      origFilename.c_str( ),
      writeToFile,
      ignoreAsyncMpi,
      verbose );

    if ( !writeToFile )
    {
      filename = "none.otf2";
    }
  }

  if ( !writer )
  {
    throw RTException( "Could not create trace writer" );
  }

  writer->open( filename.c_str( ), 100, allStreams.size( ) );

  CounterTable::CtrIdSet ctrIdSet = this->ctrTable.getAllCounterIDs( );
  for ( CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin( );
        ctrIter != ctrIdSet.end( ); ++ctrIter )
  {
    CtrTableEntry* entry = this->ctrTable.getCounter( *ctrIter );
    if ( !entry->isInternal )
    {
      writer->writeDefCounter( *ctrIter, entry->name, entry->otfMode );
    }
  }

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          allStreams.begin( );
        pIter != allStreams.end( ); ++pIter )
  {
    EventStream* p = *pIter;

    if ( p->isRemoteStream( ) )
    {
      continue;
    }

    writer->writeDefProcess( p->getId( ), p->getParentId( ), p->getName( ),
                             this->streamTypeToGroup( p->getStreamType( ) ) );
    
    writer->setupEventReader( ( *pIter )->getId( ) );
  }
  
  // clear list that has been created in this function
  allStreams.clear();
}

/**
 * Write process events to OTF2 output.
 * 
 * @param verbose verbose level
 * 
 * @return true, if events are available, otherwise false
 */
bool
AnalysisEngine::writeOTF2EventStreams( int verbose )
{
  assert(writer);
  
  // reset "per interval" values in the trace writer
  writer->reset();
  
  bool events_available = false;
  // \todo: make this a private class member!
  EventStreamGroup::EventStreamList allStreams;
  getStreams( allStreams );

  std::sort( allStreams.begin( ), allStreams.end( ), streamSort );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  uint64_t events_read = 0;
  
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          allStreams.begin( ); pIter != allStreams.end( ); ++pIter )
  {
    EventStream* p = *pIter;

    // write only streams that are local to the process 
    if ( p->isRemoteStream( ) )
    {
      continue;
    }

    // how to interrupt the event reading on e.g. CUDA and OpenMP streams?
    // >> by time? 

    uint64_t events_read_per_stream = 0;
    events_available = writer->writeStream( p, &( this->getCtrTable( ) ), 
      &( this->getGraph( ) ), &events_read_per_stream );
    
    events_read += events_read_per_stream;
  }
  
  if( getMPIRank() == 0 && verbose >= VERBOSE_BASIC ){
    UTILS_MSG( events_available, 
               "[0] Writer interrupted by callback: Read %lu events", 
               events_read );
    
    UTILS_MSG( !events_available, 
               "[0] Writer: Read %lu events", 
               events_read );
  }
  
  // clear list that has been created in this function
  allStreams.clear();
  
  // \todo: delete the graph
  
  return events_available;
}

/**
 * Check for pending non-blocking MPI.
 */
void
AnalysisEngine::checkPendingMPIRequests( )
{
  const EventStreamGroup::EventStreamList& streams = getHostStreams();
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
              streams.begin( ); pIter != streams.end( ); ++pIter )
  {
    if( (*pIter)->havePendingMPIRequests( ) )
    {
      UTILS_MSG( true, "[%u] There are pending MPI requests on stream %d (%s)!", 
                       getMPIRank(), (*pIter)->getId( ), (*pIter)->getName( ) );
    }
  }
}