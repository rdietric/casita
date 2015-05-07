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
  maxFunctionId( 0 ),
  pendingMPICommForWaitAll( 0 ),
  waitStateFuncId( 0 )
{
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

void
AnalysisEngine::saveParallelEventGroupToFile( std::string filename,
                                              std::string origFilename,
                                              bool        writeToFile,
                                              bool        ignoreAsyncMpi,
                                              bool        verbose )
{
  EventStreamGroup::EventStreamList allStreams;
  getStreams( allStreams );

  std::sort( allStreams.begin( ), allStreams.end( ), streamSort );

  writer = NULL;
  if ( strstr( origFilename.c_str( ), ".otf2" ) != NULL )
  {
    writer = new OTF2ParallelTraceWriter(
      mpiAnalysis.getMPIRank( ),
      mpiAnalysis.getMPISize( ),
      origFilename.c_str( ),
      writeToFile,
      ignoreAsyncMpi );

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
  }

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          allStreams.begin( ); pIter != allStreams.end( ); ++pIter )
  {
    EventStream* p = *pIter;

    if ( p->isRemoteStream( ) )
    {
      continue;
    }

    EventStream::SortedGraphNodeList& nodes = p->getNodes( );
    GraphNode* pLastGraphNode = p->getLastNode( );

    writer->writeProcess(
      ( *pIter )->getId( ), &nodes, pLastGraphNode,
      verbose, &( this->getCtrTable( ) ), &( this->getGraph( ) ), ( *pIter )->isHostStream( ) );
  }

}

void
AnalysisEngine::addPendingIRecvRequest( uint64_t request )
{
  pendingIRecvRequest = request;
}

void
AnalysisEngine::addPendingIRecv( GraphNode* node )
{
  requestIRecvMap[pendingIRecvRequest] = node;
}

void
AnalysisEngine::addDataForPendingIRecv( uint64_t request, uint64_t partner )
{
  uint64_t* tmpId = new uint64_t;
  *tmpId = partner;
  requestIRecvMap[request]->setData( tmpId );
}

void
AnalysisEngine::addPendingMPIRequest( MPI_Request* request )
{
  /* std::cout << "[" << mpiAnalysis.getMPIRank() << "] addRequest: " << request << " " << (uint64_t) *request <<
   * std::endl; */
  pendingMPIRequests.push_back( request );
}

MPI_Request*
AnalysisEngine::getPendingMPIRequest( )
{
  /* std::cout << "[" << mpiAnalysis.getMPIRank() << "] getRequest Size: " << pendingMPIRequests.size() <<  std::endl;
   **/
  MPI_Request* temp = pendingMPIRequests.front( );
  pendingMPIRequests.erase( pendingMPIRequests.begin( ) );
  /* std::cout << "[" << mpiAnalysis.getMPIRank() << "] getRequest: " << temp << " " << (uint64_t) *temp << std::endl;
   **/
  return temp;
}

void
AnalysisEngine::addPendingMPICommForWaitAll( )
{
  pendingMPICommForWaitAll++;
}

uint64_t
AnalysisEngine::getNumberOfPendingMPICommForWaitAll( )
{
  uint64_t temp = pendingMPICommForWaitAll;
  pendingMPICommForWaitAll = 0;
  return temp;
}
