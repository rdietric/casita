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
  maxMetricClassId( 0 ),
  maxMetricMemberId( 0 ),
  maxAttributeId( 0 ),
  availableParadigms ( 0 )
{
  // add analysis paradigms
  // \todo: only if paradigm found
  // \todo: Where deleted?
  //addAnalysisParadigm( new cuda::AnalysisParadigmCUDA( this ) );
  //addAnalysisParadigm( new opencl::AnalysisParadigmOpenCL( this ) );
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
AnalysisEngine::addDetectedParadigm( Paradigm paradigm )
{
  availableParadigms |= paradigm;
}

bool 
AnalysisEngine::haveParadigm( Paradigm paradigm )
{
  //\todo: check not implemented
  if( paradigm == PARADIGM_MPI )
  {
    return true;
  }
  
  return availableParadigms & paradigm;
}

bool
AnalysisEngine::getFunctionType( uint32_t            id,
                                 const char*         name,
                                 EventStream*        stream,
                                 FunctionDescriptor* descr )
{
  assert( name );
  assert( descr );
  assert( stream );

  return FunctionTable::getAPIFunctionType( name, descr, stream->isDeviceStream( ),
                                            stream->isDeviceNullStream( ) );
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
AnalysisEngine::handleKeyValuesEnter( OTF2TraceReader*     reader,
                                      GraphNode*        node,
                                      OTF2KeyValueList* list )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handleKeyValuesEnter( reader, node, list );
  }
}

void
AnalysisEngine::handleKeyValuesLeave( OTF2TraceReader*     reader,
                                      GraphNode*        node,
                                      GraphNode*        oldNode,
                                      OTF2KeyValueList* list )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin( );
        iter != analysisParadigms.end( ); ++iter )
  {
    iter->second->handleKeyValuesLeave( reader, node, oldNode, list );
  }
}

void
AnalysisEngine::addDeferredNode( GraphNode* node )
{
  deferredNodes.push_back( node );
}

void
AnalysisEngine::processDeferredNodes( Paradigm paradigm )
{
  if( deferredNodes.size() == 0 )
    return;
  
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[%u] Processing %lu deferred nodes", 
             getMPIRank(), deferredNodes.size() );
  
  for ( EventStream::SortedGraphNodeList::const_iterator nIter = deferredNodes.begin( );
        nIter != deferredNodes.end( ); ++nIter )
  {
    applyRules( *nIter, paradigm, false );
  }
  
  // clear the deferred nodes after processing them
  deferredNodes.clear();
}

EventStream*
AnalysisEngine::getNullStream( ) const
{
  return streamGroup.getNullStream( );
}

void
AnalysisEngine::getLastLeaveEvent( EventStream **stream, uint64_t *timestamp )
{
  uint64_t lastLeave = 0;
  
  EventStreamGroup::EventStreamList localStreams;
  getLocalStreams( localStreams );
  
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          localStreams.begin( );
        pIter != localStreams.end( ); ++pIter )
  {
    EventStream* p = *pIter;
    
    // get last leave event
    if ( p->getPeriod().second > lastLeave )
    {
      lastLeave = p->getPeriod().second;
      *stream = p;
    }
  }
  
  *timestamp = lastLeave;
  
  localStreams.clear();
}

/** Find last leave node on given stream before the given timestamp.
 * 
 * @param timestamp
 * @param streamId
 * 
 * @return the graph node
 */
GraphNode*
AnalysisEngine::getLastLeaveNode( uint64_t timestamp, uint64_t streamId ) const
{
  
  EventStream* stream = getStream( streamId );
  if ( !stream )
  {
    return NULL;
  }

  //\todo: Why do we not use our find function and pass a node as input
  EventStream::SortedGraphNodeList& nodes = stream->getNodes( );
  for ( EventStream::SortedGraphNodeList::const_reverse_iterator rIter =
          nodes.rbegin( );
        rIter != nodes.rend( ); ++rIter )
  {
    GraphNode* node = *rIter;
    
    // ignore nodes that are not a leave or MPI
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
AnalysisEngine::createIntermediateBegin( )
{
  // clean all lists in the graph and delete edges, node objects are deleted via the streams
  this->graph.cleanup( true );
  
  //this->reset();

  EventStreamGroup::EventStreamList streams;
  getStreams( streams );
  
  // sort streams by ID with host streams first
  // std::sort( streams.begin( ), streams.end( ), EventStream::streamSort );
  
  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin( );
        iter != streams.end( ); ++iter )
  {
    bool isMpiStream = false;
    EventStream* p   = *iter;
    
    EventStream::SortedGraphNodeList& nodes = p->getNodes( );
    
    //GraphNode* startNode = nodes.front( );
    
    // \todo check for > 1
    if ( nodes.size( ) > 0 )
    {
      //do not remove the last MPI collective leave node
      if( nodes.back()->isMPI() )
      {
        nodes.pop_back( );
        isMpiStream = true;
      }
      
      EventStream::SortedGraphNodeList::const_iterator it = nodes.begin( );
      
      // keep the first node (stream begin node) for MPI processes
      if( p->isHostMasterStream() )
      {
        ++it;
      }
      // do not delete the last node of a device stream, if it is an enter node
      else if( p->isDeviceStream() && nodes.back()->isEnter() )
      {
        UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                   "[%"PRIu64"] Do not delete incomplete kernel %s",
                   p->getId(), getNodeInfo( nodes.back() ).c_str() );
        nodes.pop_back( );
      }
      
      // delete all remaining nodes
      for (; it != nodes.end( ); ++it )
      {
        //UTILS_MSG(true, "[%"PRIu64"] Delete node %s", p->getId(), getNodeInfo(*it).c_str() );
        
        // do not remove CUDA nodes that might be required later
        if( (*it)->isCUDA() )
        {
          // incomplete (only enter exists) and unsynchronized kernels are not deleted
          if( (*it)->isCUDAKernel() )
          {
            if( (*it)->hasPartner() )
            {
              // kernel leave has not yet been synchronized (compare BlameKernelRule)
              if( (*it)->getGraphPair().second->getLink() == NULL )
              {
                UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                           "[%"PRIu64"] Do not delete unsynchronized kernel %s", 
                           p->getId(), getNodeInfo( *it ).c_str() );
                continue;
              }
            }
            /* enter kernel nodes without partner must not be deleted
            else if( (*it)->isEnter() )
            {
              UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                         "[%"PRIu64"] Do not delete incomplete kernel %s", 
                         p->getId(), getNodeInfo( *it ).c_str() );
              continue;
            }*/

            if( ( (*it)->isEnter() && !(*it)->hasPartner() ) || 
                ( (*it)->hasPartner() && (*it)->getGraphPair().second->getLink() == NULL ) )
            {
              
              continue;
            }
          }
          else          
          // if the CUDA kernel launch enter node is not linked with the 
          // associated kernel, the kernel has not started
          if( (*it)->isCUDAKernelLaunch() /*&& (*it)->isEnter() && (*it)->getLink() == NULL*/ )
          {
            //UTILS_MSG(true, "[%"PRIu64"] Do not delete %s", p->getId(), 
            //                getNodeInfo( *it ).c_str() );
            continue;
          }
          
          if( (*it)->isCUDASync() )
          {
            continue;
          }
          
          //continue;
        }
        
        delete( *it );
      }

      //check stream (e.g. pending MPI and other members)
      // \todo
    }
    
    // clean up stream internal data, keep graphData (first and last node)
    p->reset( );

    // create a new global begin node on the MPI synchronization point stream
    if( isMpiStream )
    //if ( p->isHostStream() )
    {
      GraphNode* lastNode = p->getLastNode( );

      // set the stream's last node to type atomic (the collective end node)
      lastNode->setRecordType( RECORD_ATOMIC );
      lastNode->addType( MISC_PROCESS ); // to match node->isProcess())
      
      // clear the nodes vector and reset first and last node of the stream
      p->clearNodes();
      
      // add node to event stream
      //p->addGraphNode( startNode, NULL );
      p->addGraphNode( lastNode, NULL );
      
      // add the stream's start node and previously end node to the empty graph
      //graph.addNode(startNode);
      graph.addNode( lastNode );
      
      // create and add a new edge (with paradigm MPI) between the above added nodes
      //Paradigm paradigm_mpi = PARADIGM_MPI;
      //newEdge( startNode, lastNode, EDGE_NONE, &paradigm_mpi );
      newEdge( globalSourceNode, lastNode ); 
      
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "[%"PRIu64"] Created intermediate start node: %s",
                 p->getId(), getNodeInfo(lastNode).c_str() );
    }
    else
    {
      // clear nodes of device streams
      p->clearNodes();
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
                 "[%"PRIu64"] Cleared nodes list", p->getId() );
    }
  }
  streams.clear();
  
  // reset MPI-related objects
  this->getMPIAnalysis().reset();
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

OTF2ParallelTraceWriter::ActivityGroupMap*
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
 * Get information on a node as char pointer (similar to Node getUniqueName).
 * Includes stream ID, node name, node type, and elapsed time.
 * 
 * @param node
 * 
 * @return node information as char pointer
 */
const std::string
AnalysisEngine::getNodeInfo( Node* node )
{
  std::stringstream sstream;
  
  sstream << node->getUniqueName() << ":" << getRealTime( node->getTime() );

  return sstream.str( );
}

/**
 * Sort the streams by stream id, but with host streams first.
 
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
}*/

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
  
  //\todo why not getLocalStreams( allStreams ) ?
  getStreams( allStreams );

  std::sort( allStreams.begin( ), allStreams.end( ), EventStream::streamSort );

  writer = NULL;
  if ( strstr( origFilename.c_str( ), ".otf2" ) != NULL )
  {
    // \todo: only the first time
    writer = new OTF2ParallelTraceWriter(
      mpiAnalysis.getMPIRank( ),
      mpiAnalysis.getMPISize( ),
      origFilename.c_str( ),
      writeToFile,
      &( this->getCtrTable( ) ),
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

  writer->open( filename.c_str( ), 100 );
  
  if( writeToFile )
  {
    writer->writeAnalysisMetricDefinitions( );
    writer->setupAttributeList( );
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
                             OTF2ParallelTraceWriter::streamTypeToGroup( p->getStreamType( ) ) );
    
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

  // sort streams by ID with host streams first
  std::sort( allStreams.begin( ), allStreams.end( ), EventStream::streamSort );
  
  // first stream should be the MPI rank
  //uint64_t masterPeriodEnd = allStreams.front().getPeriod().second;

  // \todo: needed?
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  uint64_t events_read = 0;
  
  // write all process local streams
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          allStreams.begin( ); pIter != allStreams.end( ); ++pIter )
  {
    EventStream* stream = *pIter;

    // skip remote streams and streams without new nodes
    if ( stream->isRemoteStream( ) || stream->hasNewNodes( ) == false )
    {
      continue;
    }

    uint64_t events_read_per_stream = 0;
    events_available |= writer->writeStream( stream, &( this->getGraph( ) ), 
                                             &events_read_per_stream );
    
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