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
#include "cuda/AnalysisParadigmCUDA.hpp"

#include "otf/OTF2ParallelTraceWriter.hpp"

using namespace casita;
using namespace casita::io;

AnalysisEngine::AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize ) :
  mpiAnalysis( mpiRank, mpiSize ),
  writer( NULL ),
  maxFunctionId( 0 ),
  waitStateFuncId( 0 ),
  maxMetricClassId( 0 ),
  maxMetricMemberId( 0 ),
  maxAttributeId( 0 ),
  availableParadigms ( 0 ),
  analysisFeature( 0 )
{
  // add analysis paradigms
  // \todo: Where deleted?
  //addAnalysisParadigm( new omp::AnalysisParadigmOMP( this ) );
  addAnalysisParadigm( new mpi::AnalysisParadigmMPI( this, mpiRank, mpiSize ) );
}

AnalysisEngine::~AnalysisEngine()
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
  {
    delete iter->second;
  }
  if ( writer != NULL )
  {
    writer->close();
    delete writer;
  }
}

uint32_t
AnalysisEngine::getMPIRank()
{
  return mpiAnalysis.getMPIRank();
}

uint32_t
AnalysisEngine::getMPISize()
{
  return mpiAnalysis.getMPISize();
}

MPIAnalysis&
AnalysisEngine::getMPIAnalysis()
{
  return mpiAnalysis;
}

//\todo: not implemented for MPI
void 
AnalysisEngine::addDetectedParadigm( Paradigm paradigm )
{
  availableParadigms |= paradigm;
}

bool 
AnalysisEngine::haveParadigm( Paradigm paradigm ) const
{
  //\todo: check not implemented
  if( paradigm == PARADIGM_MPI )
  {
    return true;
  }
  
  return availableParadigms & paradigm;
}

void
AnalysisEngine::addAnalysisFeature( AnalysisFeature feature )
{
  analysisFeature |= feature;
}

bool 
AnalysisEngine::haveAnalysisFeature( AnalysisFeature feature ) const
{
  return analysisFeature & feature;
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

  return FunctionTable::getAPIFunctionType( name, descr, stream->isDeviceStream(),
                                            stream->isDeviceNullStream() );
}

void
AnalysisEngine::addFunction( uint32_t funcId, const char* name )
{
  maxFunctionId       = std::max( maxFunctionId, funcId );
  functionMap[funcId] = name;
}

uint32_t
AnalysisEngine::getNewFunctionId()
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
  if ( iter != functionMap.end() )
  {
    return iter->second.c_str();
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
  if ( iter == analysisParadigms.end() )
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
    analysisParadigms[ paradigm->getParadigm() ] = paradigm;
  }
}

IAnalysisParadigm*
AnalysisEngine::getAnalysisParadigm( Paradigm paradigm )
{
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( paradigm );
  if ( iter == analysisParadigms.end() )
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
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
  {
    iter->second->handlePostEnter( node );
  }
}

void
AnalysisEngine::handlePostLeave( GraphNode* node )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
  {
    iter->second->handlePostLeave( node );
  }
}

void
AnalysisEngine::handleKeyValuesEnter( OTF2TraceReader*     reader,
                                      GraphNode*        node,
                                      OTF2KeyValueList* list )
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
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
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
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
  
  for ( EventStream::SortedGraphNodeList::const_iterator nIter = 
          deferredNodes.begin(); nIter != deferredNodes.end(); ++nIter )
  {
    applyRules( *nIter, paradigm, false );
  }
  
  // clear the deferred nodes after processing them
  deferredNodes.clear();
}

EventStream*
AnalysisEngine::getNullStream() const
{
  return streamGroup.getNullStream();
}

void
AnalysisEngine::getLastLeaveEvent( EventStream **stream, uint64_t *timestamp )
{
  uint64_t lastLeave = 0;
  
  const EventStreamGroup::EventStreamList allStreams = getStreams();
  
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
          allStreams.begin();
        pIter != allStreams.end(); ++pIter )
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
  EventStream::SortedGraphNodeList& nodes = stream->getNodes();
  for ( EventStream::SortedGraphNodeList::const_reverse_iterator rIter =
          nodes.rbegin();
        rIter != nodes.rend(); ++rIter )
  {
    GraphNode* node = *rIter;
    
    // ignore nodes that are not a leave or MPI
    if ( !node->isLeave() || node->isMPI() )
    {
      continue;
    }

    if ( node->getTime() <= timestamp )
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
                              RecordType        recordType,
                              int               nodeType )
{
  GraphNode* node = GraphEngine::newGraphNode( time, streamId, name,
                                               paradigm, recordType, nodeType );

  if ( node->isWaitstate() )
  {
    node->setFunctionId( waitStateFuncId );
  }

  return node;
}

GraphNode*
AnalysisEngine::addNewGraphNode( uint64_t            time,
                                 EventStream*        stream,
                                 const char*         name,
                                 FunctionDescriptor* funcDesc )
{
  GraphNode* node = GraphEngine::addNewGraphNode( time, stream, name, 
                                                  funcDesc->paradigm,
                                                  funcDesc->recordType,
                                                  funcDesc->functionType );

  if ( node->isWaitstate() )
  {
    node->setFunctionId( waitStateFuncId );
  }

  return node;
}

void 
AnalysisEngine::createIntermediateBegin()
{
  // clean all lists in the graph and delete edges, 
  // node objects are deleted via the streams
  graph.cleanup( true );
  
  // reset MPI-related objects (before deleting nodes!)
  this->getMPIAnalysis().reset();
  
  // reset several structures in other analysis paradigms
  this->reset();

  const EventStreamGroup::EventStreamList streams = getStreams();
  
  cuda::AnalysisParadigmCUDA* cudaAnalysis = 
    (cuda::AnalysisParadigmCUDA*)this->getAnalysisParadigm( PARADIGM_CUDA );
  
  //cudaAnalysis->printKernelLaunchMap();
  
  for ( EventStreamGroup::EventStreamList::const_iterator iter = streams.begin();
        iter != streams.end(); ++iter )
  {
    bool isMpiStream = false;
    EventStream* p   = *iter;
    
    EventStream::SortedGraphNodeList& nodes = p->getNodes();
    
    // \todo check for > 1
    if ( nodes.size() > 0 )
    {
      //do not remove the last MPI collective leave node
      if( nodes.back()->isMPI() )
      {
        nodes.pop_back();
        isMpiStream = true;
      }
      
      EventStream::SortedGraphNodeList::const_iterator it = nodes.begin();
      
      // used to avoid individual CUDA kernel node checks
      bool havePendingKernels = true; 
      
      // keep the first node (stream begin node) for MPI processes
      if( p->isHostMasterStream() )
      {
        ++it;
      }
      else if( p->isDeviceStream() )
      {
        // do not delete the last node of a device stream, if it is an enter node
        if( nodes.back()->isEnter() )
        {
          UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                     "[%"PRIu64"] Do not delete incomplete kernel %s",
                     p->getId(), getNodeInfo( nodes.back() ).c_str() );
          nodes.pop_back();
        }
        
        // check for pending kernels
        if( p->getLastPendingKernel() )
        {
          UTILS_MSG( true, "[%"PRIu64"] There are pending kernels!" );
        }
        else
        {
          // we do not have pending kernels, 
          // hence kernels and kernel launches can be removed
          havePendingKernels = false;
          
          // clear pending kernel launches for this stream
          cudaAnalysis->clearKernelLaunches( p->getId() );
        }
      }
      
      // delete all remaining nodes
      for (; it != nodes.end(); ++it )
      {
        // do not remove CUDA nodes that might be required later
        if( (*it)->isCUDA() )
        {
          if( havePendingKernels )
          {
            // incomplete (only enter exists) and unsynchronized kernels are not deleted
            if( (*it)->isCUDAKernel() )
            {
              // do not delete kernels that have not yet been synchronized
              if( cudaAnalysis->isKernelPending( *it ) )
              {
                continue;
              }
              else
              {
                // delete kernel launch leave nodes in kernel launch map
                if( (*it)->isEnter() )
                {
                  cudaAnalysis->removeKernelLaunch( *it );
                }
                // kernel launch enter nodes are consumed from kernel launch map at kernel enter
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
          }
          
          //else
          // if CUDA event record node has not yet been synchronized
          if( (*it)->isCUDAEventLaunch() && (*it)->isLeave() && (*it)->getData() == NULL )
          {
            continue;
          }
          
          // do not delete CUDA synchronization nodes
          // \todo: why not?
          if( (*it)->isCUDASync() )
          {
            continue;
          }
          
          //continue;
        }
        
        //UTILS_MSG( true , 
        //  "[%"PRIu64"] Delete node %s", p->getId(), getNodeInfo(*it).c_str() );
        
        delete( *it );
      }

      //check stream (e.g. pending MPI and other members)
      // \todo
    }
    
    // clean up stream internal data, keep graphData (first and last node)
    p->reset();

    // create a new global begin node on the MPI synchronization point stream
    if( isMpiStream )
    //if ( p->isHostStream() )
    {
      GraphNode* lastNode = p->getLastNode();

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
}

void
AnalysisEngine::reset()
{
  //GraphEngine::reset();
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
  {
    iter->second->reset();
  }
}

size_t
AnalysisEngine::getNumAllDeviceStreams()
{
  return streamGroup.getNumStreams() - streamGroup.getNumHostStreams();
}

OTF2ParallelTraceWriter::ActivityGroupMap*
AnalysisEngine::getActivityGroupMap()
{
  if ( !writer )
  {
    UTILS_MSG( true, "Writer is NULL!!!" );
    return NULL;
  }
  else
  {
    return writer->getActivityGroupMap();
  }
}

double
AnalysisEngine::getRealTime( uint64_t t )
{
  return (double)t / (double)getTimerResolution();
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

  return sstream.str();
}

/**
 * Sort the streams by stream id, but with host streams first.
 
static bool
streamSort( EventStream* p1, EventStream* p2 )
{
  if ( p1->isDeviceStream() && p2->isHostStream() )
  {
    return false;
  }

  if ( p2->isDeviceStream() && p1->isHostStream() )
  {
    return true;
  }

  return p1->getId() <= p2->getId();
}*/

/**
 * Write OTF2 output trace definitions. Should be called only once per rank.
 * 
 * @param origFilename
 * @param writeToFile
 */
void
AnalysisEngine::writeOTF2Definitions( const bool writeToFile )
{
  writer = new OTF2ParallelTraceWriter(
    mpiAnalysis.getMPIRank(),
    mpiAnalysis.getMPISize(),
    writeToFile,
    &( this->getCtrTable() ) );

  writer->open();
  
  if( writeToFile )
  {
    writer->writeAnalysisMetricDefinitions();
  }

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  writer->setupGlobalEvtReader();
}

/**
 * Write process events to OTF2 output.
 * 
 * @return number of events read by event writer
 */
uint64_t 
AnalysisEngine::writeOTF2EventStreams( const uint64_t eventsToRead )
{
  assert( writer );
  
  // reset "per interval" values in the trace writer
  writer->reset();

  // \todo: needed?
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  
  // write all process local streams
  // \todo: make this a private class member!
  const EventStreamGroup::EventStreamList allStreams = getStreams();
  
  uint64_t events_read = 
    writer->writeLocations( allStreams, &( this->getGraph() ), eventsToRead );
  
  return events_read;
}

/**
 * Check for pending non-blocking MPI.
 */
void
AnalysisEngine::checkPendingMPIRequests()
{
  const EventStreamGroup::EventStreamList& streams = getHostStreams();
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
              streams.begin(); pIter != streams.end(); ++pIter )
  {
    if( (*pIter)->havePendingMPIRequests() )
    {
      UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_NONE, 
                 "[%"PRIu32"] There are pending MPI requests on stream %"PRIu64" (%s)!", 
                 getMPIRank(), (*pIter)->getId(), (*pIter)->getName() );
    }
  }
}

Statistics&
AnalysisEngine::getStatistics()
{
  return statistics;
}