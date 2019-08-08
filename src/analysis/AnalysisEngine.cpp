/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2019,
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
#include <ios>

#include "IAnalysisParadigm.hpp"
#include "AnalysisEngine.hpp"
#include "common.hpp"

#include "mpi/AnalysisParadigmMPI.hpp"
#include "omp/AnalysisParadigmOMP.hpp"
#include "offload/AnalysisParadigmOffload.hpp"

#if defined(SCOREP_USER_ENABLE)
#include "scorep/SCOREP_User.h"
#endif

using namespace casita;
using namespace casita::io;

AnalysisEngine::AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize ) :
  mpiAnalysis( mpiRank, mpiSize ),
  maxMetricClassId( 0 ),
  maxMetricMemberId( 0 ),
  maxAttributeId( 0 ),
  availableParadigms ( 0 ),
  analysisFeature( 0 )
{
  // add analysis paradigms
  // \todo: Where deleted?
  
  if( mpiSize > 1 )
  {
    addAnalysis( new mpi::AnalysisParadigmMPI( this, mpiRank, mpiSize ) );
  }
  else
  {
    Parser::getInstance().getProgramOptions().ignoreAsyncMpi = true;
  }
}

AnalysisEngine::~AnalysisEngine()
{
  for ( AnalysisParadigmsMap::const_iterator iter = analysisParadigms.begin();
        iter != analysisParadigms.end(); ++iter )
  {
    delete iter->second;
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

void
AnalysisEngine::setDefinitionHandler( OTF2DefinitionHandler* defHandler )
{
  this->defHandler = defHandler;
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



void
AnalysisEngine::addFilteredRegion( uint32_t regionId )
{
  filteredFunctions.insert( regionId );
}

bool
AnalysisEngine::isRegionFiltered( uint32_t funcId )
{
  return ( filteredFunctions.count( funcId ) > 0 );
}

void
AnalysisEngine::applyRules( GraphNode* node )
{
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION_DEFINE( apply_offload_rules_handle )
  SCOREP_USER_REGION_DEFINE( apply_mpi_rules_handle )
  SCOREP_USER_REGION_DEFINE( apply_omp_rules_handle )
#endif
    
  Paradigm paradigm;
    
  // handle PARADIGM_CUDA and PARADIGM_OCL nodes with offload analysis paradigm
  if ( node->getParadigm() & PARADIGM_OFFLOAD )
  {
    paradigm = PARADIGM_OFFLOAD;
  }
  else
  {
    paradigm = node->getParadigm();
  }
  
  AnalysisParadigmsMap::const_iterator iter = analysisParadigms.find( paradigm );
  if ( iter != analysisParadigms.end() )
  {
#if defined(SCOREP_USER_ENABLE)
    if ( paradigm == PARADIGM_OFFLOAD )
    {
      SCOREP_USER_REGION_BEGIN( apply_offload_rules_handle, "apply_offload_rules",
                                SCOREP_USER_REGION_TYPE_PHASE )
    }
    else if(node->getParadigm() & PARADIGM_MPI)
    {
      SCOREP_USER_REGION_BEGIN( apply_mpi_rules_handle, "apply_mpi_rules",
                                SCOREP_USER_REGION_TYPE_PHASE )
    }
    else if(node->getParadigm() & PARADIGM_OMP)
    {
      SCOREP_USER_REGION_BEGIN( apply_omp_rules_handle, "apply_omp_rules",
                                SCOREP_USER_REGION_TYPE_PHASE )
    }
#endif
    
    iter->second->applyRules( node );
    
#if defined(SCOREP_USER_ENABLE)
    if ( paradigm == PARADIGM_OFFLOAD )
    {
      SCOREP_USER_REGION_END( apply_offload_rules_handle )
    }
    else if(node->getParadigm() & PARADIGM_MPI)
    {
      SCOREP_USER_REGION_END( apply_mpi_rules_handle )
    }
    else if(node->getParadigm() & PARADIGM_OMP)
    {
      SCOREP_USER_REGION_END( apply_omp_rules_handle )
    }
#endif
  }
  

}

void
AnalysisEngine::addAnalysis( IAnalysisParadigm* paradigm )
{
  assert( paradigm );
  analysisParadigms[ paradigm->getParadigm() ] = paradigm;
}

IAnalysisParadigm*
AnalysisEngine::getAnalysis( Paradigm paradigm )
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

/**
 * Apply analysis rules to all nodes.
 */
void
AnalysisEngine::runAnalysis()
{
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION( "runAnalysis", SCOREP_USER_REGION_TYPE_FUNCTION )
#endif
    
  EventStream::SortedGraphNodeList allNodes;
  getAllNodes( allNodes );
  
  bool printStatus = mpiAnalysis.getMPIRank() == 0  
                  && Parser::getVerboseLevel() >= VERBOSE_BASIC 
                  && !Parser::getOptions().analysisInterval;
  
  size_t ctr       = 0, last_ctr = 0;
  size_t num_nodes = allNodes.size();

  // apply paradigm specific rules
  for ( EventStream::SortedGraphNodeList::const_iterator nIter = allNodes.begin();
        nIter != allNodes.end(); ++nIter )
  {
    GraphNode* node = *nIter;
    ctr++;

    applyRules( node );

    // print process every 5 percent (TODO: depending on number of events per paradigm)
    if ( printStatus && ( ctr - last_ctr > num_nodes / 20 ) )
    {
      UTILS_OUT( "[0] %lu%% ", (size_t)( 100.0 * (double)ctr / (double)num_nodes ) );
      fflush( NULL );
      last_ctr = ctr;
    }
  }

  UTILS_MSG( printStatus, "[0] 100%%" );
  
  // apply rules on pending nodes
  //analysis.processDeferredNodes( paradigm );

//#ifdef DEBUG
//  clock_t time_sanity_check = clock();
//  
//  runSanityCheck( mpiAnalysis.getMPIRank() );
//  
//  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_TIME && 
//             mpiAnalysis.getMPIRank() == 0 && 
//             !Parser::getOptions().analysisInterval,
//             "[0] Sanity check: %f sec", 
//             ( (float) ( clock() - time_sanity_check ) ) / CLOCKS_PER_SEC );
//#endif
  
  allNodes.clear();
}

void
AnalysisEngine::clearNodes()
{
  allNodes.clear();
}

void
AnalysisEngine::handlePostEnter( GraphNode* node )
{
  if( node->isOfld() && analysisParadigms.count( PARADIGM_OFFLOAD ) > 0 )
  {
    analysisParadigms[ PARADIGM_OFFLOAD ]->handlePostEnter( node );
    return;
  }
  
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( node->getParadigm() );
  if ( iter != analysisParadigms.end() )
  {
    iter->second->handlePostEnter( node );
  }
}

void
AnalysisEngine::handlePostLeave( GraphNode* node )
{
  if( node->isOfld() && analysisParadigms.count( PARADIGM_OFFLOAD ) > 0 )
  {
    analysisParadigms[ PARADIGM_OFFLOAD ]->handlePostLeave( node );
    return;
  }
  
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( node->getParadigm() );
  if ( iter != analysisParadigms.end() )
  {
    iter->second->handlePostLeave( node );
  }
}

void
AnalysisEngine::handleKeyValuesEnter( OTF2TraceReader*  reader,
                                      GraphNode*        node,
                                      OTF2KeyValueList* list )
{
  if( node->isOfld() && analysisParadigms.count( PARADIGM_OFFLOAD ) > 0 )
  {
    analysisParadigms[ PARADIGM_OFFLOAD ]->handleKeyValuesEnter( 
      reader, node, list );
    return;
  }
  
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( node->getParadigm() );
  if ( iter != analysisParadigms.end() )
  {
    iter->second->handleKeyValuesEnter( reader, node, list );
  }
}

void
AnalysisEngine::handleKeyValuesLeave( OTF2TraceReader*  reader,
                                      GraphNode*        node,
                                      GraphNode*        oldNode,
                                      OTF2KeyValueList* list )
{
  if( node->isOfld() && analysisParadigms.count( PARADIGM_OFFLOAD ) > 0 )
  {
    analysisParadigms[ PARADIGM_OFFLOAD ]->handleKeyValuesLeave( 
      reader, node, oldNode, list );
    return;
  }
  
  AnalysisParadigmsMap::iterator iter = analysisParadigms.find( node->getParadigm() );
  if ( iter != analysisParadigms.end() )
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
AnalysisEngine::processDeferredNodes()
{
  if( deferredNodes.size() == 0 )
    return;
  
  UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "[%u] Processing %lu deferred nodes", 
             getMPIRank(), deferredNodes.size() );
  
  for ( EventStream::SortedGraphNodeList::const_iterator nIter = 
          deferredNodes.begin(); nIter != deferredNodes.end(); ++nIter )
  {
    applyRules( *nIter );
  }
  
  // clear the deferred nodes after processing them
  deferredNodes.clear();
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
                              const char*       name,
                              Paradigm          paradigm,
                              RecordType        recordType,
                              int               nodeType )
{
  GraphNode* node = GraphEngine::newGraphNode( time, streamId, name,
                                               paradigm, recordType, nodeType );

  if ( node->isWaitstate() )
  {
    node->setFunctionId( defHandler->getWaitStateRegionId() );
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
    node->setFunctionId( defHandler->getWaitStateRegionId() );
  }

  return node;
}

void 
AnalysisEngine::createIntermediateBegin()
{
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION( "createIntermediateBegin", SCOREP_USER_REGION_TYPE_FUNCTION )
#endif
    
  // clean all lists in the graph and delete edges, 
  // node objects are deleted via the streams
  graph.cleanup( true );
  
  // reset MPI-related objects (before deleting nodes!)
  this->getMPIAnalysis().reset();
  
  // reset several structures in other analysis paradigms
  this->reset();

  const EventStreamGroup::EventStreamList streams = getStreams();
  
  offload::AnalysisParadigmOffload* ofldAnalysis = NULL;
  if( haveParadigm( PARADIGM_OFFLOAD ) )
  {
    ofldAnalysis = 
      (offload::AnalysisParadigmOffload*)this->getAnalysis( PARADIGM_OFFLOAD );
  }
  
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
      if( p->isMpiStream() )
      {
        ++it;
      }
      else if( p->isDeviceStream() )
      {
        // do not delete the last node of a device stream, if it is an enter node
        if( nodes.back()->isEnter() )
        {
          UTILS_MSG_ONCE_OR( Parser::getVerboseLevel() > VERBOSE_BASIC,
            "[%" PRIu64 "] Found incomplete kernel %s at intermediate analysis start.", 
            p->getId(), getNodeInfo( nodes.back() ).c_str())
          
          // reset left link to avoid a segmentation fault when creating kernel
          // dependencies
          nodes.back()->setLinkLeft( NULL );
          nodes.pop_back();
        }
        
        // check for pending kernels
        if( ( ( DeviceStream* ) p )->getLastPendingKernel() )
        {
          UTILS_MSG_ONCE_OR( Parser::getVerboseLevel() > VERBOSE_BASIC, 
            "Stream %" PRIu64 " (%s) has pending kernels at intermediate analysis start.", 
            p->getId(), p->getName() );
        }
        else
        {
          // we do not have pending kernels, 
          // hence kernels and kernel launches can be removed
          havePendingKernels = false;
          
          if( ofldAnalysis )
          {
            ofldAnalysis->clearKernelEnqueues( p->getId() );
          }
        }
      }
      
      // delete all remaining nodes
      for (; it != nodes.end(); ++it )
      {
        GraphNode* node = *it;
        
        ////////////////////// Offload /////////////////////
        // do not remove offload nodes that might be required later
        if( node->isOfld() )
        {
          if( havePendingKernels )
          {
            // incomplete (only enter exists) and unsynchronized kernels are not deleted
            if( node->isOffloadKernel() )
            {
              // do not delete kernels that have not yet been synchronized
              if( ofldAnalysis->isKernelPending( node ) )
              {
                node->setLinkLeft( NULL ); // only needed for kernel enter
                continue;
              }
              else
              {
                // delete kernel launch leave nodes in kernel launch map
                if( node->isEnter() )
                {
                  ofldAnalysis->removeKernelLaunch( node );
                }
                // kernel launch enter nodes are consumed from kernel launch map at kernel enter
              }
            }
            else          
            // if the CUDA kernel launch enter node is not linked with the 
            // associated kernel, the kernel has not started
            if( node->isOffloadEnqueueKernel() /*&& (*it)->isEnter() && (*it)->getLink() == NULL*/ )
            {
              //UTILS_MSG(true, "[%"PRIu64"] Do not delete %s", p->getId(), 
              //                getNodeInfo( *it ).c_str() );
              continue;
            }
          }
          
          //else
          // if CUDA event record node has not yet been synchronized
          if( node->isCUDAEventLaunch() && node->isLeave() && node->getData() == NULL )
          {
            continue;
          }
          
          // do not delete Offload synchronization nodes
          // \todo: why not?
          if( node->isOffloadWait() )
          {
            continue;
          }
        }
        ////////////////////// End: Offload /////////////////////
        
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
      p->addGraphNode( lastNode, NULL );
      //addNewGraphNodeInternal( lastNode, p );
      
      // add the stream's start node and previously end node to the empty graph
      //graph.addNode(startNode);
      graph.addNode( lastNode );
      
      // create and add a new edge (with paradigm MPI) between the above added nodes
      newEdge( globalSourceNode, lastNode ); 
      
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "[%" PRIu64 "] Created intermediate start node: %s",
                 p->getId(), getNodeInfo(lastNode).c_str() );
    }
    else
    {
      // clear nodes of device streams
      p->clearNodes();
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_SOME, 
                 "[%" PRIu64 "] Cleared nodes list", p->getId() );
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
 * Check for pending non-blocking MPI.
 */
void
AnalysisEngine::checkPendingMPIRequests()
{
  const EventStreamGroup::EventStreamList& streams = getHostStreams();
  for ( EventStreamGroup::EventStreamList::const_iterator pIter =
              streams.begin(); pIter != streams.end(); ++pIter )
  {
    EventStream* stream = *pIter;

    if( stream->isMpiStream() )
    {
      size_t pendingRequests = 
        ( ( MpiStream* ) stream )->havePendingMPIRequests();

      if( pendingRequests > 0 )
      {
        UTILS_WARN_ONCE( "[%" PRIu32 "] There are %lu pending MPI requests on "
                         "stream %" PRIu64 " (%s)!", getMPIRank(), 
                         pendingRequests, stream->getId(), stream->getName() );
      }
    }
  }
}

Statistics&
AnalysisEngine::getStatistics()
{
  return statistics;
}
