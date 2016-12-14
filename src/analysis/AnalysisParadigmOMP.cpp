/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014-2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#include "omp/AnalysisParadigmOMP.hpp"

#include "omp/OMPForkJoinRule.hpp"
#include "omp/OMPComputeRule.hpp"
#include "omp/OMPBarrierRule.hpp"
#include "omp/OMPTComputeRule.hpp"
#include "omp/OMPTargetRule.hpp"
#include "omp/OMPTargetBarrierRule.hpp"

using namespace casita;
using namespace casita::omp;
using namespace casita::io;

AnalysisParadigmOMP::AnalysisParadigmOMP( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine )
{
  
  // use different rules for OMPT and OPARI2 instrumentation
  if( analysisEngine->haveParadigm( PARADIGM_OMPT ) )
  {
    addRule( new OMPTComputeRule( 1 ) );
  }
  else
  {
    addRule( new OMPForkJoinRule( 1 ) );
    addRule( new OMPComputeRule( 1 ) );
    addRule( new OMPBarrierRule( 1 ) );
  }
  
  // add OpenMP target rules only, if a MIC device stream is available
  if( analysisEngine->haveParadigm( PARADIGM_OMP_TARGET ) )
  {
    addRule( new OMPTargetRule( 1 ) );
    addRule( new OMPTargetBarrierRule( 1 ) );
  }
}

AnalysisParadigmOMP::~AnalysisParadigmOMP( ){ }

Paradigm
AnalysisParadigmOMP::getParadigm( )
{
  return PARADIGM_OMP;
}

void
AnalysisParadigmOMP::handlePostLeave( GraphNode* node )
{
  if ( node->isOMPForkJoinRegion() &&
       ( commonAnalysis->getStream( node->getStreamId() )->getStreamType()
         == EventStream::ES_DEVICE ) )
  {
    popOmpTargetRegion( node );
  }

  if ( node->isOMPSync() )
  {
    Edge *edge = commonAnalysis->getEdge( node->getPartner( ), node );
    
    // mark this barrier if it has callees 
    if ( !edge )
    {
      node->setCounter( OMP_IGNORE_BARRIER, 1 );
      //UTILS_WARNING( "Ignore barrier for %s", node->getUniqueName().c_str());
    }
    
    // if a sync operation is nested into another sync 
    // (e.g. wait_barrier inside of barrier)
    if( node->getCaller() && node->getCaller()->isOMPSync() )
    {
      if( edge && edge->isBlocking() )
      {
        edge->unblock();
        UTILS_WARNING( "Unblock edge %s", edge->getName().c_str() );
      }
    }
  }
  
  if( commonAnalysis->haveParadigm( PARADIGM_OMPT ) )
  {
    // node has no caller AND is not parallel (e.g. implicit task)
    if( !node->getCaller() && !node->isOMPParallel() )
    {
      UTILS_WARNING( "No caller for %s", node->getUniqueName().c_str());

      // parallel leave event has not been read yet!
      // therefore, edge cannot been created yet.
    }
  }
}

/**
 * TODO: parts could be moved to analysis (rules)
 * 
 * @param reader
 * @param node
 * @param list
 */
void
AnalysisParadigmOMP::handleKeyValuesEnter( OTF2TraceReader*  reader,
                                           GraphNode*        node,
                                           OTF2KeyValueList* list )
{
  // if the trace has been generated with OMPT instrumentation
  if( commonAnalysis->haveParadigm( PARADIGM_OMPT ) )
  {
    int32_t streamRefKey = -1;
    uint64_t parallel_id = 0;

    // get the parallel region ID, 
    streamRefKey = reader->getFirstKey( SCOREP_OMPT_PARALLEL_ID );
    if ( streamRefKey > -1 && list && list->getSize() > 0 &&
         list->getUInt64( (uint32_t)streamRefKey, &parallel_id ) == 
                                                  OTF2KeyValueList::KV_SUCCESS )
    {
      // if parallel region enter event
      if( node->isOMPParallel() )
      {
        if( ompParallelIdNodeMap.count( parallel_id ) > 0 )
        {
          UTILS_WARNING( "Overwriting OMPT parallel region ID for %s!",
                         commonAnalysis->getNodeInfo( node ).c_str() );
        }
        
        // store parallel region ID
        ompParallelIdNodeMap[ parallel_id ] = node;
      }
      // if node has no caller (first event on a stream), but no parallel
      else if ( !node->getCaller() )
      {
        // create dependency edge to associated parallel region begin event
        if( ompParallelIdNodeMap.count( parallel_id ) > 0 )
        {
          // add edge for critical path analysis
          commonAnalysis->newEdge( ompParallelIdNodeMap[ parallel_id ], node );
          
          // add link to store dependency for leave event
          node->setLink( ompParallelIdNodeMap[ parallel_id ] );
        }
        else
        {
          UTILS_WARNING( "OMPT parallel region ID  for %s not available!",
                         commonAnalysis->getNodeInfo( node ).c_str() );
        }
      }
    }
  }

  // this is only for offloaded regions
  if ( commonAnalysis->getStream( node->getStreamId( ) )->getStreamType( )
       == EventStream::ES_DEVICE )
  {
    int32_t streamRefKey = -1;
    uint64_t key_value = 0;

    // parent region id
    streamRefKey = reader->getFirstKey( SCOREP_OMP_TARGET_PARENT_REGION_ID );
    if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
         list->getUInt64( (uint32_t)streamRefKey, &key_value ) == 
                                                  OTF2KeyValueList::KV_SUCCESS )
    {
      // only create intra-device dependency edges for first event on each stream
      if ( !node->getCaller( ) )
      {
        GraphNode* parentNode = findOmpTargetParentRegion( node, key_value );
        if ( parentNode )
        {
          commonAnalysis->newEdge( parentNode, node, EDGE_NONE );
        }
      }

      if ( node->isOMPSync( ) )
      {
        node->setCounter( OMP_PARENT_REGION_ID, key_value );
      }
    }

    /* region id */
    streamRefKey = reader->getFirstKey( SCOREP_OMP_TARGET_REGION_ID );
    if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
         list->getUInt64( (uint32_t)streamRefKey,
                          &key_value ) == OTF2KeyValueList::KV_SUCCESS )
    {
      pushOmpTargetRegion( node, key_value );

      if ( node->isOMPSync( ) )
      {
        node->setCounter( OMP_REGION_ID, key_value );
      }
    }
  }
}

void
AnalysisParadigmOMP::handleKeyValuesLeave( OTF2TraceReader*     reader,
                                           GraphNode*        node,
                                           GraphNode*        oldNode,
                                           OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_OMP_TARGET_LOCATIONREF );

  if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
  }
}

/**
 * Get the innermost fork-join node (top node on the stack).
 * 
 * @return the innermost fork-join node or NULL if stack is empty.
 */
GraphNode*
AnalysisParadigmOMP::getInnerMostFork( )
{
  if( forkJoinStack.empty() )
  {
    return NULL;
  }
  
  return forkJoinStack.top();
}

/**
 * Push fork operation (parallel begin) to the fork-join stack.
 * 
 * @param forkJoinNode fork-join node
 */
void
AnalysisParadigmOMP::pushFork( GraphNode* node )
{
  forkJoinStack.push( node );
}

/**
 * Take the innermost fork-join node from stack.
 * 
 * @param forkJoinNode innermost fork-join node
 * 
 * @return the innermost fork-join node or NULL if stack is empty.
 */
GraphNode*
AnalysisParadigmOMP::popFork( )
{
  if( forkJoinStack.empty() )
  {
    return NULL;
  }
  
  GraphNode* node = forkJoinStack.top();
  
  forkJoinStack.pop();
  
  return node;
}

/**
 * Get the last active OpenMP compute node on the given stream.
 * 
 * @param streamId stream ID
 * 
 * @return last active OpenMP compute node on the given stream
 */
GraphNode*
AnalysisParadigmOMP::getOmpCompute( uint64_t streamId )
{
  return ompComputeTrackMap[streamId];
}

/**
 * Set the last active OpenMP compute node for the given stream.
 * 
 * @param node graph node
 * @param streamId stream ID
 */
void
AnalysisParadigmOMP::setOmpCompute( GraphNode* node, uint64_t streamId )
{
  ompComputeTrackMap[streamId] = node;
}

const GraphNode::GraphNodeList&
AnalysisParadigmOMP::getBarrierEventList( bool device, GraphNode* caller, int matchingId )
{
  if ( device )
  {
    return ompBarrierListDevice[std::make_pair( 0, matchingId )];
  }
  else
  {
    return ompBarrierListHost;
  }
}

void
AnalysisParadigmOMP::addBarrierEventToList( GraphNode* node,
                                            bool       device,
                                            int        matchingId )
{
  GraphNode* leaveNode = node;
  if ( node->isEnter( ) )
  {
    leaveNode = node->getPartner( );
  }

  // only add barrier activities that have no callees
  if ( leaveNode->getCounter( OMP_IGNORE_BARRIER, NULL ) )
  {
    return;
  }

  if ( device )
  {
    ompBarrierListDevice[std::make_pair( 0, matchingId )].push_back( node );
  }
  else
  {
    ompBarrierListHost.push_back( node );
  }
}

void
AnalysisParadigmOMP::clearBarrierEventList( bool device, GraphNode* caller, int matchingId )
{
  if ( device )
  {
    ompBarrierListDevice[std::make_pair( 0, matchingId )].clear( );
  }
  else
  {
    ompBarrierListHost.clear( );
  }
}

/**
 * Set the OpenMP target begin node for the node's stream. 
 * 
 * @param node OpenMP target begin node
 */
void
AnalysisParadigmOMP::setOmpTargetBegin( GraphNode* node )
{
  if ( ompTargetRegionBeginMap.find( node->getStreamId( ) ) !=
       ompTargetRegionBeginMap.end( ) )
  {
    ErrorUtils::getInstance( ).outputMessage(
      "[OpenMP Offloading]: Nested target regions detected. Replacing target begin with %s",
      node->getUniqueName( ).c_str( ) );
  }

  ompTargetRegionBeginMap[node->getStreamId( )] = node;
}

/**
 * Consume the OpenMP target begin node of the given streamId.
 * 
 * @param streamId stream ID where the target begin node shall be consumed
 */
GraphNode*
AnalysisParadigmOMP::consumeOmpTargetBegin( uint64_t streamId )
{
  OmpEventMap::iterator iter = ompTargetRegionBeginMap.find( streamId );
  if ( iter == ompTargetRegionBeginMap.end( ) )
  {
    return NULL;
  }
  else
  {
    GraphNode* node = iter->second;
    ompTargetRegionBeginMap.erase( iter );
    return node;
  }
}

void
AnalysisParadigmOMP::setOmpTargetFirstEvent( GraphNode* node )
{
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId( ) ) ==
       ompTargetDeviceFirstEventMap.end( ) )
  {
    ompTargetDeviceFirstEventMap[node->getStreamId( )] = node;
  }
}

GraphNode*
AnalysisParadigmOMP::consumeOmpTargetFirstEvent( uint64_t streamId )
{
  OmpEventMap::iterator iter = ompTargetDeviceFirstEventMap.find(
    streamId );
  if ( iter == ompTargetDeviceFirstEventMap.end( ) )
  {
    return NULL;
  }
  else
  {
    GraphNode* node = iter->second;
    ompTargetDeviceFirstEventMap.erase( iter );
    return node;
  }
}

void
AnalysisParadigmOMP::setOmpTargetLastEvent( GraphNode* node )
{
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId( ) ) !=
       ompTargetDeviceFirstEventMap.end( ) )
  {
    ompTargetDeviceLastEventMap[node->getStreamId( )] = node;
  }
}

GraphNode*
AnalysisParadigmOMP::consumeOmpTargetLastEvent( uint64_t streamId )
{
  OmpEventMap::iterator iter = ompTargetDeviceLastEventMap.find( streamId );
  if ( iter == ompTargetDeviceLastEventMap.end( ) )
  {
    return NULL;
  }
  else
  {
    GraphNode* node = iter->second;
    ompTargetDeviceLastEventMap.erase( iter );
    return node;
  }
}

void
AnalysisParadigmOMP::pushOmpTargetRegion( GraphNode* node, uint64_t regionId )
{
  ompTargetStreamRegionsMap[node->getStreamId( )].first[regionId] = node;
  ompTargetStreamRegionsMap[node->getStreamId( )].second.push_back( regionId );
}

void
AnalysisParadigmOMP::popOmpTargetRegion( GraphNode* node )
{
  OmpStreamRegionsMap::iterator iter = ompTargetStreamRegionsMap.find(
    node->getStreamId( ) );
  if ( iter != ompTargetStreamRegionsMap.end( ) )
  {
    uint64_t region_id = iter->second.second.back( );
    iter->second.second.pop_back( );
    iter->second.first.erase( region_id );

    if ( iter->second.second.empty( ) )
    {
      ompTargetStreamRegionsMap.erase( iter );
    }
  }
}

GraphNode*
AnalysisParadigmOMP::findOmpTargetParentRegion( GraphNode* node,
                                                uint64_t   parentRegionId )
{
  /* search all current streams with parallel region ids */
  for ( OmpStreamRegionsMap::const_iterator esIter =
          ompTargetStreamRegionsMap.begin( );
        esIter != ompTargetStreamRegionsMap.end( ); ++esIter )
  {
    if ( esIter->first != node->getStreamId( ) )
    {
      /* search the current stack of parallel region ids of this *stream */
      std::map< uint64_t, GraphNode* >::const_iterator keyNodeIter =
        esIter->second.first.find( parentRegionId );

      if ( keyNodeIter != esIter->second.first.end( ) )
      {
        return keyNodeIter->second;
      }
    }
  }
  return NULL;
}
