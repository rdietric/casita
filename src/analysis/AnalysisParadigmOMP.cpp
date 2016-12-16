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

#include <vector>

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
    //addRule( new OMPTComputeRule( 1 ) );
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

/**
 * Handle leave nodes of the OpenMP paradigm during the trace read process.
 * 
 * @param ompLeave OpenMP leave node
 */
void
AnalysisParadigmOMP::handlePostLeave( GraphNode* ompLeave )
{
  if ( ompLeave->isOMPForkJoinRegion() &&
       ( commonAnalysis->getStream( ompLeave->getStreamId() )->getStreamType()
         == EventStream::ES_DEVICE ) )
  {
    popOmpTargetRegion( ompLeave );
  }

  //\todo: move into barrier rule?
  if ( ompLeave->isOMPSync() )
  {
    Edge *edge = commonAnalysis->getEdge( ompLeave->getPartner(), ompLeave );
    
    // mark this barrier if it has callees 
    if ( !edge )
    {
      ompLeave->setCounter( OMP_IGNORE_BARRIER, 1 );
      //UTILS_WARNING( "Ignore barrier for %s", node->getUniqueName().c_str());
    }

    // this applies only to OMPT events
    if( commonAnalysis->haveParadigm( PARADIGM_OMPT ) )
    {
      // if a sync operation is nested into another sync 
      // (e.g. wait_barrier inside of barrier)
      if( ompLeave->getCaller() && ompLeave->getCaller()->isOMPSync() )
      {
        if( edge && edge->isBlocking() )
        {
          edge->unblock();
          UTILS_WARNING( "Unblock edge %s", edge->getName().c_str() );
        }
      }
      else // outer sync
      {
        // get edge to predecessor node
        /*edge = commonAnalysis->getEdge( ompLeave->getGraphPair().first, ompLeave );
        
        Graph::EdgeList& inEdges = 
          commonAnalysis->getGraph().getInEdges( ompLeave );
        
        for ( EdgeList::const_iterator eIter = iter->second.begin( );
          eIter != iter->second.end( ); ++eIter )
        {
          
        }
        
        if( inEdges.size() > 1 )
        {
          
        }
        else
        {
          edge = inEdges.front();
        }*/
        
        // make the edge blocking
        if( edge )
        {
          edge->makeBlocking();
        }
        else
        {
          //UTILS_WARNING( "Edge between %s and %s should be available!", 
          //               ompLeave->getGraphPair().first->getUniqueName().c_str(), 
          //               ompLeave->getUniqueName().c_str() );
          
          // add new blocking edge for barrier region (no way in CPA)
          // leave out, as CPA needs edges and if no edge exists ...
          commonAnalysis->newEdge( ompLeave->getGraphPair().first, ompLeave, 
                                   EDGE_IS_BLOCKING );
        }
        
        //// get parallel region enter ////
        
        // get first parallel region compute event (e.g. implicit task enter)
        GraphNode* parallelEnter = ompLeave;
        while( parallelEnter->getCaller() )
        {
          parallelEnter = parallelEnter->getCaller();
        }
        
        // get parallel region event which is linked from the first compute event
        parallelEnter = (GraphNode*) parallelEnter->getLink();
        
        if( parallelEnter )
        {
          // if no barrier has been set for the given parallel enter
          // OR the current barrier event has entered the barrier later
          if( ompParallelLastBarrierMap.count( parallelEnter ) == 0 || 
              ( ompLeave->getGraphPair().first > 
                ompParallelLastBarrierMap[ parallelEnter ]->getGraphPair().first ) )
          {
              ompParallelLastBarrierMap[ parallelEnter ] = ompLeave;
          }
        }
        else // if link is not set this is the barrier of the master thread
        {
          UTILS_WARNING( "Barrier of master thread %s",  ompLeave->getUniqueName().c_str() );
        }
        
        // if this is the barrier of the master stream
        if( parallelEnter->getStreamId() == ompLeave->getStreamId() )
        {
          UTILS_WARNING( "sBarrier of master thread %s",  ompLeave->getUniqueName().c_str() );
          
        }
      }
    }
  }
  
  // this applies only to OMPT events
  if( commonAnalysis->haveParadigm( PARADIGM_OMPT ) )
  {
    //// Parallel Rule /////
    
    // at parallel leave (AND enter available)
    if( ompLeave->isOMPParallel() && ompLeave->getGraphPair().first )
    {
      // use parallel enter node to get the last barrier leave
      GraphNode *barrierLeave = ompParallelLastBarrierMap[ ompLeave->getGraphPair().first ];

      if( barrierLeave )
      {
        // unblock barrier
        Edge *edge = commonAnalysis->getEdge( barrierLeave->getGraphPair().first, barrierLeave );
        if( edge )
        {
          edge->unblock();
        }
        else
        {
          //UTILS_WARNING( "Edge should be available. Create it???" );
          commonAnalysis->newEdge( barrierLeave->getGraphPair().first, barrierLeave, 
                                   EDGE_NONE );
        }

        // add dependency edge between barrier leave and parallel leave
        commonAnalysis->newEdge( barrierLeave, ompLeave ); 
        
        UTILS_WARNING( "Edge to %s -> %s created!", 
                       barrierLeave->getUniqueName().c_str(),
                       ompLeave->getUniqueName().c_str() );
      }
      else
      {
        UTILS_WARNING( "No edge to %s created!", ompLeave->getUniqueName().c_str() );
      }
    }
    
    //////////////////////
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
        if( ompParallelMap.count( parallel_id ) > 0 )
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
  IdNodeMap::iterator iter = ompTargetRegionBeginMap.find( streamId );
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
  IdNodeMap::iterator iter = ompTargetDeviceFirstEventMap.find(
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
  IdNodeMap::iterator iter = ompTargetDeviceLastEventMap.find( streamId );
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
