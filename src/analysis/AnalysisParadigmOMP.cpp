/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014-2018,
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
#include "omp/OMPTTargetRule.hpp"
//#include "omp/OMPTargetBarrierRule.hpp"

using namespace casita;
using namespace casita::omp;
using namespace casita::io;

AnalysisParadigmOMP::AnalysisParadigmOMP( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine )
{  
  // use different rules for OMPT and OPARI2 instrumentation
  if( !analysisEngine->haveParadigm( PARADIGM_OMPT ) )
  {
    addRule( new OMPForkJoinRule( 1 ) );
    addRule( new OMPComputeRule( 1 ) );
    addRule( new OMPBarrierRule( 1 ) );
  }
  
  // add OpenMP target rules only, if a MIC device stream is available
  if( analysisEngine->haveParadigm( PARADIGM_OMP_TARGET ) )
  {
    addRule( new OMPTTargetRule( 1 ) );
    //addRule( new OMPTargetRule( 1 ) );        // libmpti is deprecated
    //addRule( new OMPTargetBarrierRule( 1 ) ); //libmpti is deprecated
  }
  
  nestingLevel = 0;
}

AnalysisParadigmOMP::~AnalysisParadigmOMP(){ }

Paradigm
AnalysisParadigmOMP::getParadigm()
{
  return PARADIGM_OMP;
}

void 
AnalysisParadigmOMP::omptParallelRule( GraphNode* ompLeave )
{
  GraphNode *parallelEnter = ompLeave->getGraphPair().first;
    
  // at parallel leave (AND enter available)
  if( ompLeave->isOMPParallel() && parallelEnter )
  {
    // evaluate barriers in current parallel region
    if( ompBarrierNodesMap.count( parallelEnter ) > 0 )
    {
      for( VecNodeVec::iterator itParallel = 
             ompBarrierNodesMap[parallelEnter].begin();
           itParallel != ompBarrierNodesMap[parallelEnter].end(); ++itParallel )
      {
        UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC,
                     "[OMPT] Parallel region %s with %u barrier(s) on %u threads", 
                     parallelEnter->getUniqueName().c_str(),
                     (unsigned int) ompBarrierNodesMap[parallelEnter].size(),
                     (unsigned int) itParallel->size() );

        /// find latest barrier enter ///
        GraphNode *latestBarrierEnter = NULL;
        GraphNodeVec::const_iterator itBarrier = itParallel->begin();
        for( ;itBarrier != itParallel->end(); ++itBarrier )
        {
          //UTILS_WARNING( "[OMPT] Barrier %s", (*itBarrier)->getUniqueName().c_str() );

          // set the latest barrier
          if( latestBarrierEnter == NULL || 
              Node::compareLess( latestBarrierEnter, (*itBarrier)->getGraphPair().first ) )
          {
              latestBarrierEnter = (*itBarrier)->getGraphPair().first;
          }
        }

        bool lastBarrier = ompBarrierNodesMap[parallelEnter].back() == (*itParallel);
        
        // if this is the last barrier in the parallel region
        if( lastBarrier )
        {
          Edge *edge = analysisEngine->newEdge(
            latestBarrierEnter, ( GraphNode * )parallelEnter->getData() );

          // in case this edge was a reverse edge, unblock it
          edge->unblock();
        }
        
        // accumulate blame, set edges from latest enter to all other leaves
        uint64_t blame = 0;
        for( itBarrier = itParallel->begin(); itBarrier != itParallel->end(); ++itBarrier )
        {
          GraphNode *barrierEnter = (*itBarrier)->getGraphPair().first;
          
          // for all barriers nodes except the latest
          if( barrierEnter != latestBarrierEnter )
          {
            // if this is not the last barrier in the region
            if( !lastBarrier )
            {
              // create inter stream edge
              Edge *edge = analysisEngine->newEdge(
                latestBarrierEnter, ( GraphNode * )parallelEnter->getData() );
              
              // in case this edge was a reverse edge, unblock it
              edge->unblock();
            }

            // compute waiting time and blame for this barrier region
            uint64_t waitingTime = 
              latestBarrierEnter->getTime() - barrierEnter->getTime();
            
            // set waiting time counter
            (*itBarrier)->setCounter( WAITING_TIME, waitingTime );
            
            // add waiting time to blame
            blame += waitingTime;
          }
        }

        //UTILS_WARNING("Distribute blame %" PRIu64 " to %s", blame, 
        //               commonAnalysis->getNodeInfo( latestBarrierEnter).c_str() );
        
        distributeBlame( analysisEngine,
                         latestBarrierEnter,
                         blame,
                         ompHostStreamWalkCallback,
                         REASON_OMP_BARRIER );

        // clear barrier vector after processing the respective barrier
        itParallel->clear();
      }
      
      // clear vector of barriers
      ompBarrierNodesMap[parallelEnter].clear();

      // remove parallel region from map
      ompBarrierNodesMap.erase( parallelEnter );

      // iterate over all stream parallel barrier stack
      for( StreamParallelBarrierMap::iterator itStream = 
             ompParallelBarriersMap.begin();
           ompParallelBarriersMap.end() != itStream; ++itStream )
      {
        // remove the parallel region from the stack, if this stream a member
        if( !itStream->second.empty() &&
            itStream->second.top().first == parallelEnter )
        {
          itStream->second.pop();
          //UTILS_WARNING( "[OMPT] Close parallel region %s", 
          //               parallelEnter->getUniqueName().c_str() );
        }
      }
    }
    else
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC,
                 "[OMPT] Parallel region %s without barriers.", 
                 parallelEnter->getUniqueName().c_str());
    }
    
    // remove this parallel region from parallel map AND
    // check open parallel worker nodes
    bool valid = false;
    uint64_t parallel_id = parallelEnter->getCounter( OMPT_REGION_ID, &valid );
    if( valid )
    {
      IdNodeMap &parallelMap = ompParallelIdNodeMap;
      EventStream* parallelStream = 
          analysisEngine->getStream(parallelEnter->getStreamId());
      if( parallelStream->isDeviceStream() )
      {
        parallelMap = ompDeviceParallelIdNodeMap;
      }
      
      if( parallelMap.count( parallel_id ) > 0 )
      {
        parallelMap.erase( parallel_id );
      }
      
      if( ompOpenWorkerNodesMap.count( parallel_id ) > 0 )
      {
        for( GraphNodeVec::const_iterator it = ompOpenWorkerNodesMap[parallel_id].begin();
               it != ompOpenWorkerNodesMap[parallel_id].end(); ++it )
        {
          // if parallel node and open node are either both host or device streams
          if( parallelStream->getStreamType() & 
              analysisEngine->getStream( (*it)->getStreamId() )->getStreamType() )
          {
            UTILS_WARNING( "[OMPT] Could not generate dependency for %s",
                           (*it)->getUniqueName().c_str() );
          }
        }
      }
    }
  }
}

/**
 * Handle synchronization leave nodes. 
 * 
 * @param syncLeave
 */
void 
AnalysisParadigmOMP::omptBarrierRule( GraphNode* syncLeave )
{
  // ignore sync operations that are nested into another sync
  // (e.g. wait_barrier inside of barrier)
  if( syncLeave->getCaller() && syncLeave->getCaller()->isOMPSync() )
  {
    // used in streamWalkCallback in OMPRulesCommon.hpp
    syncLeave->setCounter( OMP_IGNORE_BARRIER, 1 );
    //UTILS_WARNING( "Ignore barrier for %s", syncLeave->getUniqueName().c_str());
  }
  //if( !( syncLeave->getCaller() && syncLeave->getCaller()->isOMPSync() ) )
  else  
  {
    ///// this is an outer sync

    // make all in edges blocking (to not walk this edge in CPA)
    const Graph::EdgeList& inEdges = 
      analysisEngine->getGraph().getInEdges( syncLeave );
    for ( Graph::EdgeList::const_iterator eIter = inEdges.begin();
          eIter != inEdges.end( ); ++eIter )
    {
      (*eIter)->makeBlocking();
    }
    
    // \todo: Does delete in edges affects blame distribution?

    //// get parallel region enter ////

    // get first parallel region compute event (e.g. implicit task enter)
    // OR the parallel enter itself for barriers on the master thread
    GraphNode* parallelEnter = syncLeave;
    while( parallelEnter->getCaller() )
    {
      parallelEnter = parallelEnter->getCaller();

      // if we found a parallel region, it is the master thread's barrier
      if( parallelEnter->isOMPParallel() )
      {
        break;
      }
    }

    // if we found the parallel region enter
    if( parallelEnter->isOMPParallel() )
    {
      //UTILS_WARNING( "Found barrier leave on master thread (%s)!",  
      //               ompLeave->getUniqueName().c_str() );

      // set the link of the parallel enter to its last barrier
      parallelEnter->setData( syncLeave );
    }
    else
    {
      // first compute region on a thread
      if( NULL == parallelEnter->getLink() )
      {
        UTILS_WARNING( "[OMPT] %s has no link to parallel enter node!",  
                       parallelEnter->getUniqueName().c_str() );
      }
      else
      {
        // get parallel region event which is linked from the first compute event
        parallelEnter = (GraphNode*) parallelEnter->getLink();
      }
    }

    // save the current barrier number of the parallel region
    if( parallelEnter )
    {
      // if the stream has an entry and it is still the same parallel region
      if( ompParallelBarriersMap.count( syncLeave->getStreamId() ) > 0 && 
          !ompParallelBarriersMap[syncLeave->getStreamId()].empty() && // needed for next check
          ompParallelBarriersMap[syncLeave->getStreamId()].top().first == parallelEnter )
      {
        ompParallelBarriersMap[syncLeave->getStreamId()].top().second++;
      }
      else //new stream or new parallel region
      {
        ompParallelBarriersMap[syncLeave->getStreamId()].push( make_pair( parallelEnter, 0 ) );
      }

      // if parallel region is already registered
      //if( ompBarrierNodesMap.count( parallelEnter ) > 0 )
      {
        uint64_t barrierNum = ompParallelBarriersMap[syncLeave->getStreamId()].top().second;
        try
        {
          ompBarrierNodesMap[parallelEnter].at(barrierNum).push_back( syncLeave );
        }
        catch(const std::out_of_range& oor)
        {
          GraphNodeVec barriers;
          barriers.push_back( syncLeave );
          ompBarrierNodesMap[parallelEnter].push_back( barriers );
          //UTILS_OUT( "New vector of barriers created (%s)", oor.what() );
        }
      }
    }
  }
}

/**
 * Get nesting level of parallel regions during trace reading.
 * 
 * @return nesting level of parallel regions
 */
size_t
AnalysisParadigmOMP::getNestingLevel()
{
  return nestingLevel;
}

/**
 * Handle enter nodes of the OpenMP paradigm during the trace read process.
 * 
 * @param ompEnter OpenMP leave node
 */
void
AnalysisParadigmOMP::handlePostEnter( GraphNode* ompEnter )
{
  if ( ompEnter->isOMPForkJoin() )
  {
    nestingLevel++;
  }
}

/**
 * Handle leave nodes of the OpenMP paradigm during the trace read process.
 * 
 * @param ompLeave OpenMP leave node
 */
void
AnalysisParadigmOMP::handlePostLeave( GraphNode* ompLeave )
{
  if ( ompLeave->isOMPForkJoin() )
  {
    nestingLevel--;
  }

  if ( ompLeave->isOMPSync() )
  {
    GraphNode* ompSyncEnter = ompLeave->getGraphPair().first;
    
    // get barrier region edge, which is only available if no event is nested
    Edge *edge = analysisEngine->getEdge( ompSyncEnter, ompLeave );
    
    // mark this barrier if it has a synchronization/barrier nested within the region
    if ( !edge )
    {
      // used in streamWalkCallback in OMPRulesCommon.hpp
      ompLeave->setCounter( OMP_IGNORE_BARRIER, 1 );
      //UTILS_WARNING( "Ignore barrier for %s", node->getUniqueName().c_str());
    }

    // this is basically the barrier rule for OMPT-based OpenMP traces
    if( analysisEngine->haveParadigm( PARADIGM_OMPT ) )
    {
      omptBarrierRule( ompLeave );
    }
  }
  
  // this applies only to OMPT events
  if( analysisEngine->haveParadigm( PARADIGM_OMPT ) )
  {
    // handle parallel region events and first event on OpenMP threads of a parallel region
    omptParallelRule( ompLeave );
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
                                           GraphNode*        enterNode,
                                           OTF2KeyValueList* list )
{
  // if the trace has been generated with OMPT instrumentation
  if( analysisEngine->haveParadigm( PARADIGM_OMPT ) )
  {
    if( analysisEngine->haveParadigm( PARADIGM_OMP_TARGET ) && 
        enterNode->isOMPTarget() )
    {
      int32_t devIdKey = reader->getFirstKey( SCOREP_OMPT_DEVICE_ID );

      if ( devIdKey > -1 && list && list->getSize() > 0 && 
           list->testAttribute( (uint32_t)devIdKey ) )
      {
        uint64_t device_id = 0;

        list->getUInt64( (uint32_t)devIdKey, &device_id );

        //UTILS_OUT( "Found device id %"PRIu64" on %s", 
        //           device_id, node->getUniqueName().c_str());

        // use this for the device ID instead of the stream ID
        // referenced stream ID is initialized to zero!
        enterNode->setReferencedStreamId( device_id );
        enterNode->setData( (void*)1 );
      }
    }
  
    // get the parallel region ID, 
    int32_t parallelIdKey = reader->getFirstKey( SCOREP_OMPT_PARALLEL_ID );
    if ( parallelIdKey > -1 && list && list->getSize() > 0 &&
         list->testAttribute( (uint32_t)parallelIdKey ) )
    {
      // we enter parallel region, either as master or worker thread ...
      
      uint64_t parallel_id = 0;
      
      list->getUInt64( (uint32_t)parallelIdKey, &parallel_id );      
      
      //UTILS_OUT( "Found parallel id %"PRIu64" on %s", 
      //           parallel_id, node->getUniqueName().c_str());
      
      // if parallel region enter event
      if( enterNode->isOMPParallel() )
      {
        IdNodeMap &parallelMap = ompParallelIdNodeMap;
        
        EventStream* parallelStream = 
          analysisEngine->getStream(enterNode->getStreamId());
        if( parallelStream->isDeviceStream() )
        {
          parallelMap = ompDeviceParallelIdNodeMap;
        }
        
        // if parallel region is already listed
        if( parallelMap.count( parallel_id ) > 0 )
        {
          UTILS_WARNING( "[OMPT] Overwriting parallel region (%" PRIu64 ") %s with %s!",
                         parallel_id,
                         analysisEngine->getNodeInfo( parallelMap[parallel_id] ).c_str(),
                         analysisEngine->getNodeInfo( enterNode ).c_str() );
        }
        
        // store parallel region ID to associate other threads with this region
        parallelMap[ parallel_id ] = enterNode;
        
        // store the parallel region id to remove this region from map at parallel leave
        enterNode->setCounter( OMPT_REGION_ID, parallel_id );
        
        // due to measurement artefacts the parallel region was opened late
        // for open nodes of this parallel region
        if( ompOpenWorkerNodesMap.count( parallel_id ) > 0 )
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                "[OMPT] Fix dependencies for parallel region %" PRIu64 " at %s",
                parallel_id, analysisEngine->getNodeInfo( enterNode ).c_str() );
          
          GraphNodeVec::iterator it = 
            ompOpenWorkerNodesMap[parallel_id].begin();
          while( it != ompOpenWorkerNodesMap[parallel_id].end() )
          {
            // if parallel node and open node are either both host or device streams
            if( parallelStream->getStreamType() & 
                analysisEngine->getStream( (*it)->getStreamId() )->getStreamType() )
            {
              // add edge for critical path analysis
              analysisEngine->newEdge( enterNode, *it );

              // add link to store dependency for leave event
              (*it)->setLink( enterNode );
              
              it = ompOpenWorkerNodesMap[ parallel_id ].erase( it );
            }
            else
            {
              ++it;
            }
          }
        }
      }
      // if node has no caller (first event on a stream), but no parallel
      else if ( enterNode->getCaller() == NULL )
      {
        IdNodeMap &parallelMap = ompParallelIdNodeMap;
        if( analysisEngine->getStream( enterNode->getStreamId() )->isDeviceStream() )
        {
          parallelMap = ompDeviceParallelIdNodeMap;
        }
        
        // create dependency edge to associated parallel region begin event
        if( parallelMap.count( parallel_id ) > 0 )
        {
          // add edge for critical path analysis
          analysisEngine->newEdge( parallelMap[ parallel_id ], enterNode );
          
          // add link to store dependency for leave event
          enterNode->setLink( parallelMap[ parallel_id ] );
        }
        else
        {
          UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                     "[OMPT] Parallel region %" PRIu64 " at %s not available!",
                     parallel_id,
                     analysisEngine->getNodeInfo( enterNode ).c_str() );
          
          // store in open dependency list (measurement artefacts)
          ompOpenWorkerNodesMap[parallel_id].push_back( enterNode );
        }
      }
    }
  }

  /* this is only for offloaded regions (deprecated, used in libmpti implementation)
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

    // region id
    streamRefKey = reader->getFirstKey( SCOREP_OMP_TARGET_REGION_ID );
    if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
         list->getUInt64( (uint32_t)streamRefKey,
                          &key_value ) == OTF2KeyValueList::KV_SUCCESS )
    {
      pushOmpTargetRegion( node, key_value );

      if ( node->isOMPSync( ) )
      {
        node->setCounter( OMPT_REGION_ID, key_value );
      }
    }
  }*/
}

void
AnalysisParadigmOMP::handleKeyValuesLeave( OTF2TraceReader*  reader,
                                           GraphNode*        node,
                                           GraphNode*        oldNode,
                                           OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_OMP_TARGET_LOCATIONREF );

  if ( streamRefKey > -1 && list && list->getSize() > 0 &&
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
AnalysisParadigmOMP::popFork()
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
  return ompComputeTrackMap[ streamId ];
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
  ompComputeTrackMap[ streamId ] = node;
}

const GraphNode::GraphNodeList&
AnalysisParadigmOMP::getBarrierEventList( bool device, GraphNode* caller, int matchingId )
{
  if ( device )
  {
    return ompBarrierListDevice[ std::make_pair( 0, matchingId ) ];
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
  if ( node->isEnter() )
  {
    leaveNode = node->getPartner();
  }

  // only add barrier activities that have no callees
  if ( leaveNode->getCounter( OMP_IGNORE_BARRIER, NULL ) )
  {
    return;
  }

  if ( device )
  {
    ompBarrierListDevice[ std::make_pair( 0, matchingId ) ].push_back( node );
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
    ompBarrierListDevice[ std::make_pair( 0, matchingId ) ].clear( );
  }
  else
  {
    ompBarrierListHost.clear();
  }
}

/**
 * Set the OpenMP target begin node on a host stream.
 * \todo: the current measurement does not provide information reasonable 
 * information on the correlation between host and device tasks
 * It is assumed that a target region is blocking.
 * 
 * @param node OpenMP target begin node
 */
void
AnalysisParadigmOMP::setTargetEnter( GraphNode* node )
{
  if ( ompTargetRegionBeginMap.find( node->getStreamId() ) !=
       ompTargetRegionBeginMap.end() )
  {
    UTILS_WARNING( "[OpenMP Offloading]: Nested target regions detected. "
                   "Replacing target begin with %s",
                   node->getUniqueName().c_str() );
  }

  ompTargetRegionBeginMap[node->getStreamId()] = node;
}

/**
 * Get the earliest OpenMP target enter node for a given targetDeviceId.
 * 
 * @param targetDeviceId target device ID (from OTF2 attribute)
 */
GraphNode*
AnalysisParadigmOMP::getTargetEnter( int targetDeviceId )
{
  if( targetDeviceId == -1 )
  {
    UTILS_WARNING( "[OpenMP Offloading]: Cannot get target enter node! "
                   "Invalid target device ID!" )
    return NULL;
  }
  
  //UTILS_OUT( "getTargetEnter for device id %d (%lu)", targetDeviceId, 
  //           ompTargetRegionBeginMap.size() );

  GraphNode* targetEnter = NULL;
  for ( IdNodeMap::const_iterator it = ompTargetRegionBeginMap.begin();
        it != ompTargetRegionBeginMap.end(); ++it )
  {
    GraphNode* currEnter = it->second;
    
    //UTILS_OUT( "Target data %p, refstream %llu", currEnter->getData(), 
    //           currEnter->getReferencedStreamId() );
    
    if( currEnter->getData() && 
        currEnter->getReferencedStreamId() == (uint64_t)targetDeviceId )
    {
      if( !targetEnter || ( targetEnter && Node::compareLess( currEnter, targetEnter ) ) )
      {
        targetEnter = currEnter;
      }
    }
  }
  
  return targetEnter;
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
  if ( iter == ompTargetRegionBeginMap.end() )
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

bool
AnalysisParadigmOMP::isFirstTargetOffloadEvent( uint64_t streamId )
{
  return ompTargetDeviceFirstEventMap.find( streamId ) ==
                                             ompTargetDeviceFirstEventMap.end();
}

void
AnalysisParadigmOMP::setTargetOffloadFirstEvent( GraphNode* node )
{
  if( node->isLeave() )
  {
    UTILS_WARNING( "First offload event cannot be a leave (%s)! Using the "
                   "respective enter instead.", node->getUniqueName().c_str() );
    node = node->getGraphPair().first;
  }
  
  // if node is not yet in the map
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId() ) ==
       ompTargetDeviceFirstEventMap.end() )
  {
    ompTargetDeviceFirstEventMap[node->getStreamId()] = node;
  }
}

GraphNode*
AnalysisParadigmOMP::consumTargetOffloadFirstEvent( uint64_t streamId )
{
  IdNodeMap::iterator iter = ompTargetDeviceFirstEventMap.find( streamId );
  if ( iter == ompTargetDeviceFirstEventMap.end() )
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
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId() ) !=
       ompTargetDeviceFirstEventMap.end() )
  {
    ompTargetDeviceLastEventMap[node->getStreamId()] = node;
  }
}

GraphNode*
AnalysisParadigmOMP::consumeOmpTargetLastEvent( uint64_t streamId )
{
  IdNodeMap::iterator iter = ompTargetDeviceLastEventMap.find( streamId );
  if ( iter == ompTargetDeviceLastEventMap.end() )
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
  ompTargetStreamRegionsMap[node->getStreamId()].first[regionId] = node;
  ompTargetStreamRegionsMap[node->getStreamId()].second.push_back( regionId );
}

void
AnalysisParadigmOMP::popOmpTargetRegion( GraphNode* node )
{
  OmpStreamRegionsMap::iterator iter = ompTargetStreamRegionsMap.find(
    node->getStreamId() );
  if ( iter != ompTargetStreamRegionsMap.end() )
  {
    uint64_t region_id = iter->second.second.back();
    iter->second.second.pop_back();
    iter->second.first.erase( region_id );

    if ( iter->second.second.empty() )
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
          ompTargetStreamRegionsMap.begin();
        esIter != ompTargetStreamRegionsMap.end(); ++esIter )
  {
    if ( esIter->first != node->getStreamId() )
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
