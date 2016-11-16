/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014, 2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * - perform analysis with CUDA-specific rules
 * - add kernels as pending, consume pending kernels
 * - Callbacks for KeyValue, postEnter/leave
 *
 */

#include "cuda/AnalysisParadigmCUDA.hpp"
#include "AnalysisEngine.hpp"

#include "cuda/BlameKernelRule.hpp"
#include "cuda/BlameSyncRule.hpp"
#include "cuda/LateSyncRule.hpp"
#include "cuda/KernelExecutionRule.hpp"
#include "cuda/EventLaunchRule.hpp"
#include "cuda/EventSyncRule.hpp"
#include "cuda/EventQueryRule.hpp"
#include "cuda/StreamWaitRule.hpp"

using namespace casita;
using namespace casita::cuda;
using namespace casita::io;

AnalysisParadigmCUDA::AnalysisParadigmCUDA( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine )
{
  addRule( new KernelExecutionRule( 9 ) );
  //\todo: check priority for the following three rules triggered on cudaSync
  // they all clear the list of pending kernels when finished
  addRule( new BlameKernelRule( 2 ) ); // triggered on cudaSync
  //addRule( new BlameSyncRule( 1 ) );   // triggered on cudaSync
  addRule( new LateSyncRule( 1 ) );    // triggered on cudaSync
  addRule( new EventLaunchRule( 1 ) );
  addRule( new EventSyncRule( 1 ) );
  addRule( new EventQueryRule( 1 ) );
  addRule( new StreamWaitRule( 1 ) );
}

AnalysisParadigmCUDA::~AnalysisParadigmCUDA( )
{
  reset();
}

void
AnalysisParadigmCUDA::reset()
{  
  UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
             "Cleanup CUDA support structures" );
  
  /* clean up pending kernel launches
  if( pendingKernelLaunchMap.size() )
  {
    size_t pendingOperations = 0;
    for ( IdNodeListMap::iterator mapIter = pendingKernelLaunchMap.begin( );
          mapIter != pendingKernelLaunchMap.end( ); ++mapIter )
    {
      pendingOperations += mapIter->second.size();
      
//      if( Parser::getVerboseLevel() >= VERBOSE_BASIC )
//      {
//        GraphNode::GraphNodeList list = mapIter->second;
//        for(GraphNode::GraphNodeList::const_iterator it = mapIter->second.begin(); 
//            it != mapIter->second.end(); ++it )
//        {
//          UTILS_MSG( true, "#### %s",  commonAnalysis->getNodeInfo(*it).c_str() );
//        }
//      }
      
      mapIter->second.clear();
    }

    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "... %llu pending kernel launches on %llu streams",
               pendingOperations, pendingKernelLaunchMap.size() );


    pendingKernelLaunchMap.clear();
  }*/
    
  // clear event record/launch map and event query map
  if( eventLaunchMap.size() )
  {
    UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
               "... %llu event launches/records",
               eventLaunchMap.size() );
    eventLaunchMap.clear();
  }
  /*
  // clear event query map
  if( eventQueryMap.size() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "... %llu event query operations",
               eventQueryMap.size() );
    eventQueryMap.clear();
  }

  // clear lists of stream wait operations (for each stream)
  if( streamWaitMap.size() )
  {
    size_t pendingOperations = 0;
    for ( IdEventsListMap::iterator mapIter = streamWaitMap.begin( );
          mapIter != streamWaitMap.end( ); ++mapIter )
    {
      pendingOperations += mapIter->second.size();
      mapIter->second.clear();
    }

    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "... %llu stream wait operations on %llu streams",
               pendingOperations, streamWaitMap.size() );

    streamWaitMap.clear();
  }
  
  // Clear list of null stream wait operations
  if( nullStreamWaits.size() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "... %llu null stream wait operations",
             nullStreamWaits.size() );
    nullStreamWaits.clear();
  }

  // clear event ID process map
  if( eventProcessMap.size() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
             "... %llu event stream mappings",
             eventProcessMap.size() );
    eventProcessMap.clear();
  }*/
}

Paradigm
AnalysisParadigmCUDA::getParadigm( )
{
  return PARADIGM_CUDA;
}

void
AnalysisParadigmCUDA::handlePostEnter( GraphNode* node )
{
  if ( node->isCUDAKernelLaunch( ) )
  {
    /* std::cout << "[" << commonAnalysis->getMPIRank() << "] add ENTER launch: " << node->getUniqueName() << std::endl;
     **/
    addPendingKernelLaunch( node );
  }
}

void
AnalysisParadigmCUDA::handlePostLeave( GraphNode* node )
{
  if ( node->isCUDAKernelLaunch( ) )
  {
    /* std::cout << "[" << commonAnalysis->getMPIRank() << "] add LEAVE launch: " << node->getUniqueName() << std::endl;
     **/
    addPendingKernelLaunch( node );
  }
}

void
AnalysisParadigmCUDA::handleKeyValuesEnter( OTF2TraceReader*     reader,
                                            GraphNode*        node,
                                            OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_CUDA_STREAMREF );

//  if( streamRefKey > -1 && list->getLocationRef( (uint32_t)streamRefKey,
//                             &refValue ) != OTF2KeyValueList::KV_SUCCESS ){
//    std::cerr << "CUDA::handleKeyValuesEnter streamRefKey: " << streamRefKey << std::endl;
//  }

  if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
  }
}

/**
 * Set the referenced stream for both given nodes (leave and enter).
 * 
 * @param reader
 * @param node
 * @param oldNode
 * @param list
 */
void
AnalysisParadigmCUDA::handleKeyValuesLeave( OTF2TraceReader*     reader,
                                            GraphNode*        node,
                                            GraphNode*        oldNode,
                                            OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_CUDA_STREAMREF );
  
//  if( streamRefKey > -1 && list->getLocationRef( (uint32_t)streamRefKey,
//                             &refValue ) != OTF2KeyValueList::KV_SUCCESS ){
//    std::cerr << "CUDA::handleKeyValuesLeave streamRefKey: " << streamRefKey << std::endl;
//  }

  if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
    oldNode->setReferencedStreamId( refValue );
  }
}


//////////////////////////////////////////////////////////////
////////////// CUDA rules support functions //////////////////

// \todo: could be static
bool
AnalysisParadigmCUDA::isKernelPending( GraphNode* kernelNode )
{
  if( kernelNode->hasPartner() )
  {
    // kernel leave has not yet been synchronized (compare BlameKernelRule)
    if( kernelNode->getGraphPair().second->getLink() == NULL )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "[%"PRIu32"] Do not delete unsynchronized kernel %s", 
                 this->commonAnalysis->getMPIRank(), 
                 this->commonAnalysis->getNodeInfo( kernelNode ).c_str() );
      return false;
    }
  }
  /* enter kernel nodes without partner must NOT be deleted
  else if( kernelNode->isEnter() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "[%"PRIu64"] Do not delete incomplete kernel %s", 
               p->getId(), getNodeInfo( *it ).c_str() );
    continue;
  }*/

  if( ( kernelNode->isEnter() && !kernelNode->hasPartner() ) || 
      ( kernelNode->hasPartner() && kernelNode->getGraphPair().second->getLink() == NULL ) )
  {
    return false;
  }
  
  return true;
}

void
AnalysisParadigmCUDA::setLastEventLaunch( EventNode* eventLaunchLeave )
{
  eventLaunchMap[eventLaunchLeave->getEventId()] = eventLaunchLeave;
}

EventNode*
AnalysisParadigmCUDA::consumeLastEventLaunchLeave( uint64_t eventId )
{
  IdEventNodeMap::iterator iter = eventLaunchMap.find( eventId );
  if ( iter != eventLaunchMap.end( ) )
  {
    EventNode* node = iter->second;
    eventLaunchMap.erase( iter );
    return node;
  }
  else
  {
    return NULL;
  }
}

EventNode*
AnalysisParadigmCUDA::getEventRecordLeave( uint64_t eventId ) const
{
  IdEventNodeMap::const_iterator iter = eventLaunchMap.find( eventId );
  if ( iter != eventLaunchMap.end( ) )
  {
    return iter->second;
  }
  else
  {
    return NULL;
  }
}

void
AnalysisParadigmCUDA::printDebugInformation( uint64_t eventId )
{
  UTILS_MSG( true, "Passed event id: %llu", eventId );
  
  UTILS_MSG( true, "Number of stored event IDs with corresponding last event record leave node: %llu",
             eventLaunchMap.size() );
  
  uint64_t pendingKernelCount = 0;
  for ( IdNodeListMap::const_iterator mapIter =
          pendingKernelLaunchMap.begin( );
        mapIter != pendingKernelLaunchMap.end( ); ++mapIter )
  {
    pendingKernelCount += mapIter->second.size();
    
    if( mapIter->second.size() )
    {
      UTILS_MSG( true, "%llu pending kernel launches on stream %llu",
                 mapIter->second.size(), mapIter->first );
      
      EventStream* evtStream = this->commonAnalysis->getStream( mapIter->first );
      if( evtStream )
      {
        UTILS_MSG( true, "  ... with stream name: %s", evtStream->getName() );
      }
      else
      {
        UTILS_MSG( true, "  ... with missing stream object" );
      }
                 
    }
  }
  UTILS_MSG( true, "%llu pending kernel launches on %llu different device streams",
             pendingKernelCount, pendingKernelLaunchMap.size() );
  
  EventNode* eventRecordLeave = eventLaunchMap[eventId];
  
  uint64_t streamId = eventRecordLeave->getStreamId( );
  
  UTILS_MSG( true, "Host stream: %llu (%s)",
             streamId,
             this->commonAnalysis->getStream( streamId )->getName() );
}

void
AnalysisParadigmCUDA::setEventProcessId( uint64_t eventId, uint64_t streamId )
{
  eventProcessMap[eventId] = streamId;
}

uint64_t
AnalysisParadigmCUDA::getEventProcessId( uint64_t eventId ) const
{
  IdIdMap::const_iterator iter = eventProcessMap.find( eventId );
  if ( iter != eventProcessMap.end( ) )
  {
    return iter->second;
  }
  else
  {
    return 0;
  }
}

/**
 * Adds kernel launch event nodes at the end of the list.
 * 
 * @param launch a kernel launch leave or enter node
 */
void
AnalysisParadigmCUDA::addPendingKernelLaunch( GraphNode* launch )
{
  // append at tail (FIFO)
  pendingKernelLaunchMap[launch->getReferencedStreamId( )].push_back(
    launch );
}

/**
 * Takes the stream ID where the kernel is executed and consumes its
 * corresponding kernel launch enter event. Consumes the first kernel launch 
 * enter event in the list of the given stream.
 * Is triggered by a kernel leave event.
 * 
 * @param kernelStreamId stream ID where the kernel is executed
 */
GraphNode*
AnalysisParadigmCUDA::consumeFirstPendingKernelLaunchEnter( uint64_t kernelStreamId )
{
  IdNodeListMap::iterator mapIter = 
    pendingKernelLaunchMap.find( kernelStreamId );
  
  // return NULL, if the element could not be found
  if ( mapIter == pendingKernelLaunchMap.end( ) )
  {
    return NULL;
  }

  // return NULL, if the list of pending kernel launch events is empty
  if ( mapIter->second.size( ) == 0 )
  {
    return NULL;
  }

  ////////////////// consume from head (FIFO) //////////////////
  
  // 
  // listIter->second (launch kernel node list) contains enter and leave records
  // set iterator to first element which should be a launch enter node
  GraphNode::GraphNodeList::iterator launchIter = mapIter->second.begin( );
  
  // skip leading leave nodes, as only enter nodes are erased
  while ( ( launchIter != mapIter->second.end( ) ) &&
          ( ( *launchIter )->isLeave( ) ) )
  {
    launchIter++;
  }
  
  if ( launchIter == mapIter->second.end( ) )
  {
    return NULL;
  }

  // erase this enter record
  GraphNode* kernelLaunch = *launchIter;
  mapIter->second.erase( launchIter );
  
  return kernelLaunch;
}

/** 
 * Find last kernel launch (leave record) which launched a kernel for the 
 * given device stream and happened before the given time stamp.
 * \todo: launch leave nodes remain in the list.
 * 
 * @param timestamp 
 * @param deviceStreamId
 * 
 * @return
 */
GraphNode*
AnalysisParadigmCUDA::getLastKernelLaunchLeave( uint64_t timestamp,
                                                uint64_t deviceStreamId ) const
{
  GraphNode* lastLaunchLeave = NULL;

  for ( IdNodeListMap::const_iterator listIter =
          pendingKernelLaunchMap.begin( );
        listIter != pendingKernelLaunchMap.end( ); ++listIter )
  {
    for ( GraphNode::GraphNodeList::const_reverse_iterator launchIter =
            listIter->second.rbegin( );
          launchIter != listIter->second.rend( ); ++launchIter )
    {
      GraphNode* gLaunchLeave = *launchIter;

      if ( gLaunchLeave->isEnter( ) )
      {
        continue;
      }

      uint64_t refDeviceProcessId =
        gLaunchLeave->getGraphPair( ).first->getReferencedStreamId( );

      // found the last kernel launch (leave) on this stream, break
      if ( ( refDeviceProcessId == deviceStreamId ) &&
           ( gLaunchLeave->getTime( ) <= timestamp ) )
      {
        // if this is the latest kernel launch leave so far, remember it
        if ( !lastLaunchLeave ||
             ( gLaunchLeave->getTime( ) > lastLaunchLeave->getTime( ) ) )
        {
          lastLaunchLeave = gLaunchLeave;
        }
        break;
      }
    }
  }
  
  return lastLaunchLeave;
}

void
AnalysisParadigmCUDA::addStreamWaitEvent( uint64_t   deviceProcId,
                                          EventNode* streamWaitLeave )
{
  EventStream* nullStream = commonAnalysis->getNullStream();
  if ( nullStream && nullStream->getId( ) == deviceProcId )
  {
    StreamWaitTagged* swTagged = new StreamWaitTagged( );
    swTagged->node = streamWaitLeave;
    nullStreamWaits.push_front( swTagged );
  }
  else
  {
    // Remove any pending streamWaitEvent with the same event ID since
    // it is replaced by this new streamWaitLeave.
    EventNode::EventNodeList& eventNodeList =
      streamWaitMap[deviceProcId];
    for ( EventNode::EventNodeList::iterator iter = eventNodeList.begin( );
          iter != eventNodeList.end( ); ++iter )
    {
      if ( ( *iter )->getEventId( ) == streamWaitLeave->getEventId( ) )
      {
        eventNodeList.erase( iter );
        break;
      }
    }
    streamWaitMap[deviceProcId].push_back( streamWaitLeave );
  }
}

/**
 * Get first cuStreamWaitEvent leave node that references the given device stream.
 * 
 * @param deviceStreamId
 * 
 * @return 
 */
EventNode*
AnalysisParadigmCUDA::getFirstStreamWaitEvent( uint64_t deviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find( deviceStreamId );
  
  // no direct streamWaitEvent found, test if one references a NULL stream
  if ( iter == streamWaitMap.end( ) )
  {
    // test if a streamWaitEvent on NULL is not tagged for this device stream
    size_t numAllDevProcs = commonAnalysis->getNumAllDeviceStreams( );
    for ( NullStreamWaitList::iterator nullIter = nullStreamWaits.begin( );
          nullIter != nullStreamWaits.end( ); )
    {
      NullStreamWaitList::iterator currentIter = nullIter;
      StreamWaitTagged* swTagged = *currentIter;
      
      // remove streamWaitEvents that have been tagged by all device streams
      if ( swTagged->tags.size( ) == numAllDevProcs )
      {
        delete( *nullIter );
        nullStreamWaits.erase( nullIter );
      }
      else
      {
        /* if a streamWaitEvent on null stream has not been tagged for
         **/
        /* waitingDeviceProcId yet, return its node */
        if ( swTagged->tags.find( deviceStreamId ) ==
             swTagged->tags.end( ) )
        {
          return swTagged->node;
        }
      }

      ++nullIter;
    }

    return NULL;
  }

  return *( iter->second.begin( ) );
}

EventNode*
AnalysisParadigmCUDA::consumeFirstStreamWaitEvent( uint64_t deviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find(
    deviceStreamId );
  /* no direct streamWaitEvent found, test if one references a NULL
   * stream */
  if ( iter == streamWaitMap.end( ) )
  {
    /* test if a streamWaitEvent on NULL is not tagged for this device
     * stream */
    size_t numAllDevProcs = commonAnalysis->getNumAllDeviceStreams( );
    for ( NullStreamWaitList::iterator nullIter = nullStreamWaits.begin( );
          nullIter != nullStreamWaits.end( ); )
    {
      NullStreamWaitList::iterator currentIter = nullIter;
      StreamWaitTagged* swTagged = *currentIter;
      /* remove streamWaitEvents that have been tagged by all device
       * streams */
      if ( swTagged->tags.size( ) == numAllDevProcs )
      {
        delete( *nullIter );
        nullStreamWaits.erase( nullIter );
      }
      else
      {
        /* if a streamWaitEvent on null stream has not been tagged for
         **/
        /* waitingDeviceProcId yet, tag it and return its node */
        if ( swTagged->tags.find( deviceStreamId ) ==
             swTagged->tags.end( ) )
        {
          swTagged->tags.insert( deviceStreamId );
          return swTagged->node;
        }
      }

      ++nullIter;
    }

    return NULL;
  }

  EventNode* node = *( iter->second.begin( ) );
  iter->second.pop_front( );
  if ( iter->second.size( ) == 0 )
  {
    streamWaitMap.erase( iter );
  }
  return node;
}

void
AnalysisParadigmCUDA::linkEventQuery( EventNode* eventQueryLeave )
{
  EventNode* lastEventQueryLeave = NULL;

  IdEventNodeMap::iterator iter  = eventQueryMap.find(
    eventQueryLeave->getEventId( ) );
  if ( iter != eventQueryMap.end( ) )
  {
    lastEventQueryLeave = iter->second;
  }

  eventQueryLeave->setLink( lastEventQueryLeave );
  eventQueryMap[eventQueryLeave->getEventId( )] = eventQueryLeave;
}

void
AnalysisParadigmCUDA::removeEventQuery( uint64_t eventId )
{
  eventQueryMap.erase( eventId );
}
