/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017-2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * - perform analysis with Offload-specific rules
 * - add kernels as pending, consume pending kernels
 * - Callbacks for KeyValue, postEnter/leave
 *
 */

#include "offload/AnalysisParadigmOffload.hpp"
#include "AnalysisEngine.hpp"

#include "offload/SyncRule.hpp"
#include "offload/KernelExecutionRule.hpp"
#include "offload/cuda/EventLaunchRule.hpp"
#include "offload/cuda/EventSyncRule.hpp"
#include "offload/cuda/EventQueryRule.hpp"
#include "offload/cuda/StreamWaitRule.hpp"

using namespace casita;
using namespace casita::offload;
using namespace casita::io;

AnalysisParadigmOffload::AnalysisParadigmOffload( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine ),
  pendingKernels( 0 ) 
{
  // triggered on offload kernel leave
  addRule( new KernelExecutionRule( 9 ) );
  
  // note: rule clears the list of pending kernels when finished
  addRule( new SyncRule( 1 ) ); // triggered on cudaSync and clFinish
  
  // add rules that are related to CUDA events only if necessary
  if( analysisEngine->haveAnalysisFeature( CUDA_EVENTS ) )
  {
    addRule( new EventLaunchRule( 1 ) );
    addRule( new EventSyncRule( 1 ) );
    addRule( new EventQueryRule( 1 ) );
    addRule( new StreamWaitRule( 1 ) );
  }
}

AnalysisParadigmOffload::~AnalysisParadigmOffload()
{
  reset();
}

void
AnalysisParadigmOffload::reset()
{  
  UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
             "Cleanup Offload support structures" );
  
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
//          UTILS_OUT( "#### %s",  commonAnalysis->getNodeInfo(*it).c_str() );
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
AnalysisParadigmOffload::getParadigm()
{
  return PARADIGM_OFFLOAD;
}

void
AnalysisParadigmOffload::handlePostEnter( GraphNode* enterNode )
{
  if( NULL == enterNode )
  {
    UTILS_WARNING( "Offload enter node == NULL!" );
    return;
  }
  
  if ( enterNode->isOffloadEnqueueKernel() )
  {
    addPendingKernelLaunch( enterNode );
    pendingKernels++;
  }
}

void
AnalysisParadigmOffload::handlePostLeave( GraphNode* leaveNode )
{
  if( NULL == leaveNode )
  {
    UTILS_WARNING( "Offload leave node == NULL!" );
    return;
  }
  
  if ( leaveNode->isOffloadEnqueueKernel() )
  {
    addPendingKernelLaunch( leaveNode );
  }
  else if( leaveNode->isOffloadKernel() )
  {
    pendingKernels--;
  }
}

void
AnalysisParadigmOffload::handleKeyValuesEnter( OTF2TraceReader*  reader,
                                               GraphNode*        enterNode,
                                               OTF2KeyValueList* list )
{
  uint64_t refValue       = 0;
  int32_t  locationRefKey = -1;
  
  if( enterNode->isCUDA() )
  {
    locationRefKey = reader->getFirstKey( SCOREP_CUDA_STREAMREF );
  }
  else if( enterNode->isOpenCL() )
  {
    locationRefKey = reader->getFirstKey( SCOREP_OPENCL_QUEUEREF );
  }

  /*if( locationRefKey > -1 && list->getLocationRef( (uint32_t)locationRefKey,
                             &refValue ) != OTF2KeyValueList::KV_SUCCESS ){
    std::cerr << "Offload::handleKeyValuesEnter locationRefKey: " << locationRefKey << std::endl;
  }*/

  if ( locationRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)locationRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    enterNode->setReferencedStreamId( refValue );
  }
}

/**
 * Set the referenced stream for both given nodes (leave and enter).
 * 
 * @param reader
 * @param leaveNode
 * @param enterNode
 * @param list
 */
void
AnalysisParadigmOffload::handleKeyValuesLeave( OTF2TraceReader*  reader,
                                               GraphNode*        leaveNode,
                                               GraphNode*        enterNode,
                                               OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  locationRefKey = -1;
  
  if( enterNode->isCUDA() )
  {
    locationRefKey = reader->getFirstKey( SCOREP_CUDA_STREAMREF );
  }
  else if( enterNode->isOpenCL() )
  {
    locationRefKey = reader->getFirstKey( SCOREP_OPENCL_QUEUEREF );
  }
  
  /*if( locationRefKey > -1 && list->getLocationRef( (uint32_t)locationRefKey,
                             &refValue ) != OTF2KeyValueList::KV_SUCCESS ){
    std::cerr << "Offload::handleKeyValuesLeave locationRefKey: " << locationRefKey << std::endl;
  }*/

  if ( locationRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)locationRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    // the device synchronization leave nodes should reference a stream
    //UTILS_WARNING( "%s references %llu", leaveNode->getUniqueName().c_str(), refValue );
    leaveNode->setReferencedStreamId( refValue );
    enterNode->setReferencedStreamId( refValue );
  }
}

size_t
AnalysisParadigmOffload::getPendingKernelCount() const
{
  return pendingKernels;
}


//////////////////////////////////////////////////////////////
////////////// Offload rules support functions //////////////////

// \todo: could be static
bool
AnalysisParadigmOffload::isKernelPending( GraphNode* kernelNode )
{
  if( kernelNode->hasPartner() )
  {
    // kernel leave has not yet been synchronized (compare BlameKernelRule)
    if( kernelNode->getGraphPair().second->getLink() == NULL )
    {
      UTILS_MSG_ONCE_OR( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                 "[%"PRIu64"] Do not delete unsynchronized kernel %s", 
                 kernelNode->getStreamId(), 
                 this->commonAnalysis->getNodeInfo( kernelNode ).c_str() );
      return true;
    }
  }
  // enter kernel nodes without partner must NOT be deleted
  else if( kernelNode->isEnter() )
  {
    UTILS_MSG_ONCE_OR( Parser::getVerboseLevel() > VERBOSE_BASIC, 
               "[%"PRIu64"] Do not delete incomplete kernel %s", 
               kernelNode->getStreamId(), 
               this->commonAnalysis->getNodeInfo( kernelNode ).c_str() );
    return true;
  }
  
  return false;
}

void
AnalysisParadigmOffload::setLastEventLaunch( EventNode* eventLaunchLeave )
{
  eventLaunchMap[eventLaunchLeave->getEventId()] = eventLaunchLeave;
}

EventNode*
AnalysisParadigmOffload::consumeLastEventLaunchLeave( uint64_t eventId )
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
AnalysisParadigmOffload::getEventRecordLeave( uint64_t eventId ) const
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
AnalysisParadigmOffload::printKernelLaunchMap()
{
  uint64_t pendingKernelLaunchCount = 0;
  for ( IdNodeListMap::const_iterator mapIter =
          pendingKernelLaunchMap.begin();
        mapIter != pendingKernelLaunchMap.end(); ++mapIter )
  {
    pendingKernelLaunchCount += mapIter->second.size();
    
    if( mapIter->second.size() )
    {
      UTILS_OUT( "[%"PRIu32"] %llu pending kernel launches for device stream %"PRIu64,
                 commonAnalysis->getMPIRank(), mapIter->second.size(), mapIter->first );
      
      EventStream* evtStream = this->commonAnalysis->getStream( mapIter->first );
      if( evtStream )
      {
        UTILS_OUT( "  ... with stream name: %s", evtStream->getName() );
      }
      else
      {
        UTILS_OUT( "  ... with missing stream object" );
      }
      
      for( GraphNode::GraphNodeList::const_iterator itList = mapIter->second.begin();
           itList != mapIter->second.end(); ++itList )
      {
        UTILS_WARNING( "[%"PRIu32"] Pending kernel launch %s",
                       commonAnalysis->getMPIRank(),
                       commonAnalysis->getNodeInfo( *itList ).c_str() );
      }
      
    }
  }
  UTILS_OUT( "[%"PRIu32"] %"PRIu64" pending kernel launches on %llu different device streams",
             commonAnalysis->getMPIRank(),pendingKernelLaunchCount, pendingKernelLaunchMap.size() );
}

void
AnalysisParadigmOffload::printDebugInformation( uint64_t eventId )
{
  UTILS_OUT( "Passed event id: %llu", eventId );
  
  UTILS_OUT( "Number of stored event IDs with corresponding last event record leave node: %llu",
             eventLaunchMap.size() );
  
    EventNode* eventRecordLeave = eventLaunchMap[eventId];
  
  uint64_t streamId = eventRecordLeave->getStreamId( );
  
  UTILS_OUT( "Host stream: %llu (%s)", streamId,
             this->commonAnalysis->getStream( streamId )->getName() );
  
  printKernelLaunchMap();
}

void
AnalysisParadigmOffload::setEventProcessId( uint64_t eventId, uint64_t streamId )
{
  eventProcessMap[eventId] = streamId;
}

uint64_t
AnalysisParadigmOffload::getEventProcessId( uint64_t eventId ) const
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
AnalysisParadigmOffload::addPendingKernelLaunch( GraphNode* launch )
{
  // append at tail (FIFO)
  pendingKernelLaunchMap[ launch->getGraphPair().first->getReferencedStreamId() ]
    .push_back( launch );
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
AnalysisParadigmOffload::consumeFirstPendingKernelLaunchEnter( uint64_t kernelStreamId )
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
  GraphNode::GraphNodeList::iterator launchIter = mapIter->second.begin();
  
  // skip leading leave nodes, as only enter nodes are erased
  while ( ( launchIter != mapIter->second.end() ) &&
          ( ( *launchIter )->isLeave() ) )
  {
    launchIter++;
  }
  
  if ( launchIter == mapIter->second.end() )
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
AnalysisParadigmOffload::getLastKernelLaunchLeave( uint64_t timestamp,
                                                   uint64_t deviceStreamId ) const
{
  GraphNode* lastLaunchLeave = NULL;

  // iterate over all streams with pending kernels
  for ( IdNodeListMap::const_iterator listIter =
          pendingKernelLaunchMap.begin();
        listIter != pendingKernelLaunchMap.end(); ++listIter )
  {
    // revers-iterate over kernel launch nodes
    for ( GraphNode::GraphNodeList::const_reverse_iterator launchIter =
            listIter->second.rbegin();
          launchIter != listIter->second.rend(); ++launchIter )
    {
      GraphNode* gLaunchLeave = *launchIter;

      // ignore enter nodes
      if ( gLaunchLeave->isEnter() )
      {
        continue;
      }

      // get the references device stream using the kernel launch enter node
      uint64_t refDeviceProcessId =
        gLaunchLeave->getGraphPair().first->getReferencedStreamId();

      // found the last kernel launch (leave) on this stream, break
      if ( ( refDeviceProcessId == deviceStreamId ) &&
           ( gLaunchLeave->getTime() <= timestamp ) )
      {
        // if this is the latest kernel launch leave so far, remember it
        if ( !lastLaunchLeave ||
             ( gLaunchLeave->getTime() > lastLaunchLeave->getTime() ) )
        {
          lastLaunchLeave = gLaunchLeave;
        }
        break;
      }
    }
  }
  
  return lastLaunchLeave;
}

/**
 * Remove a kernel launch from the map (key is stream id) of kernel launch vectors.
 * 
 * @param kernel kernel enter node
 */
void
AnalysisParadigmOffload::removeKernelLaunch( GraphNode* kernel )
{
  GraphNode* kernelLaunchEnter = ( GraphNode* )kernel->getLink();
  
  if( !kernelLaunchEnter )
  {
    return;
  }
  
  uint64_t streamId = kernel->getStreamId();

  if( pendingKernelLaunchMap.count( streamId ) > 0 )
  {  
    if( pendingKernelLaunchMap[ streamId ].size() > 0 )
    {
      GraphNode* kernelLaunchLeave = kernelLaunchEnter->getGraphPair().second;
      pendingKernelLaunchMap[ streamId ].remove( kernelLaunchLeave );

      UTILS_MSG_IF_ONCE( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                         Parser::getVerboseLevel() > VERBOSE_TIME,
                 "[%"PRIu32"] Removed %s referencing %"PRIu64" from kernel "
                 "launch map (new list size %llu)", 
                 commonAnalysis->getMPIRank(),
                 commonAnalysis->getNodeInfo( kernelLaunchLeave ).c_str(),
                 streamId,
                 pendingKernelLaunchMap[ streamId ].size() );
    }
  }
}

/**
 * Clear the list of pending Offload kernel launches for a give stream ID.
 * 
 * @param streamId
 */
void
AnalysisParadigmOffload::clearKernelEnqueues( uint64_t streamId )
{
  if( pendingKernelLaunchMap.count( streamId ) > 0 )
  {  
    if( pendingKernelLaunchMap[ streamId ].size() > 0 )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
        "[%"PRIu32"] Clear list of %llu pending kernel launches for stream %"PRIu64, 
        commonAnalysis->getMPIRank(), 
        (unsigned long long)pendingKernelLaunchMap[ streamId ].size(), streamId );
      
      pendingKernelLaunchMap[ streamId ].clear();
    }
  }
}

void
AnalysisParadigmOffload::addStreamWaitEvent( uint64_t   streamId,
                                             EventNode* streamWaitLeave )
{
  const DeviceStream* strm = 
    commonAnalysis->getStreamGroup().getDeviceStream( streamId );
  
  if( !strm )
  {
    return;
  }
  
  const DeviceStream* nullStream = 
    commonAnalysis->getStreamGroup().getDeviceNullStream( strm->getDeviceId() );
  
  if ( nullStream && nullStream->getId() == streamId )
  {
    StreamWaitTagged* swTagged = new StreamWaitTagged();
    swTagged->node = streamWaitLeave;
    nullStreamWaits.push_front( swTagged );
  }
  else
  {
    // Remove any pending streamWaitEvent with the same event ID since
    // it is replaced by this new streamWaitLeave.
    EventNode::EventNodeList& eventNodeList =
      streamWaitMap[ streamId ];
    for ( EventNode::EventNodeList::iterator iter = eventNodeList.begin();
          iter != eventNodeList.end(); ++iter )
    {
      if ( ( *iter )->getEventId() == streamWaitLeave->getEventId() )
      {
        eventNodeList.erase( iter );
        break;
      }
    }
    streamWaitMap[streamId].push_back( streamWaitLeave );
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
AnalysisParadigmOffload::getFirstStreamWaitEvent( uint64_t deviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find( deviceStreamId );
  
  // no direct streamWaitEvent found, test if one references a NULL stream
  if ( iter == streamWaitMap.end( ) )
  {
    // test if a streamWaitEvent on NULL is not tagged for this device stream
    size_t numAllDevProcs = commonAnalysis->getNumDeviceStreams( );
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
AnalysisParadigmOffload::consumeFirstStreamWaitEvent( uint64_t deviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find(
    deviceStreamId );
  /* no direct streamWaitEvent found, test if one references a NULL
   * stream */
  if ( iter == streamWaitMap.end( ) )
  {
    /* test if a streamWaitEvent on NULL is not tagged for this device
     * stream */
    size_t numAllDevProcs = commonAnalysis->getNumDeviceStreams();
    for ( NullStreamWaitList::iterator nullIter = nullStreamWaits.begin();
          nullIter != nullStreamWaits.end(); )
    {
      NullStreamWaitList::iterator currentIter = nullIter;
      StreamWaitTagged* swTagged = *currentIter;
      /* remove streamWaitEvents that have been tagged by all device
       * streams */
      if ( swTagged->tags.size() == numAllDevProcs )
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
AnalysisParadigmOffload::linkEventQuery( EventNode* eventQueryLeave )
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
AnalysisParadigmOffload::removeEventQuery( uint64_t eventId )
{
  eventQueryMap.erase( eventId );
}

/**
 * Create dependency edges from the given kernel enter to preceding kernels in
 * other device streams.
 * 
 * @param kernelEnter
 * @param prevKernelLeave
 */
void
AnalysisParadigmOffload::createKernelDependencies( GraphNode* kernelEnter ) const
{
  if( Parser::getInstance().getProgramOptions().linkKernels == false )
  {
    return;
  }
  
  // if no kernel is given, assume a global device synchronization
  if( kernelEnter == NULL )
  {
    const EventStreamGroup::DeviceStreamList& deviceStreams = 
      commonAnalysis->getDeviceStreams();
  
    if( deviceStreams.size() == 0 )
    {
      UTILS_WARNING( "Cannot sync without device streams." );
      return;
    }

    // find last kernel leave
    EventStreamGroup::DeviceStreamList::const_iterator streamIt = deviceStreams.begin();
    for( streamIt = deviceStreams.begin(); deviceStreams.end() != streamIt; ++streamIt )
    {
      DeviceStream* devStrm = *streamIt;
      GraphNode* lastKernelLeave = devStrm->getLastPendingKernel();
      if( !kernelEnter || 
         ( lastKernelLeave && Node::compareLess( kernelEnter, lastKernelLeave ) ) )
      {
        kernelEnter = lastKernelLeave;
      }
    }
  }
  
  if( NULL == kernelEnter )
  {
    return;
  }
  else
  {
    kernelEnter = kernelEnter->getGraphPair().first;
  }
  
  if( NULL == kernelEnter )
  {
    return;
  }
  
  // kernel enter found ...
  
  //UTILS_OUT( "Create kernel dependency edges from %s", 
  //                 commonAnalysis->getNodeInfo( kernelEnter ).c_str() );
  
  GraphNode* kernelLaunchEnter = ( GraphNode* ) kernelEnter->getLink();
  //GraphNode* prevKernelEnter = NULL;
  
  while( true )
  {
    // get a preceding kernel via link left
    GraphNode* prevKernelEnter = kernelEnter->getLinkLeft();
    if( !prevKernelEnter )
    {
      // create edge to kernelLaunch if necessary
      if( kernelEnter->getLink() != kernelLaunchEnter )
      {
        commonAnalysis->newEdge( kernelLaunchEnter, kernelEnter );
      }

      break;
    }
    
    prevKernelEnter = kernelEnter->getLinkLeft()->getGraphPair().first;
    if( !prevKernelEnter )
    {
      break;
    }
    
    if( prevKernelEnter->getTime() > kernelLaunchEnter->getTime() )
    {
      // create dependency edge
      //UTILS_WARNING( "Create edge between kernels: %s -> %s", 
      //               commonAnalysis->getNodeInfo( prevKernelEnter->getGraphPair().second ).c_str(),
      //               commonAnalysis->getNodeInfo( kernelEnter ).c_str());
      commonAnalysis->newEdge( prevKernelEnter->getGraphPair().second, kernelEnter );
    }
    else
    {
      commonAnalysis->newEdge( kernelLaunchEnter, kernelEnter );
      break;
    }
    
    kernelEnter = prevKernelEnter;
  }
}
