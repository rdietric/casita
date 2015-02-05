/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014,
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
#include "cuda/KernelLaunchRule.hpp"
#include "cuda/EventLaunchRule.hpp"
#include "cuda/EventSyncRule.hpp"
#include "cuda/EventQueryRule.hpp"
#include "cuda/StreamWaitRule.hpp"

/* shared with VampirTrace */
#define VT_CUPTI_CUDA_STREAMREF_KEY "CUDA_STREAM_REF_KEY"
#define VT_CUPTI_CUDA_EVENTREF_KEY "CUDA_EVENT_REF_KEY"
#define VT_CUPTI_CUDA_CURESULT_KEY "CUDA_DRV_API_RESULT_KEY"

/* shared with Score-P */
#define SCOREP_CUPTI_CUDA_STREAMREF_KEY "CUDA_STREAM_REF"
#define SCOREP_CUPTI_CUDA_EVENTREF_KEY "CUDA_EVENT_REF"
#define SCOREP_CUPTI_CUDA_CURESULT_KEY "CUDA_DRV_API_RESULT"

using namespace casita;
using namespace casita::cuda;
using namespace casita::io;

AnalysisParadigmCUDA::AnalysisParadigmCUDA( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine )
{
  addRule( new KernelLaunchRule( 9 ) );
  addRule( new BlameSyncRule( 1 ) );
  addRule( new BlameKernelRule( 1 ) );
  addRule( new LateSyncRule( 1 ) );
  addRule( new EventLaunchRule( 1 ) );
  addRule( new EventSyncRule( 1 ) );
  addRule( new EventQueryRule( 1 ) );
  addRule( new StreamWaitRule( 1 ) );
}

AnalysisParadigmCUDA::~AnalysisParadigmCUDA( )
{
  /*    for(IdNodeListMap::iterator iter = pendingKernelLaunchMap.begin(); iter != pendingKernelLaunchMap.end(); */
  /*            iter++) */
  /*    { */
  /*        if(iter->second.size() > 0 ) */
  /*            std::cout << "[" << commonAnalysis->getMPIRank() << "] WARNING: there are " << iter->second.size() << "
   * kernel launches left, but no kernels to call..." << std::endl; */
  /*    } */

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
AnalysisParadigmCUDA::handleKeyValuesEnter( ITraceReader*  reader,
                                            GraphNode*     node,
                                            IKeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = -1;

  streamRefKey = reader->getFirstKey( VT_CUPTI_CUDA_STREAMREF_KEY );
  /* give it another try, maybe it was scorep, not vt */
  if ( streamRefKey < 0 )
  {
    streamRefKey = reader->getFirstKey( SCOREP_CUPTI_CUDA_STREAMREF_KEY );
  }

  if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == IKeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
  }
}

void
AnalysisParadigmCUDA::handleKeyValuesLeave( ITraceReader*  reader,
                                            GraphNode*     node,
                                            GraphNode*     oldNode,
                                            IKeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = -1;

  streamRefKey = reader->getFirstKey( VT_CUPTI_CUDA_STREAMREF_KEY );
  /* give it another try, maybe it was scorep, not vt */
  if ( streamRefKey < 0 )
  {
    streamRefKey = reader->getFirstKey( SCOREP_CUPTI_CUDA_STREAMREF_KEY );
  }

  if ( streamRefKey > -1 && list && list->getSize( ) > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == IKeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
    oldNode->setReferencedStreamId( refValue );
  }
}

void
AnalysisParadigmCUDA::setLastEventLaunch( EventNode* eventLaunchLeave )
{
  eventLaunchMap[eventLaunchLeave->getEventId( )] = eventLaunchLeave;
}

EventNode*
AnalysisParadigmCUDA::consumeLastEventLaunchLeave( uint32_t eventId )
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
AnalysisParadigmCUDA::getLastEventLaunchLeave( uint32_t eventId ) const
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
AnalysisParadigmCUDA::setEventProcessId( uint32_t eventId, uint64_t streamId )
{
  eventProcessMap[eventId] = streamId;
}

uint64_t
AnalysisParadigmCUDA::getEventProcessId( uint32_t eventId ) const
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

void
AnalysisParadigmCUDA::addPendingKernelLaunch( GraphNode* launch )
{
  /* append at tail (FIFO) */
  pendingKernelLaunchMap[launch->getReferencedStreamId( )].push_back(
    launch );
}

GraphNode*
AnalysisParadigmCUDA::consumePendingKernelLaunch( uint64_t kernelStreamId )
{
  IdNodeListMap::iterator listIter = pendingKernelLaunchMap.find(
    kernelStreamId );
  if ( listIter == pendingKernelLaunchMap.end( ) )
  {
    return NULL;
  }

  if ( listIter->second.size( ) == 0 )
  {
    return NULL;
  }

  /* consume from head (FIFO) */
  /* listIter->second contains enter and leave records */
  GraphNode::GraphNodeList::iterator launchIter = listIter->second.begin( );
  while ( ( launchIter != listIter->second.end( ) ) &&
          ( ( *launchIter )->isLeave( ) ) )
  {
    launchIter++;
    /* found no enter record */
  }
  if ( launchIter == listIter->second.end( ) )
  {
    return NULL;
  }

  /* erase this enter record */
  GraphNode* kernelLaunch = *launchIter;
  listIter->second.erase( launchIter );
  return kernelLaunch;
}

void
AnalysisParadigmCUDA::addStreamWaitEvent( uint64_t   deviceProcId,
                                          EventNode* streamWaitLeave )
{
  EventStream* nullStream = commonAnalysis->getNullStream( );
  if ( nullStream && nullStream->getId( ) == deviceProcId )
  {
    StreamWaitTagged* swTagged = new StreamWaitTagged( );
    swTagged->node = streamWaitLeave;
    nullStreamWaits.push_front( swTagged );
  }
  else
  {
    /* Remove any pending streamWaitEvent with the same event ID since
     * they */
    /* it is replaced by this new streamWaitLeave. */
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

EventNode*
AnalysisParadigmCUDA::getFirstStreamWaitEvent( uint64_t deviceStreamId )
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
        delete ( *nullIter );
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
        delete ( *nullIter );
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
AnalysisParadigmCUDA::removeEventQuery( uint32_t eventId )
{
  eventQueryMap.erase( eventId );
}

GraphNode*
AnalysisParadigmCUDA::getLastLaunchLeave( uint64_t timestamp,
                                          uint64_t deviceStreamId ) const
{
  /* find last kernel launch (leave record) which launched on */
  /* deviceProcId and happened before timestamp */
  GraphNode* lastLaunchLeave = NULL;

  for ( IdNodeListMap::const_iterator listIter =
          pendingKernelLaunchMap.begin( );
        listIter != pendingKernelLaunchMap.end( ); ++listIter )
  {
    for ( GraphNode::GraphNodeList::const_reverse_iterator launchIter =
            listIter->second.rbegin( );
          launchIter != listIter->second.rend( ); ++launchIter )
    {
      GraphNode* gLaunchLeave     = *launchIter;

      if ( gLaunchLeave->isEnter( ) )
      {
        continue;
      }

      uint64_t refDeviceProcessId =
        gLaunchLeave->getGraphPair( ).first->getReferencedStreamId( );

      /* found the last kernel launch (leave) on this stream, break
       **/
      if ( ( refDeviceProcessId == deviceStreamId ) &&
           ( gLaunchLeave->getTime( ) <= timestamp ) )
      {
        /* if this is the latest kernel launch leave so far, remember
         * it */
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
