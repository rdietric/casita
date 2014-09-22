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
 */

#include <stdio.h>
#include <mpi.h>
#include <list>
#include <stack>

#include "AnalysisEngine.hpp"
#include "common.hpp"

#if ( ENABLE_OTF1 == 1 )
# include "otf/OTF1ParallelTraceWriter.hpp"
#endif

#if ( ENABLE_OTF2 == 1 )
# include "otf/OTF2ParallelTraceWriter.hpp"
#endif

using namespace casita;
using namespace casita::io;

AnalysisEngine::AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize ) :
  mpiAnalysis( mpiRank, mpiSize ),
  pendingParallelRegion( NULL ),
  maxFunctionId( 0 ),
  waitStateFuncId( 0 )
{
}

AnalysisEngine::~AnalysisEngine( )
{
  for ( std::vector< AbstractRule* >::iterator iter = rules.begin( );
        iter != rules.end( ); ++iter )
  {
    delete( *iter );
  }

  writer->close( );
  delete writer;
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

  return FunctionTable::getAPIFunctionType( name, descr, stream->isDeviceStream(
                                                                               ),
                                            stream->isDeviceNullStream( ) );
}

MPIAnalysis&
AnalysisEngine::getMPIAnalysis( )
{
  return mpiAnalysis;
}

uint32_t
AnalysisEngine::getMPIRank( ) const
{
  return mpiAnalysis.getMPIRank( );
}

#ifdef MPI_CP_MERGE

void
AnalysisEngine::mergeMPIGraphs( )
{
  mpiAnalysis.mergeMPIGraphs( this );
}
#endif

bool
AnalysisEngine::rulePriorityCompare( AbstractRule* r1, AbstractRule* r2 )
{
  /* sort in descending order */
  return r2->getPriority( ) < r1->getPriority( );
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

void
AnalysisEngine::addRule( AbstractRule* rule )
{
  rules.push_back( rule );
  std::sort( rules.begin( ), rules.end( ), rulePriorityCompare );
}

void
AnalysisEngine::removeRules( )
{
  for ( std::vector< AbstractRule* >::iterator iter = rules.begin( );
        iter != rules.end( ); ++iter )
  {
    delete( *iter );
  }
  rules.clear( );
}

bool
AnalysisEngine::applyRules( GraphNode* node, bool verbose )
{
  bool ruleResult = false;
  for ( std::vector< AbstractRule* >::iterator iter = rules.begin( );
        iter != rules.end( ); ++iter )
  {
    if ( ( *iter )->apply( this, node ) )
    {
      if ( verbose )
      {
        printf( "[%u] * Applied %s to %s (%f)\n",
                mpiAnalysis.getMPIRank( ),
                ( *iter )->getName( ),
                node->getUniqueName( ).c_str( ),
                getRealTime( node->getTime( ) ) );
      }

      ruleResult = true;
    }
  }

  return ruleResult;
}

EventStream*
AnalysisEngine::getNullStream( ) const
{
  return streamGroup.getNullStream( );
}

void
AnalysisEngine::setLastEventLaunch( EventNode* eventLaunchLeave )
{
  eventLaunchMap[eventLaunchLeave->getEventId( )] = eventLaunchLeave;
}

EventNode*
AnalysisEngine::consumeLastEventLaunchLeave( uint32_t eventId )
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
AnalysisEngine::getLastEventLaunchLeave( uint32_t eventId ) const
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
AnalysisEngine::setEventProcessId( uint32_t eventId, uint64_t streamId )
{
  eventProcessMap[eventId] = streamId;
}

uint64_t
AnalysisEngine::getEventProcessId( uint32_t eventId ) const
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
AnalysisEngine::addPendingKernelLaunch( GraphNode* launch )
{
  /* append at tail (FIFO) */
  pendingKernelLaunchMap[launch->getReferencedStreamId( )].push_back( launch );
}

GraphNode*
AnalysisEngine::consumePendingKernelLaunch( uint64_t kernelProcessId )
{
  IdNodeListMap::iterator listIter = pendingKernelLaunchMap.find(
    kernelProcessId );
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
  }

  /* found no enter record */
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
AnalysisEngine::addStreamWaitEvent( uint64_t   waitingDeviceProcId,
                                    EventNode* streamWaitLeave )
{
  EventStream* nullStream = getNullStream( );
  if ( nullStream && nullStream->getId( ) == waitingDeviceProcId )
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
      streamWaitMap[waitingDeviceProcId];
    for ( EventNode::EventNodeList::iterator iter = eventNodeList.begin( );
          iter != eventNodeList.end( ); ++iter )
    {
      if ( ( *iter )->getEventId( ) == streamWaitLeave->getEventId( ) )
      {
        eventNodeList.erase( iter );
        break;
      }
    }

    streamWaitMap[waitingDeviceProcId].push_back( streamWaitLeave );
  }
}

EventNode*
AnalysisEngine::getFirstStreamWaitEvent( uint64_t waitingDeviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find( waitingDeviceStreamId );
  /* no direct streamWaitEvent found, test if one references a NULL
   * stream */
  if ( iter == streamWaitMap.end( ) )
  {
    /* test if a streamWaitEvent on NULL is not tagged for this device
     * stream */
    size_t numAllDevProcs = getNumAllDeviceStreams( );
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
        /* waitingDeviceProcId yet, return its node */
        if ( swTagged->tags.find( waitingDeviceStreamId ) == swTagged->tags.end( ) )
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
AnalysisEngine::consumeFirstStreamWaitEvent( uint64_t waitingDeviceStreamId )
{
  IdEventsListMap::iterator iter = streamWaitMap.find( waitingDeviceStreamId );
  /* no direct streamWaitEvent found, test if one references a NULL
   * stream */
  if ( iter == streamWaitMap.end( ) )
  {
    /* test if a streamWaitEvent on NULL is not tagged for this device
     * stream */
    size_t numAllDevProcs = getNumAllDeviceStreams( );
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
        if ( swTagged->tags.find( waitingDeviceStreamId ) == swTagged->tags.end( ) )
        {
          swTagged->tags.insert( waitingDeviceStreamId );
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
AnalysisEngine::linkEventQuery( EventNode* eventQueryLeave )
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
AnalysisEngine::removeEventQuery( uint32_t eventId )
{
  eventQueryMap.erase( eventId );
}

GraphNode*
AnalysisEngine::getLastLaunchLeave( uint64_t timestamp,
                                    uint64_t deviceProcId ) const
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
      if ( ( refDeviceProcessId == deviceProcId ) &&
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
  mpiAnalysis.reset( );
}

size_t
AnalysisEngine::getNumAllDeviceStreams( )
{
  return streamGroup.getNumStreams( ) - streamGroup.getNumHostStreams( );
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
                                              bool        enableWaitStates,
                                              bool        verbose )
{
  EventStreamGroup::EventStreamList allStreams;
  getStreams( allStreams );

  std::sort( allStreams.begin( ), allStreams.end( ), streamSort );

  writer = NULL;
  if ( strstr( origFilename.c_str( ), ".otf2" ) == NULL )
  {
#if ( ENABLE_OTF1 == 1 )
    writer = new OTF1ParallelTraceWriter(
      VT_CUPTI_CUDA_STREAMREF_KEY,
      VT_CUPTI_CUDA_EVENTREF_KEY,
      VT_CUPTI_CUDA_CURESULT_KEY,
      mpiAnalysis.getMPIRank( ),
      mpiAnalysis.getMPISize( ),
      origFilename.c_str( ) );
#endif
  }
  else
  {
#if ( ENABLE_OTF2 == 1 )
    writer = new OTF2ParallelTraceWriter(
      VT_CUPTI_CUDA_STREAMREF_KEY,
      VT_CUPTI_CUDA_EVENTREF_KEY,
      VT_CUPTI_CUDA_CURESULT_KEY,
      mpiAnalysis.getMPIRank( ),
      mpiAnalysis.getMPISize( ),
      mpiAnalysis.getMPICommGroup( 0 ).comm,     /* just get CommWorld
                                                  **/
      origFilename.c_str( ),
      this->ctrTable.getAllCounterIDs( ) );
#endif
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

  /* printf( "[%u] wrote definition \n", mpiAnalysis.getMPIRank( ) ); */

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

    if ( verbose )
    {
      printf( "[%u] def stream %lu (%s)\n",
              mpiAnalysis.getMPIRank( ), p->getId( ), p->getName( ) );
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
      ( *pIter )->getId( ), &nodes, enableWaitStates, pLastGraphNode,
      verbose, &( this->getCtrTable( ) ), &( this->getGraph( ) ) );
  }

  /* printf( "[%u] wrote events \n", mpiAnalysis.getMPIRank( ) ); */

}

GraphNode*
AnalysisEngine::getPendingParallelRegion( )
{
  return pendingParallelRegion;
}

void
AnalysisEngine::setPendingParallelRegion( GraphNode* node )
{
  pendingParallelRegion = node;
}

GraphNode*
AnalysisEngine::getOmpCompute( uint64_t streamId )
{
  return ompComputeTrackMap[streamId];
}

void
AnalysisEngine::setOmpCompute( GraphNode* node, uint64_t streamId )
{
  ompComputeTrackMap[streamId] = node;
}

const GraphNode::GraphNodeList&
AnalysisEngine::getBarrierEventList( bool device, int matchingId )
{
  if ( device )
  {
    return ompBarrierListDevice[matchingId];
  }
  else
  {
    return ompBarrierListHost;
  }
}

void
AnalysisEngine::clearBarrierEventList( bool device, int matchingId )
{
  if ( device )
  {
    ompBarrierListDevice[matchingId].clear( );
  }
  else
  {
    ompBarrierListHost.clear( );
  }
}

void
AnalysisEngine::addBarrierEventToList( GraphNode* node,
                                       bool       device,
                                       int        matchingId )
{
  if ( device )
  {
    ompBarrierListDevice[matchingId].push_back( node );
  }
  else
  {
    ompBarrierListHost.push_back( node );
  }
}

void
AnalysisEngine::setOmpTargetBegin( GraphNode* node )
{
  if ( ompTargetRegionBeginMap.find( node->getStreamId( ) ) !=
       ompTargetRegionBeginMap.end( ) )
  {
    ErrorUtils::getInstance( ).outputMessage(
      "Warning: Replacing nested target region at %s",
      node->getUniqueName( ).c_str( ) );
  }

  ompTargetRegionBeginMap[node->getStreamId( )] = node;
}

GraphNode*
AnalysisEngine::consumeOmpTargetBegin( uint64_t streamId )
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
AnalysisEngine::setOmpTargetFirstEvent( GraphNode* node )
{
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId( ) ) ==
       ompTargetDeviceFirstEventMap.end( ) )
  {
    ompTargetDeviceFirstEventMap[node->getStreamId( )] = node;
  }
}

GraphNode*
AnalysisEngine::consumeOmpTargetFirstEvent( uint64_t streamId )
{
  OmpEventMap::iterator iter = ompTargetDeviceFirstEventMap.find( streamId );
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
AnalysisEngine::setOmpTargetLastEvent( GraphNode* node )
{
  if ( ompTargetDeviceFirstEventMap.find( node->getStreamId( ) ) !=
       ompTargetDeviceFirstEventMap.end( ) )
  {
    ompTargetDeviceLastEventMap[node->getStreamId( )] = node;
  }
}

GraphNode*
AnalysisEngine::consumeOmpTargetLastEvent( uint64_t streamId )
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
AnalysisEngine::pushOmpTargetRegion( GraphNode* node, uint64_t regionId )
{
  ompTargetStreamRegionsMap[node->getStreamId( )].push_back( std::make_pair(
                                                               regionId, node ) );
}

void
AnalysisEngine::popOmpTargetRegion( GraphNode* node )
{
  OmpStreamRegionsMap::iterator iter = ompTargetStreamRegionsMap.find(
    node->getStreamId( ) );
  if ( iter != ompTargetStreamRegionsMap.end( ) )
  {
    iter->second.pop_back( );
  }
}

void
AnalysisEngine::findOmpTargetParentRegion( GraphNode* node,
                                           uint64_t   parentRegionId )
{
  /* search all current stream with parallel region ids */
  for ( OmpStreamRegionsMap::const_iterator esIter =
          ompTargetStreamRegionsMap.begin( );
        esIter != ompTargetStreamRegionsMap.end( ); ++esIter )
  {
    if ( esIter->first != node->getStreamId( ) )
    {
      /* search the current stack of parallel region ids of this
       *stream */
      for ( std::vector< std::pair< uint64_t,
                                    GraphNode* > >::const_iterator rIter =
              esIter->second.begin( );
            rIter != esIter->second.end( ); ++rIter )
      {
        if ( rIter->first == parentRegionId )
        {
          std::cout << "added edge " <<
          newEdge( rIter->second, node, EDGE_NONE )->getName( ) << std::endl;
          return;
        }
      }
    }
  }
}
