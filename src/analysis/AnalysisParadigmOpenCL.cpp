/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016, 2017
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

#include "opencl/AnalysisParadigmOpenCL.hpp"
#include "AnalysisEngine.hpp"

#include "opencl/SyncRule.hpp"
#include "opencl/KernelExecutionRule.hpp"

using namespace casita;
using namespace casita::opencl;
using namespace casita::io;

AnalysisParadigmOpenCL::AnalysisParadigmOpenCL( AnalysisEngine* analysisEngine ) :
  IAnalysisParadigm( analysisEngine )
{
  addRule( new KernelExecutionRule( 9 ) );
  addRule( new SyncRule( 2 ) ); // triggered on clFinish
}

AnalysisParadigmOpenCL::~AnalysisParadigmOpenCL()
{

}

Paradigm
AnalysisParadigmOpenCL::getParadigm()
{
  return PARADIGM_OCL;
}

void
AnalysisParadigmOpenCL::handlePostEnter( GraphNode* node )
{
  if ( node->isOpenCLKernelEnqueue() )
  {
    addPendingKernelEnqueue( node );
  }
}

void
AnalysisParadigmOpenCL::handlePostLeave( GraphNode* node )
{
  if ( node->isOpenCLKernelEnqueue() )
  {
    addPendingKernelEnqueue( node );
  }
}

void
AnalysisParadigmOpenCL::handleKeyValuesEnter( OTF2TraceReader* reader,
                                            GraphNode*         node,
                                            OTF2KeyValueList*  list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_OPENCL_QUEUEREF );

  if ( streamRefKey > -1 && list && list->getSize() > 0 &&
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
AnalysisParadigmOpenCL::handleKeyValuesLeave( OTF2TraceReader* reader,
                                              GraphNode*       node,
                                              GraphNode*       oldNode,
                                              OTF2KeyValueList* list )
{
  uint64_t refValue     = 0;
  int32_t  streamRefKey = reader->getFirstKey( SCOREP_OPENCL_QUEUEREF );

  if ( streamRefKey > -1 && list && list->getSize() > 0 &&
       list->getLocationRef( (uint32_t)streamRefKey,
                             &refValue ) == OTF2KeyValueList::KV_SUCCESS )
  {
    node->setReferencedStreamId( refValue );
    oldNode->setReferencedStreamId( refValue );
    
    // if the enqueue buffer operation has an OTF2 attribute, it is blocking
    if( node->getType() == OCL_ENQUEUE_BUFFER )
    {
      node->addType( OCL_SYNC_QUEUE );
    }
  }
}


//////////////////////////////////////////////////////////////
////////////// OpenCL rules support functions //////////////////

void
AnalysisParadigmOpenCL::setEventProcessId( uint64_t eventId, uint64_t streamId )
{
  eventProcessMap[eventId] = streamId;
}

uint64_t
AnalysisParadigmOpenCL::getEventProcessId( uint64_t eventId ) const
{
  IdIdMap::const_iterator iter = eventProcessMap.find( eventId );
  if ( iter != eventProcessMap.end() )
  {
    return iter->second;
  }
  else
  {
    return 0;
  }
}

bool
AnalysisParadigmOpenCL::isKernelPending( GraphNode* kernelNode ) const
{
  if( kernelNode->hasPartner() )
  {
    // kernel leave has not yet been synchronized (compare BlameKernelRule)
    if( kernelNode->getGraphPair().second->getLink() == NULL )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
                 "[%"PRIu64"] Do not delete unsynchronized kernel %s", 
                 kernelNode->getStreamId(), 
                 this->commonAnalysis->getNodeInfo( kernelNode ).c_str() );
      return true;
    }
  }
  // enter kernel nodes without partner must NOT be deleted
  else if( kernelNode->isEnter() )
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "[%"PRIu64"] Do not delete incomplete kernel %s", 
               kernelNode->getStreamId(), 
               this->commonAnalysis->getNodeInfo( kernelNode ).c_str() );
    return true;
  }
  
  return false;
}

/**
 * Adds kernel launch event nodes at the end of the list.
 * 
 * @param launch a kernel launch leave or enter node
 */
void
AnalysisParadigmOpenCL::addPendingKernelEnqueue( GraphNode* launch )
{
  // append at tail (FIFO)
  pendingKernelEnqueueMap[launch->getReferencedStreamId()].push_back(
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
AnalysisParadigmOpenCL::consumeFirstPendingKernelEnqueueEnter( uint64_t kernelStreamId )
{
  IdNodeListMap::iterator mapIter = 
    pendingKernelEnqueueMap.find( kernelStreamId );
  
  // return NULL, if the element could not be found
  if ( mapIter == pendingKernelEnqueueMap.end() )
  {
    return NULL;
  }

  // return NULL, if the list of pending kernel launch events is empty
  if ( mapIter->second.size() == 0 )
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
 * 
 * @param timestamp 
 * @param deviceStreamId
 * 
 * @return
 */
GraphNode*
AnalysisParadigmOpenCL::getLastEnqueueLeave( uint64_t timestamp,
                                             uint64_t deviceStreamId ) const
{
  GraphNode* lastLaunchLeave = NULL;

  for ( IdNodeListMap::const_iterator listIter =
          pendingKernelEnqueueMap.begin();
        listIter != pendingKernelEnqueueMap.end(); ++listIter )
  {
    for ( GraphNode::GraphNodeList::const_reverse_iterator launchIter =
            listIter->second.rbegin();
          launchIter != listIter->second.rend(); ++launchIter )
    {
      GraphNode* gLaunchLeave     = *launchIter;

      if ( gLaunchLeave->isEnter() )
      {
        continue;
      }

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
AnalysisParadigmOpenCL::removeKernelLaunch( GraphNode* kernel )
{
  GraphNode* kernelLaunchEnter = ( GraphNode* )kernel->getLink();
  
  if( !kernelLaunchEnter )
  {
    return;
  }
  
  uint64_t streamId = kernel->getStreamId();

  if( pendingKernelEnqueueMap.count( streamId ) > 0 )
  {  
    if( pendingKernelEnqueueMap[ streamId ].size() > 0 )
    {
      GraphNode* kernelLaunchLeave = kernelLaunchEnter->getGraphPair().second;
      pendingKernelEnqueueMap[ streamId ].remove( kernelLaunchLeave );

      UTILS_WARNING( "[%"PRIu32"] Removed %s referencing %"PRIu64" from kernel "
                     "launch map (new list size %llu)", 
                     commonAnalysis->getMPIRank(),
                     commonAnalysis->getNodeInfo( kernelLaunchLeave ).c_str(),
                     streamId,
                     pendingKernelEnqueueMap[ streamId ].size() );
    }
  }
}

/**
 * Clear the list of pending OpenCL kernel launches for a give stream ID.
 * 
 * @param streamId
 */
void
AnalysisParadigmOpenCL::clearKernelEnqueues( uint64_t streamId )
{
  if( pendingKernelEnqueueMap.count( streamId ) > 0 )
  {  
    if( pendingKernelEnqueueMap[ streamId ].size() > 0 )
    {
      UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
        "[%"PRIu32"] Clear list of %llu pending kernel launches for stream %"PRIu64, 
        commonAnalysis->getMPIRank(), 
        (unsigned long long)pendingKernelEnqueueMap[ streamId ].size(), streamId );
      
      pendingKernelEnqueueMap[ streamId ].clear();
    }
  }
}
