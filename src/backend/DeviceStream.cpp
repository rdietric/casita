/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

#include "DeviceStream.hpp"

using namespace casita;

DeviceStream::DeviceStream( uint64_t id, 
                            uint64_t parentId, 
                            const std::string name ) :
  EventStream( id, parentId, name, ES_DEVICE ),
  deviceId ( -1 ),
  nativeStreamId ( -1 )
  { 
  
  }

//DeviceStream::~DeviceStream() { }

void
DeviceStream::setDeviceId( int deviceId )
{
  this->deviceId = deviceId;
}

/**
 * Obtain the device ID that has been parsed from the name of the stream. 
 * Return -1 if unknown.
 * 
 * @return the device ID or -1 if unknown
 */
int
DeviceStream::getDeviceId() const
{
  return deviceId;
}

void
DeviceStream::setNativeStreamId( int streamID )
{
  this->nativeStreamId = streamID;
}

int
DeviceStream::getNativeStreamId() const
{
  return nativeStreamId;
}

void
DeviceStream::addPendingKernel( GraphNode* kernelLeave )
{
  pendingKernels.push_back( kernelLeave );
  //std::cerr << "["<< this->id << "] Add pending kernel: " << kernelLeave->getUniqueName() << std::endl;
}

/**
 * Retrieve the last pending kernel leave in the vector.
 * 
 * @return first pending kernel (leave) in the vector
 */
GraphNode*
DeviceStream::getLastPendingKernel()
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
  if ( iter != pendingKernels.rend() )
  {
    return *iter;
  }
  
  return NULL;
}

/**
 * Remove the last pending kernel from the list.
 * 
 * @return the kernel leave node that has been removed
 */
GraphNode*
DeviceStream::consumeLastPendingKernel()
{
  SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
  if ( iter != pendingKernels.rend() )
  {
    GraphNode* result = *iter;
    pendingKernels.pop_back();
    return result;
  }

  return NULL;
}

/**
 * Consume all pending kernels before the given node.
 * 
 * @kernelLeave the kernel leave node
 */
void
DeviceStream::consumePendingKernels( GraphNode* kernelLeave )
{
  if( !kernelLeave )
  {
    UTILS_WARNING( "Cannot consume pending kernels as input node is invalid!" );
    return;
  }
  
  // do nothing, if there are no pending kernels
  if( pendingKernels.empty() )
    return;
  
  // frequent case: kernel is the last one in the list
  GraphNode*  lastKernel = pendingKernels.back();
  if( lastKernel == kernelLeave )
  {
    clearPendingKernels();
  }

  // erase a range of kernels
  SortedGraphNodeList::iterator iterBegin = pendingKernels.begin();
  SortedGraphNodeList::iterator iter = iterBegin;
  while( iter != pendingKernels.end() )
  {
    if( ( *iter ) == kernelLeave )
    {
      break;
    }
      
    ++iter;
  }
  
  pendingKernels.erase( iterBegin, iter );
}

void
DeviceStream::clearPendingKernels()
{
  pendingKernels.clear();
}

void
DeviceStream::setPendingKernelsSyncLink( GraphNode* syncLeave )
{
  for( SortedGraphNodeList::iterator it = pendingKernels.begin();
       it != pendingKernels.end(); ++it )
  {
    (*it)->setLink( syncLeave );
  }
}

/**
 * Reset stream internal data structures.
 * The routine does not touch the list of nodes!!!
 */
void
DeviceStream::reset()
{
  EventStream::reset();
  
  // Check pending (unsynchronized) CUDA kernels
  if( !(this->pendingKernels.empty()) && Parser::getVerboseLevel() > VERBOSE_BASIC )
  {
    UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
               "[%"PRIu64"] %lz pending kernels found!", 
                     this->id, this->pendingKernels.size() );
    
    if( Parser::getVerboseLevel() > VERBOSE_BASIC )
    {
      for( SortedGraphNodeList::const_iterator it = pendingKernels.begin();
           it != pendingKernels.end(); ++it )
      {
        UTILS_MSG( Parser::getVerboseLevel() > VERBOSE_BASIC, 
                   "   %s", ( *it )->getUniqueName().c_str() );
        
        //if( !isKernelPending )
        //pendingKernels.erase(  )
      }
    }
    
    // do not clear pending kernels as they might be required in the following interval
    //clearPendingKernels();
  }
}
