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

#pragma once

#include "EventStream.hpp"

namespace casita
{

  class DeviceStream : public EventStream
  {
    public:
      DeviceStream( uint64_t id, uint64_t parentId, const std::string name );
      //virtual ~DeviceStream();
      
      void
      reset();
      
      void
      setDeviceId( int deviceId );
     
      int
      getDeviceId( void ) const;

      void
      setNativeStreamId( int streamId );

      int
      getNativeStreamId( void ) const;
      
      void
      addPendingKernel( GraphNode* kernelLeave );

      GraphNode*
      getLastPendingKernel();

      GraphNode*
      consumeLastPendingKernel();
     
      /**
       * Consume all pending kernels before the given node.
       */
      void
      consumePendingKernels( GraphNode* kernelEnter );
     
      void
      setPendingKernelsSyncLink( GraphNode* syncLeave );

      void
      clearPendingKernels();

    private:
      //!< device ID parsed from stream name, -1 if unknown
      int deviceId;
     
      //!< native stream ID (only CUDA)
      int nativeStreamId;
      
      //!< list of unsynchronized CUDA kernels (leave nodes only)
      SortedGraphNodeList pendingKernels;

  };
  
}