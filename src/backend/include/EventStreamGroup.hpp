/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2017
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <vector>
#include <cstddef>
#include "EventStream.hpp"
#include "DeviceStream.hpp"
#include "MpiStream.hpp"

namespace casita
{

  class EventStreamGroup
  {
    public:

      typedef std::vector< EventStream* > EventStreamList;
      typedef std::vector< DeviceStream* > DeviceStreamList;
      typedef std::map< uint64_t, EventStream* > EventStreamMap;

      EventStreamGroup();

      virtual
      ~EventStreamGroup();

      void
      addHostStream( EventStream* p );

      void
      addDeviceStream( DeviceStream* p );

      EventStreamList::iterator
      removeHostStream( EventStream* p );

      void
      setDeviceNullStream( DeviceStream* p );
      
      DeviceStream*
      getDeviceNullStream( int deviceId = -1 );
      
      bool
      deviceWithNullStreamOnly() const;
      
      EventStream*
      getStream( uint64_t id ) const;
     
      const EventStreamList&
      getAllStreams() const;

      void
      getAllStreams( EventStreamList& streams, Paradigm paradigm ) const;

      MpiStream*
      getMpiStream( uint64_t id ) const;
      
      const EventStreamList&
      getHostStreams() const;
      
      DeviceStream*
      getDeviceStream( uint64_t id ) const;

      const DeviceStreamList&
      getDeviceStreams() const;
      
      const DeviceStreamList&
      getDeviceStreams( int deviceId );

      void
      getDeviceStreams( DeviceStreamList& newDeviceStreams ) const;
      
      size_t
      getNumDevices() const;
      
      DeviceStream*
      getFirstDeviceStream( int deviceId );

    private:
      //<! stores stream IDs together with a pointer to the object
      EventStreamMap streamsMap;
     
      EventStreamList  hostStreams;
      DeviceStreamList deviceStreams;
      EventStreamList  allStreams;
      
      //<! initially false, true if only one device stream that is the null stream exists
      bool deviceNullStreamOnly;
      
      // associates device ID and corresponding null stream
      std::map< int, DeviceStream* > deviceNullStreamMap;
      
      std::map< int, DeviceStream* > deviceFirstStreamMap;
      
      //<! collect all device streams per device Id
      std::map< int, DeviceStreamList > deviceIdStreamsMap; 
 };

}
