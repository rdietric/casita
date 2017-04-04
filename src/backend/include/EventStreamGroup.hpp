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

namespace casita
{

  class EventStreamGroup
  {
    public:

      typedef std::vector< EventStream* > EventStreamList;

      EventStreamGroup();

      virtual
      ~EventStreamGroup();

      void
      addHostStream( EventStream* p );

      void
      addDeviceStream( EventStream* p );

      EventStreamList::iterator
      removeHostStream( EventStream* p );

      void
      setDeviceNullStream( EventStream* p );
      
      bool
      deviceWithNullStreamOnly() const;
     
      const EventStreamList&
      getAllStreams() const;

      void
      getAllStreams( EventStreamList& streams, Paradigm paradigm ) const;

      const EventStreamList&
      getHostStreams() const;

      const EventStreamList&
      getDeviceStreams() const;
      
      const EventStreamList&
      getDeviceStreams( int deviceId );

      void
      getAllDeviceStreams( EventStreamList& newDeviceStreams ) const;

      EventStream*
      getNullStream( int deviceId = -1 );

      size_t
      getNumStreams() const;
      
      EventStream*
      getFirstDeviceStream( int deviceId );

    private:
     
      EventStreamList hostStreams;
      EventStreamList deviceStreams;
      EventStreamList allStreams;
      
      //<! initially false, true if only one device stream that is the null stream exists
      bool deviceNullStreamOnly;
      
      // associates device ID and corresponding null stream
      std::map< int, EventStream* > deviceNullStreamMap;
      
      std::map< int, EventStream* > deviceFirstStreamMap;
      
      //<! collect all device streams per device Id
      std::map< int, EventStreamList > deviceIdStreamsMap; 
 };

}
