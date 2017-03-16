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
      EventStreamGroup( const EventStreamList& hostStreams,
                        const EventStreamList& deviceProcs,
                        EventStream*           nullStream );
      virtual
      ~EventStreamGroup();

      void
      addHostStream( EventStream* p );

      void
      addDeviceStream( EventStream* p );

      EventStreamList::iterator
      removeHostStream( EventStream* p );

      void
      setNullStream( EventStream* p );
     
      const EventStreamList&
      getAllStreams() const;

      void
      getAllStreams( EventStreamList& streams, Paradigm paradigm ) const;

      const EventStreamList&
      getHostStreams() const;

      const EventStreamList&
      getDeviceStreams() const;
     
      void
      getDeviceStreams( int deviceId, EventStreamList& newDeviceStreams ) const;

      void
      getAllDeviceStreams( EventStreamList& newDeviceStreams ) const;

      EventStream*
      getNullStream() const;

      size_t
      getNumStreams() const;

      size_t
      getNumHostStreams() const;

      size_t
      getNumDeviceStreams() const;
      
      EventStream*
      getFirstDeviceStream( int deviceId );

    private:
     
      EventStreamList hostStreams;
      EventStreamList deviceStreams;
      EventStreamList allStreams;
     
      EventStream*    deviceNullStream;
      
      std::map< int, EventStream* > deviceFirstStreamMap;      
 };

}
