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

     EventStreamGroup( );
     EventStreamGroup( uint64_t start,
                       const EventStreamList& hostStreams,
                       const EventStreamList& deviceProcs,
                       EventStream* nullStream );
     virtual
     ~EventStreamGroup( );

     void
     addHostStream( EventStream* p );

     void
     addDeviceStream( EventStream* p );

     EventStreamList::iterator
     removeHostStream( EventStream* p );

     void
     setNullStream( EventStream* p );

     void
     getAllStreams( EventStreamList& streams ) const;

     void
     getAllStreams( EventStreamList& streams, Paradigm paradigm ) const;

     const EventStreamList&
     getHostStreams( ) const;

     const EventStreamList&
     getDeviceStreams( ) const;

     void
     getAllDeviceStreams( EventStreamList& newDeviceStreams ) const;

     EventStream*
     getNullStream( ) const;

     size_t
     getNumStreams( ) const;

     size_t
     getNumHostStreams( ) const;

     size_t
     getNumDeviceStreams( ) const;

     uint64_t
     getStartTime( ) const;

   private:
     uint64_t startTime;
     EventStreamList hostStreams;
     EventStreamList deviceStreams;
     EventStream* nullStream;
 };

}
