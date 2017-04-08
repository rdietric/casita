/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013, 2014, 2016, 2017
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * - Combine streams for one process
 *
 */

#include <algorithm>

#include "EventStreamGroup.hpp"

using namespace casita;

EventStreamGroup::EventStreamGroup() :
 deviceNullStreamOnly ( false )
{

}

EventStreamGroup::~EventStreamGroup()
{

}

void
EventStreamGroup::addHostStream( EventStream* p )
{
  hostStreams.push_back( p );
  allStreams.push_back( p );
}

void
EventStreamGroup::addDeviceStream( EventStream* p )
{
  deviceStreams.push_back( p );
  allStreams.push_back( p );
  
  // generate a map with per device streams
  deviceIdStreamsMap[ p->getDeviceId() ].push_back( p );;
}

EventStreamGroup::EventStreamList::iterator
EventStreamGroup::removeHostStream( EventStream* p )
{
  for ( EventStreamList::iterator iter = hostStreams.begin();
        iter != hostStreams.end(); ++iter )
  {
    if ( *iter == p )
    {
      return hostStreams.erase( iter );
    }
  }
  return hostStreams.end();
}

/**
 * Set the given stream as null stream.
 * 
 * @param streamId ID of the event stream.
 */
void
EventStreamGroup::setDeviceNullStream( EventStream* stream )
{
  if( stream == NULL )
  {
    UTILS_WARNING( "Cannot set null stream" );
    return;
  }
  
  stream->setStreamType( EventStream::ES_DEVICE_NULL );
  
  deviceNullStreamMap[ stream->getDeviceId() ] = stream;
  
  // this assumes that the location property is read after all location definitions
  // and hence the total number of device streams is known here
  if( getDeviceStreams().size() == 1 )
  {
    deviceNullStreamOnly = true;
    
    //UTILS_WARN_ONCE( "There is only one device stream, which is the null stream." );
  }
}

bool
EventStreamGroup::deviceWithNullStreamOnly() const
{
  return deviceNullStreamOnly;
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getAllStreams() const
{
  return allStreams;
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getDeviceStreams() const
{
  return deviceStreams;
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getHostStreams() const
{
  return hostStreams;
}

/**
 * Get list of event streams for a given device ID.
 * 
 * @param deviceId
 * @param newDeviceStreams
 */
const EventStreamGroup::EventStreamList&
EventStreamGroup::getDeviceStreams( int deviceId )
{
  return deviceIdStreamsMap[ deviceId ];
}

void
EventStreamGroup::getAllDeviceStreams( 
                     EventStreamGroup::EventStreamList& newDeviceStreams ) const
{
  newDeviceStreams.clear();
  newDeviceStreams.insert( newDeviceStreams.end(),
                           deviceStreams.begin(), deviceStreams.end() );
}

/**
 * Return the null stream for the given device. If device id is left empty,
 * only a single device is assumed to be present.
 * 
 * @param deviceId
 * @return 
 */
EventStream*
EventStreamGroup::getNullStream( int deviceId )
{
  return deviceNullStreamMap[ deviceId ];
}

EventStream*
EventStreamGroup::getFirstDeviceStream( int deviceId )
{
  if( deviceFirstStreamMap.count( deviceId ) == 0 )
  {
    std::sort( deviceStreams.begin(), deviceStreams.end() );
    
    deviceFirstStreamMap[ deviceId ] = deviceStreams.front();
  }
  
  return deviceFirstStreamMap[ deviceId ];
}