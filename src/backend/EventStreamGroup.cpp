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
  
  streamsMap[ p->getId() ] = p;
}

void
EventStreamGroup::addDeviceStream( DeviceStream* p )
{
  if( !p->isDeviceStream() )
  {
    UTILS_WARNING( "%s (%"PRIu64") is not a device stream", 
                   p->getName(), p->getId() );
    return;
  }
  
  deviceStreams.push_back( p );
  allStreams.push_back( p );
  
  streamsMap[ p->getId() ] = p;
  
  DeviceStream* devStrm = ( DeviceStream* ) p;
  
  // generate a map with per device streams
  deviceIdStreamsMap[ devStrm->getDeviceId() ].push_back( p );
  
  // if the user chose to set a specific null stream
  if( Parser::getInstance().getProgramOptions().nullStream != -1 &&
      devStrm->getNativeStreamId() == Parser::getInstance().getProgramOptions().nullStream )
  {
    setDeviceNullStream( p );
  }
  else
  {
    deviceNullStreamOnly = false;
  }
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
EventStreamGroup::setDeviceNullStream( DeviceStream* stream )
{
  if( stream == NULL )
  {
    UTILS_WARNING( "Cannot set null stream" );
    return;
  }
  
  if( !stream->isDeviceStream() )
  {
    UTILS_WARNING( "%s (%"PRIu64") Cannot set device null stream for non-device"
                   " stream", stream->getName(), stream->getId() );
    return;
  }
  
  DeviceStream* devStrm = ( DeviceStream* ) stream;
  
  stream->setStreamType( EventStream::ES_DEVICE_NULL );
  
  deviceNullStreamMap[ devStrm->getDeviceId() ] = stream;
  
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

EventStream*
EventStreamGroup::getStream( uint64_t id ) const
{
  EventStreamMap::const_iterator iter = streamsMap.find( id );
  if ( iter != streamsMap.end() )
  {
    return iter->second;
  }
  else
  {
    return NULL;
  }
}

MpiStream*
EventStreamGroup::getMpiStream( uint64_t id ) const
{
  EventStream* strm = getStream( id );
  
  if( !strm->isMpiStream() )
  {
    UTILS_WARNING( "%"PRIu64" is not an MPI stream!" );
    
    return NULL;
  }
  
  return ( MpiStream* )strm;
}

DeviceStream*
EventStreamGroup::getDeviceStream( uint64_t id ) const
{
  EventStreamMap::const_iterator iter = streamsMap.find( id );
  if ( iter != streamsMap.end() )
  {
    if( !iter->second->isDeviceStream() )
    {
      UTILS_WARNING( "%"PRIu64" is not a device stream!" );
      
      return NULL;
    }
    
    return ( DeviceStream* ) iter->second;
  }
  else
  {
    return NULL;
  }
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getAllStreams() const
{
  return allStreams;
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getHostStreams() const
{
  return hostStreams;
}

const EventStreamGroup::DeviceStreamList&
EventStreamGroup::getDeviceStreams() const
{
  return deviceStreams;
}

void
EventStreamGroup::getDeviceStreams( DeviceStreamList& newDeviceStreams ) const
{
  newDeviceStreams.clear();
  newDeviceStreams.insert( newDeviceStreams.end(),
                           deviceStreams.begin(), deviceStreams.end() );
}

/**
 * Get list of event streams for a given device ID.
 * 
 * @param deviceId
 * @param newDeviceStreams
 */
const EventStreamGroup::DeviceStreamList&
EventStreamGroup::getDeviceStreams( int deviceId )
{
  return deviceIdStreamsMap[ deviceId ];
}

/**
 * Return the null stream for the given device. If device id is left empty,
 * only a single device is assumed to be present.
 * 
 * @param deviceId
 * @return 
 */
DeviceStream*
EventStreamGroup::getDeviceNullStream( int deviceId )
{
  return deviceNullStreamMap[ deviceId ];
}

DeviceStream*
EventStreamGroup::getFirstDeviceStream( int deviceId )
{
  if( deviceFirstStreamMap.count( deviceId ) == 0 )
  {
    std::sort( deviceStreams.begin(), deviceStreams.end() );
    
    deviceFirstStreamMap[ deviceId ] = deviceStreams.front();
  }
  
  return deviceFirstStreamMap[ deviceId ];
}