/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013 - 2014, 2016 - 2017
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
  nullStream( NULL )
{

}

EventStreamGroup::EventStreamGroup( uint64_t               start,
                                    const EventStreamList& hostStreams,
                                    const EventStreamList& deviceStreams,
                                    EventStream*           nullStream )
{
  this->hostStreams.assign( hostStreams.begin(), hostStreams.end() );
  this->deviceStreams.assign( deviceStreams.begin(), deviceStreams.end() );
  this->nullStream = nullStream;
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
}

EventStreamGroup::EventStreamList::iterator
EventStreamGroup::removeHostStream( EventStream* p )
{
  for ( EventStreamList::iterator iter = hostStreams.begin( );
        iter != hostStreams.end( ); ++iter )
  {
    if ( *iter == p )
    {
      return hostStreams.erase( iter );
    }
  }
  return hostStreams.end( );
}

void
EventStreamGroup::setNullStream( EventStream* p )
{
  nullStream = p;
  
  allStreams.push_back( nullStream );
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getAllStreams() const
{
  return allStreams;
}

void
EventStreamGroup::getAllStreams( EventStreamList& streams ) const
{
  // clear list of streams (input)
  streams.clear( );
  
  // add all host streams
  streams.assign( hostStreams.begin( ), hostStreams.end( ) );
  
  // add the device null stream if available
  if ( nullStream )
  {
    streams.insert( streams.end( ), nullStream );
  }
  
  // add all other device streams
  streams.insert( streams.end( ), deviceStreams.begin( ), deviceStreams.end( ) );
}

void
EventStreamGroup::getAllStreams( EventStreamList& streams,
                                 Paradigm         paradigm ) const
{
  // clear stream list
  streams.clear();
  
  // add all streams
  streams.assign( hostStreams.begin(), hostStreams.end() );
  if ( nullStream )
  {
    streams.insert( streams.end(), nullStream );
  }
  streams.insert( streams.end(), deviceStreams.begin(), deviceStreams.end() );

  // iterate over streams
  for ( EventStreamList::iterator iter = streams.begin(); iter != streams.end(); )
  {
    EventStream* p = *iter;
    GraphNode*   lastGNode = p->getLastNode( paradigm );
    
    // if we did not find a last node OR the last node is atomic process or 
    // intermediate node
    if ( !lastGNode || lastGNode->isProcess() )
    {
      iter = streams.erase( iter );
    }
    else
    {
      ++iter;
    }
  }
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
void
EventStreamGroup::getDeviceStreams( int deviceId, 
                      EventStreamGroup::EventStreamList& newDeviceStreams) const
{
  for( EventStreamList::const_iterator it = deviceStreams.begin(); 
       it != deviceStreams.end(); ++it )
  {
    if( (*it)->getDeviceId() == deviceId )
    {
      newDeviceStreams.insert( newDeviceStreams.end(), *it );
    }
  }
}

void
EventStreamGroup::getAllDeviceStreams( 
                     EventStreamGroup::EventStreamList& newDeviceStreams ) const
{
  newDeviceStreams.clear();
  if ( nullStream )
  {
    newDeviceStreams.insert( newDeviceStreams.end(), nullStream );
  }
  newDeviceStreams.insert( newDeviceStreams.end(),
                           deviceStreams.begin(), deviceStreams.end() );
}

EventStream*
EventStreamGroup::getNullStream() const
{
  return nullStream;
}

size_t
EventStreamGroup::getNumStreams() const
{
  //size_t numProcs = hostStreams.size() + deviceStreams.size();
  size_t numProcs = allStreams.size();
  
  if ( nullStream )
  {
    numProcs++;
  }

  return numProcs;
}

size_t
EventStreamGroup::getNumHostStreams() const
{
  return hostStreams.size();
}

size_t
EventStreamGroup::getNumDeviceStreams() const
{
  return deviceStreams.size();
}
