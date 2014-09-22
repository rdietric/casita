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

#include <algorithm>

#include "EventStreamGroup.hpp"

using namespace casita;

EventStreamGroup::EventStreamGroup( ) :
  startTime( 0 ),
  nullStream( NULL )
{

}

EventStreamGroup::EventStreamGroup( uint64_t               start,
                                    const EventStreamList& hostStreams,
                                    const EventStreamList& deviceStreams,
                                    EventStream*           nullStream )
{
  this->startTime  = start;
  this->hostStreams.assign( hostStreams.begin( ), hostStreams.end( ) );
  this->deviceStreams.assign( deviceStreams.begin( ), deviceStreams.end( ) );
  this->nullStream = nullStream;
}

EventStreamGroup::~EventStreamGroup( )
{

}

void
EventStreamGroup::addHostStream( EventStream* p )
{
  hostStreams.push_back( p );
}

void
EventStreamGroup::addDeviceStream( EventStream* p )
{
  deviceStreams.push_back( p );
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
}

void
EventStreamGroup::getAllStreams( EventStreamList& streams ) const
{
  streams.clear( );
  streams.assign( hostStreams.begin( ), hostStreams.end( ) );
  if ( nullStream )
  {
    streams.insert( streams.end( ), nullStream );
  }
  streams.insert( streams.end( ), deviceStreams.begin( ), deviceStreams.end( ) );
}

void
EventStreamGroup::getAllStreams( EventStreamList& streams,
                                 Paradigm         paradigm ) const
{
  streams.clear( );
  streams.assign( hostStreams.begin( ), hostStreams.end( ) );
  if ( nullStream )
  {
    streams.insert( streams.end( ), nullStream );
  }
  streams.insert( streams.end( ), deviceStreams.begin( ), deviceStreams.end( ) );

  for ( EventStreamList::iterator iter = streams.begin( ); iter != streams.end( ); )
  {
    EventStreamList::iterator current = iter;
    iter++;

    EventStream* p = *current;
    GraphNode*   lastGNode = p->getLastNode( paradigm );
    if ( !lastGNode || ( lastGNode->isProcess( ) ) )
    {
      streams.erase( current );
    }
  }
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getDeviceStreams( ) const
{
  return deviceStreams;
}

const EventStreamGroup::EventStreamList&
EventStreamGroup::getHostStreams( ) const
{
  return hostStreams;
}

void
EventStreamGroup::getAllDeviceStreams( EventStreamGroup::EventStreamList& newDeviceStreams ) const
{
  newDeviceStreams.clear( );
  if ( nullStream )
  {
    newDeviceStreams.insert( newDeviceStreams.end( ), nullStream );
  }
  newDeviceStreams.insert( newDeviceStreams.end( ),
                           deviceStreams.begin( ), deviceStreams.end( ) );
}

EventStream*
EventStreamGroup::getNullStream( ) const
{
  return nullStream;
}

size_t
EventStreamGroup::getNumStreams( ) const
{
  size_t numProcs = hostStreams.size( ) + deviceStreams.size( );
  if ( nullStream )
  {
    numProcs++;
  }

  return numProcs;
}

size_t
EventStreamGroup::getNumHostStreams( ) const
{
  return hostStreams.size( );
}

size_t
EventStreamGroup::getNumDeviceStreams( ) const
{
  return deviceStreams.size( );
}

uint64_t
EventStreamGroup::getStartTime( ) const
{
  return startTime;
}
