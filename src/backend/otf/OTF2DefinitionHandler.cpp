/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017-2019,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

#include <cstring>

#include "common.hpp"
#include "utils/Utils.hpp"

#include "otf/OTF2DefinitionHandler.hpp"

/* using namespace casita; */
using namespace casita::io;

OTF2DefinitionHandler::OTF2DefinitionHandler( ) :
  timerResolution( 1 ),
  timerOffset( 0 ),
  traceLength( 0 )
{

}

OTF2DefinitionHandler::~OTF2DefinitionHandler( )
{

}

uint64_t
OTF2DefinitionHandler::getTimerResolution( )
{
  return timerResolution;
}

void
OTF2DefinitionHandler::setTimerResolution( uint64_t ticksPerSecond )
{
  this->timerResolution = ticksPerSecond;
}

uint64_t
OTF2DefinitionHandler::getTimerOffset( )
{
  return timerOffset;
}

void
OTF2DefinitionHandler::setTimerOffset( uint64_t offset )
{
  this->timerOffset = offset;
}

uint64_t
OTF2DefinitionHandler::getTraceLength( )
{
  return traceLength;
}

void
OTF2DefinitionHandler::setTraceLength( uint64_t length )
{
  this->traceLength = length;
}

/**
 * Make a copy of the given char and store it with its OTF2 region reference.
 *
 * @param stringRef
 * @param name
 */
void
OTF2DefinitionHandler::storeString( uint32_t stringRef, const char* name )
{
  /* make a copy of the string and put it into our internal map */
  size_t length = 1023;
  length      = strnlen( name, length );

  /* allocate memory for the string + the \0 end of string */
  char*  str    = (char*)malloc( length + 1 ); /* \todo: this is not freed */
  strncpy( str, name, length );
  str[length] = '\0';
  /* *str = '\0'; */
  /* strncat( str, name, length ); */

  stringRefMap[stringRef] = str;
}

/**
 * Get new OTF2 string reference based on the sorted property of the
 * stringRefMap.
 *
 * @param string string to generate a new OTF2 reference for
 * @return new OTF2 string reference
 */
uint32_t
OTF2DefinitionHandler::getNewStringRef( const char* string )
{
  uint32_t newStringRef = 1;

  if ( !stringRefMap.empty( ) )
  {
    /* get the largest string reference and add '1' */
    newStringRef += stringRefMap.rbegin( )->first;
  }

  stringRefMap[newStringRef] = string;

  return newStringRef;
}

bool
OTF2DefinitionHandler::haveStringRef( uint32_t stringRef ) const
{
  return ( stringRefMap.count( stringRef ) > 0 );
}

/**
 * Get a char pointer (name) for the given OTF2 string reference.
 *
 * @param stringRef
 * @return
 */
const char*
OTF2DefinitionHandler::getName( uint32_t stringRef )
{
  if ( stringRefMap.count( stringRef ) > 0 )
  {
    return stringRefMap[stringRef];
  }
  else
  {
    UTILS_WARNING( "Could not find string name for %u", stringRef );
    return NULL;
  }
}

void
OTF2DefinitionHandler::addRegion( OTF2_RegionRef regionRef,
    OTF2_Paradigm                                paradigm,
    OTF2_RegionRole                              regionRole,
    OTF2_StringRef                               stringRef )
{
  RegionInfo regInf;
  regInf.name              = getName( stringRef );
  regInf.paradigm          = paradigm;
  regInf.role              = regionRole;

  regionInfoMap[regionRef] = regInf;
}

/**
 * Create a new region and add it to the internal map.
 * Return the new OTF2 region reference based on the sorted property of the
 * regionRefMap.
 *
 * @param string name of the region
 * @param paradigm the OTF2 paradigm
 * @return new OTF2 region reference
 */
uint32_t
OTF2DefinitionHandler::createNewRegion( const char* string,
    OTF2_Paradigm                                   paradigm )
{
  uint32_t newRegionRef = 1;

  if ( !regionInfoMap.empty( ) )
  {
    /* get the largest region reference and add '1' */
    newRegionRef += regionInfoMap.rbegin( )->first;
  }

  RegionInfo regInf;
  regInf.name     = string;
  regInf.paradigm = paradigm;
  regInf.role     = OTF2_REGION_ROLE_ARTIFICIAL;
  regionInfoMap[newRegionRef] = regInf;

  return newRegionRef;
}

void
OTF2DefinitionHandler::setInternalRegions( )
{
  ompForkJoinRef  = createNewRegion( OTF2_OMP_FORKJOIN_INTERNAL, OTF2_PARADIGM_OPENMP );
  waitStateFuncId = createNewRegion( "WaitState", OTF2_PARADIGM_UNKNOWN );
}

uint32_t
OTF2DefinitionHandler::getWaitStateRegionId( ) const
{
  return waitStateFuncId;
}

uint32_t
OTF2DefinitionHandler::getForkJoinRegionId( ) const
{
  return ompForkJoinRef;
}

/**
 * Returns the region information. Needed in every enter and leave event during
 * trace reading and writing
 *
 * @param regionRef     ID of region the name is requested for
 * @return              region information (name, paradigm, role)
 */
const RegionInfo&
OTF2DefinitionHandler::getRegionInfo( const uint32_t regionRef ) const
{
  /* commented out, as count requires a find and [] access too */
  /*UTILS_ASSERT( regionInfoMap.count( regionRef ) > 0,
                "Could not find region reference!" );

  return regionInfoMap[ regionRef ];*/
  std::map< uint32_t, RegionInfo >::const_iterator it =
      regionInfoMap.find( regionRef );

  UTILS_ASSERT( it != regionInfoMap.end( ), "Could not find region reference!" );

  return it->second;
}

/**
 * Get the name of the region by its OTF2 region id (reference).
 *
 * @param id OTF2 region ID (reference)
 * @return string object containing the name of the region
 */
const char*
OTF2DefinitionHandler::getRegionName( uint32_t id ) const
{
  return this->getRegionInfo( id ).name;
}

/**
 * Determine whether the region with the given ID is a device function (e.g.
 * a CUDA kernel).
 *
 * @param id OTF2 region ID (reference)
 * @return true if the given region ID refers to a device function
 */
bool
OTF2DefinitionHandler::isDeviceFunction( uint32_t id ) const
{
  const RegionInfo& regInfo = this->getRegionInfo( id );
  if ( regInfo.role == OTF2_REGION_ROLE_FUNCTION &&
      ( regInfo.paradigm == OTF2_PARADIGM_CUDA ||
      regInfo.paradigm == OTF2_PARADIGM_OPENCL ) )
  {
    return true;
  }
  else
  {
    return false;
  }
}

void
OTF2DefinitionHandler::addLocationInfo( const uint64_t mpiRank, const char* nodeName )
{
  this->locationInfoMap[mpiRank] = nodeName;
}

/**
 * Get the name of the MPI rank.
 *
 * @param mpiRank MPI rank
 * @return string object containing the name of the node it is executed on
 */
const char*
OTF2DefinitionHandler::getNodeName( const int mpiRank ) const
{
  std::map< uint64_t, const char* >::const_iterator iter =
      locationInfoMap.find( mpiRank );
  if ( iter != locationInfoMap.end( ) )
  {
    return iter->second;
  }
  else
  {
    return NULL;
  }
}
