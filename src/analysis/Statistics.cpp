/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017, 2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 */

// This class collects simple statistics

#include <fstream>

#include "Statistics.hpp"

using namespace casita;

/**
 * Constructor
 */
Statistics::Statistics() 
{ 
  /* initialize statistics with zero values and counts */
  std::fill( stats, stats + STAT_NUMBER, 0 );
  
  /* initialize activity counts with zero */
  std::fill( activity_count, activity_count + STAT_ACTIVITY_TYPE_NUMBER, 0 );
}


/**
 * Destructor
 */
Statistics::~Statistics() { }

void
Statistics::addStatWithCount( StatMetric statType, uint64_t time, uint64_t count )
{
  stats[ statType ] += count;
  stats[ statType + 1 ] += time;
}

void
Statistics::addStatValue( StatMetric statType, uint64_t value )
{
  stats[ statType ] += value;
}

void
Statistics::addAllStats( uint64_t* stats )
{
  int i;
  for( i = 0; i < STAT_NUMBER; ++i )
  {
    this->stats[ i ] += stats[ i ];
  }
}

uint64_t*
Statistics::getStats()
{
  return stats;
}

void
Statistics::countActivity( ActivityType activityType )
{
  activity_count[ activityType ]++;
}

uint64_t*
Statistics::getActivityCounts()
{
  return activity_count;
}

void
Statistics::addActivityCounts( uint64_t* counts )
{
  int i;
  for( i = 0; i < STAT_ACTIVITY_TYPE_NUMBER; ++i )
  {
    this->activity_count[ i ] += counts[ i ];
  }
}
