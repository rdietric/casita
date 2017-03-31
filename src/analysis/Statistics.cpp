/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017,
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
  std::fill( stats, stats + STATS_NUMBER, 0 );
}


/**
 * Destructor
 */
Statistics::~Statistics() { }

