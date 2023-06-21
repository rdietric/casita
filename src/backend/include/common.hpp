/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <assert.h>
#include <stdexcept>

// the following definition and include is needed for the printf PRIu64 macro
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#if !defined(UINT64_MAX)
#include <limits>
#define UINT64_MAX std::numeric_limits< uint64_t >::max()
#endif

#if !defined(UINT32_MAX)
#if !defined(UINT64_MAX)
#include <limits>
#endif
#define UINT32_MAX std::numeric_limits< uint32_t >::max()
#endif

#define VERBOSE_NONE  0
#define VERBOSE_TIME  1
#define VERBOSE_BASIC 2
#define VERBOSE_SOME  3
#define VERBOSE_ALL   4
#define VERBOSE_ANNOY 5


#define OTF2_OMP_FORKJOIN_INTERNAL "__ompforkjoin__internal"

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) { throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
  }

namespace casita
{
  class RTException :
    virtual public std::runtime_error
  {
    public:

      RTException( const char* format, ... ) :
        std::runtime_error( "Runtime exception" )
      {
        va_list args;
        fprintf( stderr, "Runtime Error: " );
        va_start( args, format );
        vfprintf( stderr, format, args );
        va_end( args );
        fprintf( stderr, "\n" );
        fflush( stderr );
        assert( 0 );
      }
  };
}
