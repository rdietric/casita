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

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <assert.h>
#include <stdexcept>

#define VERBOSE_NONE 0
#define VERBOSE_BASIC 1
#define VERBOSE_ALL 2
#define VERBOSE_ANNOY 3

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
