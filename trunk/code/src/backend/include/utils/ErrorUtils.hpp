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
#include <cstdlib>
#include <cstdarg>
#include <cassert>
#include "common.hpp"
#include "Parser.hpp"

namespace casita
{

 class ErrorUtils
 {
   public:

     static ErrorUtils&
     getInstance( )
     {
       static ErrorUtils instance;
       return instance;
     }

     void
     throwError( const char* fmt, ... )
     {
       va_list      argp;
       const size_t MAX_LEN = 512;
       char msg[MAX_LEN];

       va_start( argp, fmt );
       vsnprintf( msg, MAX_LEN, fmt, argp );
       va_end( argp );

       if ( throwExceptions )
       {
         throw RTException( msg );
       }
       else
       {
         fprintf( stderr, "%s\n", msg );
       }
     }

     void
     throwFatalError( const char* fmt, ... )
     {
       va_list      argp;
       const size_t MAX_LEN = 512;
       char msg[MAX_LEN];

       va_start( argp, fmt );
       vsnprintf( msg, MAX_LEN, fmt, argp );
       va_end( argp );
       throw RTException( msg );
     }

     void
     outputMessage( const char* fmt, ... )
     {
       va_list      argp;
       const size_t MAX_LEN = 512;
       char msg[MAX_LEN];

       va_start( argp, fmt );
       vsnprintf( msg, MAX_LEN, fmt, argp );
       va_end( argp );

       fprintf( stderr, "%s\n", msg );
     }

     void
     setNoExceptions( )
     {
       throwExceptions = false;
       fprintf( stderr, "Disabled exceptions\n" );
     }

     void
     setVerbose( )
     {
       verbose = true;
       //fprintf( stderr, "Enabled verbose output for error utils\n" );
     }

   private:
     bool throwExceptions;
     bool verbose;

     ErrorUtils( ) :
       throwExceptions( true ),
       verbose( false )
     {

     }

     ErrorUtils( const ErrorUtils& utils )
     {

     }
 };

#define UTILS_ASSERT( cond, fmt, ... ) \
  if ( !( cond ) ) { \
    ErrorUtils::getInstance( ).throwError( fmt, ## __VA_ARGS__ ); \
  }

#define UTILS_MSG( cond, fmt, ... ) \
  if ( cond ) { \
    ErrorUtils::getInstance( ).outputMessage( fmt, ## __VA_ARGS__ ); \
  }

// debugging
#if defined(DEBUG) && defined(DEBUG_LEVEL)

// debug MPI non-blocking communication (cmake .. -DDEBUG_LEVEL=2)
#if (DEBUG_LEVEL == 1)
  #define DEBUG_MPI_ICOMM 1
#else
  #define DEBUG_MPI_ICOMM 0
#endif
  
// debug MPI critical-path detection (cmake .. -DDEBUG_LEVEL=2)
#if (DEBUG_LEVEL == 2)
  #define DEBUG_CPA_MPI 1
#else
  #define DEBUG_CPA_MPI 0
#endif
 
  #define UTILS_DBG_MSG( cond, fmt, ... ) \
    if ( cond ) { \
      ErrorUtils::getInstance( ).outputMessage( fmt, ## __VA_ARGS__ ); \
    }
#else
  #define UTILS_DBG_MSG( cond, fmt, ... )
#endif

}
