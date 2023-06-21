/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2017, 2019,
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

 class Utils
 {
   public:

     static Utils&
     getInstance( )
     {
       static Utils instance;
       return instance;
     }
     
     void
     throwWarning( const char* fmt, ... )
     {
       
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
     outputMessageNoLineBreak( const char* fmt, ... )
     {
       va_list      argp;
       const size_t MAX_LEN = 512;
       char msg[MAX_LEN];

       va_start( argp, fmt );
       vsnprintf( msg, MAX_LEN, fmt, argp );
       va_end( argp );

       fprintf( stderr, "%s", msg );
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
     
     void
     setTimerResolution( uint64_t ticksPerSecond )
     {
       this->ticksPerSecond = ticksPerSecond;
     }
     
     double
     getRealTime( uint64_t t )
     {
       return (double)t / (double)(this->ticksPerSecond);
     }

   private:
     bool throwExceptions;
     bool verbose;
     
     uint64_t ticksPerSecond;

     Utils( ) :
       throwExceptions( true ),
       verbose( false )
     {

     }

     Utils( const Utils& utils )
     {

     }
 };

#define UTILS_ASSERT( cond, fmt, ... ) \
  if ( !( cond ) ) { \
    Utils::getInstance( ).throwError( fmt, ## __VA_ARGS__ ); \
  }

#define UTILS_OUT( fmt, ... ) \
   Utils::getInstance().outputMessage( fmt, ## __VA_ARGS__ );

#define UTILS_OUT_NOBR( fmt, ... ) \
   Utils::getInstance().outputMessageNoLineBreak( fmt, ## __VA_ARGS__ ); \
 
 
#define UTILS_MSG( cond, fmt, ... ) \
  if ( cond ) { \
    Utils::getInstance().outputMessage( fmt, ## __VA_ARGS__ ); \
  }
 
#define UTILS_MSG_NOBR( cond, fmt, ... ) \
  if ( cond ) { \
    Utils::getInstance().outputMessageNoLineBreak( fmt, ## __VA_ARGS__ ); \
  }
 
#define UTILS_WARNING( fmt, ... ) \
    Utils::getInstance().outputMessage( "Warning: " fmt, ## __VA_ARGS__ );
 
/**
 * Emit a warning, but only on first occurrence.
 * (Code snippet taken from Score-P)
 */
#define UTILS_WARN_ONCE( fmt, ... ) \
  do { \
    static int utils_warn_once_##__LINE__; \
    if ( !utils_warn_once_##__LINE__ ) \
    { \
      utils_warn_once_##__LINE__ = 1; \
      Utils::getInstance().outputMessage( "Warn once: " fmt, ## __VA_ARGS__ ); \
    }\
  } while ( 0 )
 
#define UTILS_MSG_ONCE_OR( cond, fmt, ... ) \
 if ( cond ) { \
   UTILS_OUT( fmt, __VA_ARGS__ ); \
 } \
 else \
 { \
   UTILS_WARN_ONCE( fmt, __VA_ARGS__ ); \
 }
 
 #define UTILS_MSG_IF_ONCE( cond, cond2, fmt, ... ) \
 if ( cond ) { \
   UTILS_OUT( fmt, __VA_ARGS__ ); \
 } \
 else if ( cond2 ) \
 { \
   UTILS_WARN_ONCE( fmt, __VA_ARGS__ ); \
 }

#define UTILS_GET_REALTIME(ticks) Utils::getInstance().getRealTime(ticks)
#define UTILS_GET_NODE_REALTIME(node) Utils::getInstance().getRealTime(node->getTime())
#define UTILS_GET_NODE_INFO(node) node->getUniqueName().c_str(), Utils::getInstance().getRealTime(node->getTime())

// debugging
#if defined(DEBUG) && defined(DEBUG_LEVEL)

// debug MPI non-blocking communication (cmake .. -DDEBUG_LEVEL=1)
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
      Utils::getInstance( ).outputMessage( fmt, ## __VA_ARGS__ ); \
    }
#else
  #define UTILS_DBG_MSG( cond, fmt, ... )
#endif

}
