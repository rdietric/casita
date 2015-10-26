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
 * What this file does:
 * - parsing command line options
 *
 */

#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>

#include "Parser.hpp"

#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/variables_map.hpp>

namespace casita
{
 namespace po = boost::program_options;

 Parser::Parser( )
 {

 }

 Parser::Parser( Parser& )
 {

 }

 bool
 Parser::endsWith( std::string const& str, std::string const& ext )
 {
   if ( str.length( ) >= ext.length( ) )
   {
     return ( 0 ==
              str.compare( str.length( ) - ext.length( ), ext.length( ), ext ) );
   }
   else
   {
     return false;
   }
 }

 template < class T >
 bool
 from_string( T& t,
              const std::string& s,
              std::ios_base& ( *f )( std::ios_base & ) )
 {
   std::istringstream iss( s );
   if ( ( iss >> f >> t ).fail( ) )
   {
     throw std::invalid_argument( "conversion invalid!" );
   }

   return true;
 }

 Parser&
 Parser::getInstance( )
 {
   static Parser instance;
   return instance;
 }
 
 int
 Parser::getVerboseLevel( )
 {
   return Parser::getInstance().options.verbose;
 }

 bool
 Parser::init( int argc, char** argv ) throw ( std::runtime_error )
 {
   // default values
   bool noSummary = false;

   options.createOTF         = false;
   options.filename          = "";
   options.mergeActivities   = true;
   options.noErrors          = false;
   options.outOtfFile        = "";
   options.printCriticalPath = false;
   options.criticalPathSecureMPI = false;
   options.verbose           = 0;
   options.ignoreAsyncMpi    = false;
   options.analysisInterval  = 0;

   try
   {
     /* add help message as options description */
     std::stringstream desc_stream;
     desc_stream << "Usage: casita <otf-file> [options]" << std::endl;
     po::options_description desc( desc_stream.str( ) );

     /* add standard options */
     desc.add_options( )
       ( "help,h", "print help message" )

       ( "input,i", po::value< std::string >( &options.filename ),
       "input OTF2 trace file" )
       ( "output,o", po::value< std::string >( &options.outOtfFile ),
       "output OTF2 trace file" )
       ( "no-summary", po::value< bool >( &noSummary )->zero_tokens( ),
       "do not aggregate statistics to summary" )
       ( "path,p", po::value< bool >( &options.printCriticalPath )->zero_tokens( ),
       "print critical paths" )
       ( "verbose,v", po::value< int >( &options.verbose ),
       "verbosity level" )
       ( "no-errors", po::value< bool >( &options.noErrors )->zero_tokens( ),
       "ignore non-fatal analysis errors" )
       ( "ignore-nb-mpi", po::value< bool >( &options.ignoreAsyncMpi )->zero_tokens( ),
       "Treat non-blocking MPI functions as CPU functions." )
       ( "secure-mpi-cpa", po::value< bool >( &options.criticalPathSecureMPI )->zero_tokens( ),
       "Perform MPI critical-path analysis with slave feedback to ensure that a master has been found. "
       "(Avoids a potential deadlock situation, when no master has been found.)" )
       ( "interval-analysis,c", po::value< uint32_t >( &options.analysisInterval )->implicit_value( 64 ),
       "Run analysis in intervals (between global collectives) to reduce memory footprint. "
       "The optional value sets the number of pending graph nodes before an analysis run is started." )
     ;

     po::positional_options_description pos_options_descr;
     pos_options_descr.add( "input", 1 );

     /* parse command line options and config file and store values in vm */
     po::variables_map vm;
     po::store( po::command_line_parser( argc, argv ).options(
                  desc ).positional( pos_options_descr ).run( ), vm );

     po::notify( vm );

     // print help message and quit simulation
     if ( vm.count( "help" ) )
     {
       std::cout << desc << "\n";
       return false;
     }

     if ( vm.count( "output" ) )
     {
       options.createOTF = true;
     }

     if ( vm.count( "input" ) != 1 )
     {
       std::cerr << "Please specify exactly one input OTF file." << std::endl;
       std::cerr << desc << "\n";
       return false;
     }
     else
     {
       if ( endsWith( options.filename, ".otf2" ) )
       {
#if ( ENABLE_OTF2 != 1 )
         std::cerr << "OTF2 not supported" << std::endl;
         return false;
#endif
       }
     }

     if ( noSummary )
     {
       options.mergeActivities = false;
     }

   }
   catch( boost::program_options::error& e )
   {
     std::cerr << e.what( ) << std::endl;
     return false;
   }
   return true;
 }

 ProgramOptions&
 Parser::getProgramOptions( )
 {
   return options;
 }

}
