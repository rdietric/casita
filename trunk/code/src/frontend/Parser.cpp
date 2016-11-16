/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016,
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
#include <string>
#include <vector>
#include <stdlib.h>
#include <unistd.h> //for getcwd
#include "utils/ErrorUtils.hpp"
#include "Parser.hpp"

#if defined(BOOST_AVAILABLE)
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/variables_map.hpp>
#endif

namespace casita
{
#if defined(BOOST_AVAILABLE)
  namespace po = boost::program_options;
#endif

  Parser::Parser( ) { }

  Parser::Parser( Parser& ) { }

  bool
  Parser::endsWith( std::string const& str, std::string const& ext )
  {
    if( str.length( ) >= ext.length( ) )
    {
      return( 0 ==
             str.compare( str.length( ) - ext.length( ), ext.length( ), ext ));
    }
    else
    {
      return false;
    }
  }

  template < class T >
  bool
  from_string( T& t,
               const std::string& s )
  {
    std::istringstream iss( s );
    if( !(iss >> t) )
    {
      //UTILS_WARNING( "Conversion of argument %s invalid!", s.c_str() );
      return false;
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
    return Parser::getInstance( ).options.verbose;
  }

#if defined(BOOST_AVAILABLE)

  bool
  Parser::init_with_boost( int argc, char** argv ) throw( std::runtime_error)
  {
    // default values
    bool noSummary = false;

    setDefaultValues( );

    try
    {
      /* add help message as options description */
      std::stringstream desc_stream;
      desc_stream << "Usage: casita <otf-file> [options]" << std::endl;
      po::options_description desc( desc_stream.str( ) );

      /* add standard options */
      desc.add_options( )
        ("help,h", "print help message")

        ("input,i", po::value< std::string >(&options.filename),
         "input OTF2 trace file")
        ("output,o", po::value< std::string >(&options.outOtfFile),
         "output OTF2 trace file")
        ("no-summary", po::value< bool >(&noSummary)->zero_tokens( ),
         "do not aggregate statistics to summary")
        ("path,p", po::value< bool >(&options.printCriticalPath)->zero_tokens( ),
         "print critical paths")
        ("verbose,v", po::value< int >(&options.verbose),
         "verbosity level")
        ("no-errors", po::value< bool >(&options.noErrors)->zero_tokens( ),
         "ignore non-fatal analysis errors")
        ("ignore-nb-mpi", po::value< bool >(&options.ignoreAsyncMpi)->zero_tokens( ),
         "Treat non-blocking MPI functions as CPU functions.")
        ("secure-mpi-cpa", po::value< bool >(&options.criticalPathSecureMPI)->zero_tokens( ),
         "Perform MPI critical-path analysis with slave feedback to ensure that a master has been found. "
         "(Avoids a potential deadlock situation, when no master has been found.)")
        ("interval-analysis,c", po::value< uint32_t >(&options.analysisInterval)->implicit_value( 64 ),
         "Run analysis in intervals (between global collectives) to reduce memory footprint. "
         "The optional value sets the number of pending graph nodes before an analysis run is started.")
        ;

      po::positional_options_description pos_options_descr;
      pos_options_descr.add( "input", 1 );

      /* parse command line options and config file and store values in vm */
      po::variables_map vm;
      po::store( po::command_line_parser( argc, argv ).options(
                                                                desc ).positional( pos_options_descr ).run( ), vm );

      po::notify( vm );

      // print help message and quit simulation
      if( vm.count( "help" ) )
      {
        std::cout << desc << "\n";
        return false;
      }

      if( vm.count( "output" ) )
      {
        options.createOTF = true;
      }

      if( vm.count( "input" ) != 1 )
      {
        std::cerr << "Please specify exactly one input OTF2 file." << std::endl;
        std::cerr << desc << "\n";
        return false;
      }

      if( noSummary )
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
#endif

  void
  Parser::printHelp( )
  {
    std::cout << "Usage: casita <otf2-file> [options]\n" << std::endl;
    std::cout << "  -h [--help]             print help message" << std::endl;
    std::cout << "  -i [--input=]NAME       input OTF2 file" << std::endl;
    std::cout << "  -o [--output=]NAME      output OTF2 file" << std::endl;
    std::cout << "  -r [--replace]          replace CASITA trace and summary file" << std::endl;
    std::cout << "  -v [--verbose=]INTEGER  verbosity level" << std::endl;
    std::cout << "  -s [--summary]          create summary CSV file" << std::endl;
    std::cout << "      --top=INTEGER       print top optimization candidates" << std::endl;
    std::cout << "  -p [--path]             print critical paths" << std::endl;
    std::cout << "     [--no-errors]        ignore non-fatal errors" << std::endl;
    std::cout << "     [--ignore-impi]      treat non-blocking MPI functions as CPU functions" << std::endl;
    std::cout << "     [--ignore-cuda]      treat CUDA functions as CPU functions" << std::endl;
    std::cout << "  -c [--interval-analysis=][uint32_t]   Run analysis in intervals (between global" << std::endl
      << "                          collectives) to reduce memory footprint. The optional value sets the " << std::endl
      << "                          number of pending graph nodes before an analysis run is started." << std::endl;


  }

  bool
  Parser::processArgs( int argc, char** argv )
  {

    std::string opt;
    for( int i = 1; i < argc; i++ )
    {
      opt = std::string( argv[i] );

      //  input file:
      if( i == 1 && opt.find_first_of( "-" ) != 0 )
      {
        options.filename = std::string( argv[1] );
      }

      else if( opt.compare( std::string( "-i" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.filename = std::string( argv[i] );
        }
      }

      else if( opt.find( "--input=" ) != std::string::npos )
      {
        options.filename = opt.erase( 0, std::string( "--input=" ).length( ) );
      }


        //  output file
      else if( opt.compare( std::string( "-o" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.outOtfFile = std::string( argv[i] );
          options.createTraceFile = true;
        }
      }

      else if( opt.find( "--output=" ) != std::string::npos )
      {
        options.outOtfFile = opt.erase( 0, std::string( "--output=" ).length( ) );
        options.createTraceFile = true;
      }
      
      // replace CASITA output trace and summary file
      else if( opt.compare( std::string( "-r" ) ) == 0 ||
               opt.find( "--replace" ) != std::string::npos )
      {
        options.replaceCASITAoutput = true;
      }


        // help
      else if( opt.compare( std::string( "-h" ) ) == 0 ||
               opt.find( "--help" ) != std::string::npos )
      {
        return false;
      }

        //else if( checkOption( argc, argv, &i, "-v", "--verbose" ) )

        // verbose
      else if( opt.compare( std::string( "-v" ) ) == 0 ||
               opt.compare( std::string( "--verbose" ) ) == 0 )
      {
        if( ++i < argc )
        {
          int verbose;
          if( from_string( verbose, argv[i] ) )
          {
            options.verbose = verbose;
          }
          else
          {
            --i;
          }
        }
      }

      else if( opt.find( "--verbose=" ) != std::string::npos )
      {
        //options.verbose = atoi(opt.erase(0, std::string("--verbose=").length()).c_str());
        int verbose;
        if( from_string( verbose, opt.erase( 0, 10 ) ) )
        {
          options.verbose = verbose;
        }
        else
        {
          UTILS_WARNING( "Unrecognized option %s for --verbose", opt.c_str( ) );
        }
      }

      //  summary
      else if( opt.compare( std::string( "-s" ) ) == 0 ||
               opt.find( "--summary" ) != std::string::npos )
      {
        options.createRatingCSV = true;
      }


      // print top X activities
      else if( opt.find( "--top=" ) != std::string::npos )
      {
        options.topX = atoi( opt.erase( 0, std::string( "--top=" ).length( ) ).c_str( ) );
      }


      // path
      else if( opt.compare( std::string( "-p" ) ) == 0 )
      {
        options.printCriticalPath = true;
        i++;
      }

      else if( opt.find( "--path=" ) != std::string::npos )
      {
        options.printCriticalPath = true;
      }

      // no error
      else if( opt.find( "--no-errors=" ) != std::string::npos )
      {
        options.noErrors = true;
      }

      // ignore non blocking
      else if( opt.find( "--ignore-impi" ) != std::string::npos )
      {
        options.ignoreAsyncMpi = true;
      }
      
      // ignore non blocking
      else if( opt.find( "--ignore-cuda" ) != std::string::npos )
      {
        options.ignoreCUDA = true;
      }

      // interval analysis TODO: complete optional?
      else if( opt.compare( std::string( "-c" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.analysisInterval = atoi( argv[i] );
        }
      }

      else if( opt.find( "--interval-analysis=" ) != std::string::npos )
      {
        options.analysisInterval = 
          atoi( opt.erase( 0, std::string( "--interval-analysis=" ).length( ) ).c_str( ) );
      }

        // if nothing matches 
      else
      {
        std::cout << "Unrecognized option " << opt << std::endl;
        return false;
      }
    }

    if( options.filename.length( ) == 0 )
    {
      std::cout << "No Inputfile specified" << std::endl;
      return false;
    }

    return true;
  }

  bool
  Parser::init_without_boost( int mpiRank, int argc, char** argv )
  throw( std::runtime_error)
  {

    bool success = false;

    setDefaultValues( );

    success = processArgs( argc, argv );

    // if all arguments have been parsed and an OTF2 output shall be generated
    if( success && options.createTraceFile )
    {
      setOutputDirAndFile( );
      
      std::string traceEvtDir = pathToFile;
      if( !pathToFile.empty() )
      {
        traceEvtDir += std::string( "/");
      }
      
      traceEvtDir += outArchiveName;
      
      std::string file = traceEvtDir + std::string( ".otf2" );
      
      // if output .otf2 file or trace directory exist
      if( access( file.c_str( ), F_OK ) == 0 ||
          access( traceEvtDir.c_str( ), F_OK ) == 0 )
      {
        if( options.replaceCASITAoutput )
        {
          std::string rmCmd = std::string( "rm -rf " ) + traceEvtDir + std::string( "*" );
          
          UTILS_MSG( mpiRank == 0, "Output file does already exist, %s", rmCmd.c_str( ) );

          system( rmCmd.c_str() );
        }
        else // if the output file already exists, append unique number
        {
          int n = 2;
          std::stringstream num;
          num << n;

          // append underscore for new output directory
          traceEvtDir += std::string( "_" );

          // search for unique number to append 
          while( access( (traceEvtDir + num.str( ) + std::string( ".otf2" )).c_str( ), F_OK ) == 0 ||
                 access( (traceEvtDir + num.str( )).c_str( ), F_OK ) == 0 )
          {
            n++;
            num.str( "" );
            num.clear( );
            num << n;
          }

          outArchiveName = outArchiveName + std::string( "_" ) + num.str( );

          UTILS_MSG( mpiRank == 0,
                     "Output file does already exist, changed to: %s",
                     outArchiveName.c_str( ) );
        }
      }
      else //output trace directory or file do not exist
      if( !pathToFile.empty() ) // and path is given
      {
        std::string mkdirCmd = std::string( "mkdir -p " ) + pathToFile;
          
        UTILS_MSG( mpiRank == 0, "Output directory %s does not exist, %s ", 
                   traceEvtDir.c_str(), mkdirCmd.c_str( ) );
          
        // create the output directory
        system( mkdirCmd.c_str() );
      }
      
      // if the path is empty (only output file given) -> writing in PWD
      if( pathToFile.empty() )
      {
        pathToFile = std::string( "." );
      }
    }

    return success;
  }

  /*
   * 
   */
  void
  Parser::setOutputDirAndFile( )
  {
    std::string otfFilename = options.outOtfFile;
    
    std::size_t charPos = otfFilename.find_last_of("/");
    
    // if only a file name is given
    if( charPos == std::string::npos )
    {
      outArchiveName = otfFilename;
    }
    else // path (relative or absolute) with directory given
    {
      pathToFile     = otfFilename.substr( 0, charPos );
      outArchiveName = otfFilename.substr( charPos + 1 );
    }
    
    // remove the .otf2 extension from OTF2 archive name
    charPos = outArchiveName.find_last_of( "." );
    outArchiveName = outArchiveName.substr( 0, charPos );
    
    //UTILS_MSG( true, "Path %s, File: %s", 
    //             pathToFile.c_str(), outOtf2ArchiveName.c_str() );
  }

  ProgramOptions&
  Parser::getProgramOptions( )
  {
    return options;
  }

  void
  Parser::setDefaultValues( )
  {
    options.createTraceFile = false;
    options.eventsProcessed = 0;
    options.filename = "";
    options.mergeActivities = true;
    options.noErrors = false;
    options.analysisInterval = 64;
    options.outOtfFile = "casita.otf2";
    options.replaceCASITAoutput = false;
    options.printCriticalPath = false;
    options.verbose = 0;
    options.topX = 20;
    options.ignoreAsyncMpi = false;
    options.ignoreCUDA = false;
    options.createRatingCSV = true;
  }

}
