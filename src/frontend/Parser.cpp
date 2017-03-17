/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016, 2017
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

namespace casita
{
  Parser::Parser() { }

  Parser::Parser( Parser& ) { }

  bool
  Parser::endsWith( string const& str, string const& ext )
  {
    if( str.length() >= ext.length() )
    {
      return( 0 ==
             str.compare( str.length() - ext.length(), ext.length(), ext ));
    }
    else
    {
      return false;
    }
  }

  template < class T >
  bool
  from_string( T& t,
               const string& s )
  {
    istringstream iss( s );
    if( !(iss >> t) )
    {
      //UTILS_WARNING( "Conversion of argument %s invalid!", s.c_str() );
      return false;
    }

    return true;
  }

  Parser&
  Parser::getInstance()
  {
    static Parser instance;
    return instance;
  }

  int
  Parser::getVerboseLevel()
  {
    return Parser::getInstance().options.verbose;
  }
  
  vector < string >&
  Parser::getPredictionFilter()
  {
    return Parser::getInstance().predictionFilter;
  }

  void
  Parser::printHelp()
  {
    cout << "Usage: casita <otf2-file> [options]\n" << endl;
    cout << "  -h [--help]             print help message" << endl;
    cout << "  -i [--input=]NAME       input OTF2 file" << endl;
    cout << "  -o [--output=]NAME      output OTF2 file" << endl;
    cout << "  -r [--replace]          replace CASITA trace and summary file" << endl;
    cout << "  -v [--verbose=]INTEGER  verbosity level" << endl;
    cout << "  -s [--summary]          create summary CSV file" << endl;
    cout << "      --top=INTEGER       print top optimization candidates" << endl;
    cout << "      --filter=List       filter regions" << endl;
    cout << "  -p [--path]             print critical paths" << endl;
    cout << "     [--cpa-loop-check]   detect circular loops in process-local critical path (slower)" << endl;
    cout << "     [--no-errors]        ignore non-fatal errors" << endl;
    cout << "     [--ignore-impi]      treat non-blocking MPI functions as CPU functions" << endl;
    cout << "     [--ignore-cuda]      treat CUDA functions as CPU functions" << endl;
    cout << "  -c [--interval-analysis=][uint32_t]   Run analysis in intervals (between global" << endl
      << "                          collectives) to reduce memory footprint. The optional value sets the " << endl
      << "                          number of pending graph nodes before an analysis run is started." << endl;
  }

  bool
  Parser::processArgs( int argc, char** argv )
  {
    string opt;
    for( int i = 1; i < argc; i++ )
    {
      opt = string( argv[i] );

      //  input file:
      if( i == 1 && opt.find_first_of( "-" ) != 0 )
      {
        options.filename = string( argv[1] );
      }

      else if( opt.compare( string( "-i" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.filename = string( argv[i] );
        }
      }

      else if( opt.find( "--input=" ) != string::npos )
      {
        options.filename = opt.erase( 0, string( "--input=" ).length() );
      }


        //  output file
      else if( opt.compare( string( "-o" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.outOtfFile = string( argv[i] );
          options.createTraceFile = true;
        }
      }

      else if( opt.find( "--output=" ) != string::npos )
      {
        options.outOtfFile = opt.erase( 0, string( "--output=" ).length() );
        options.createTraceFile = true;
      }
      
      // replace CASITA output trace and summary file
      else if( opt.compare( string( "-r" ) ) == 0 ||
               opt.find( "--replace" ) != string::npos )
      {
        options.replaceCASITAoutput = true;
      }


        // help
      else if( opt.compare( string( "-h" ) ) == 0 ||
               opt.find( "--help" ) != string::npos )
      {
        return false;
      }

        //else if( checkOption( argc, argv, &i, "-v", "--verbose" ) )

        // verbose
      else if( opt.compare( string( "-v" ) ) == 0 ||
               opt.compare( string( "--verbose" ) ) == 0 )
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

      else if( opt.find( "--verbose=" ) != string::npos )
      {
        //options.verbose = atoi(opt.erase(0, string("--verbose=").length()).c_str());
        int verbose;
        if( from_string( verbose, opt.erase( 0, 10 ) ) )
        {
          options.verbose = verbose;
        }
        else
        {
          UTILS_WARNING( "Unrecognized option %s for --verbose", opt.c_str() );
        }
      }

      //  summary
      else if( opt.compare( string( "-s" ) ) == 0 ||
               opt.find( "--summary" ) != string::npos )
      {
        options.createRatingCSV = true;
      }


      // print top X activities
      else if( opt.find( "--top=" ) != string::npos )
      {
        options.topX = atoi( opt.erase( 0, string( "--top=" ).length() ).c_str() );
      }
      
      // get prediction filter
      else if( opt.find( "--filter=" ) != string::npos )
      {
        options.predictionFilter = opt.erase( 0, string( "--filter=" ).length() );
      }

      // print path
      else if( opt.compare( string( "-p" ) ) == 0 )
      {
        options.printCriticalPath = true;
        i++;
      }

      else if( opt.find( "--path=" ) != string::npos )
      {
        options.printCriticalPath = true;
      }
      
      // detect and avoid circular loops in process-local CPA
      else if( opt.find( "--cpa-loop-check" ) != string::npos )
      {
        options.cpaLoopCheck = true;
      }

      // no error
      else if( opt.find( "--no-errors=" ) != string::npos )
      {
        options.noErrors = true;
      }

      // ignore non blocking
      else if( opt.find( "--ignore-impi" ) != string::npos )
      {
        options.ignoreAsyncMpi = true;
      }
      
      // ignore non blocking
      else if( opt.find( "--ignore-cuda" ) != string::npos )
      {
        options.ignoreCUDA = true;
      }

      // interval analysis TODO: complete optional?
      else if( opt.compare( string( "-c" ) ) == 0 )
      {
        if( ++i < argc )
        {
          options.analysisInterval = atoi( argv[i] );
        }
      }

      else if( opt.find( "--interval-analysis=" ) != string::npos )
      {
        options.analysisInterval = 
          atoi( opt.erase( 0, string( "--interval-analysis=" ).length() ).c_str() );
      }

        // if nothing matches 
      else
      {
        cout << "Unrecognized option " << opt << endl;
        return false;
      }
    }

    if( options.filename.length() == 0 )
    {
      cout << "No input file specified" << endl;
      return false;
    }
    
    if ( options.filename.find( ".otf2" ) == string::npos )
    {
      throw RTException( "No OTF2 input file specified (%s)", 
                         options.filename.c_str() );
    }

    return true;
  }

  bool
  Parser::init( int mpiRank, int argc, char** argv )
  throw( runtime_error)
  {

    bool success = false;

    setDefaultValues();

    success = processArgs( argc, argv );

    if( success == false )
    {
      return false;
    }

    // if all arguments have been parsed and an OTF2 output shall be generated
    if( success && options.createTraceFile )
    {
      setOutputDirAndFile();
      
      string traceEvtDir = pathToFile;
      if( !pathToFile.empty() )
      {
        traceEvtDir += string( "/");
      }
      
      traceEvtDir += outArchiveName;
      
      string file = traceEvtDir + string( ".otf2" );
      
      // if output .otf2 file or trace directory exist
      if( access( file.c_str(), F_OK ) == 0 ||
          access( traceEvtDir.c_str(), F_OK ) == 0 )
      {
        if( options.replaceCASITAoutput )
        {
          string rmCmd = string( "rm -rf " ) + traceEvtDir + string( "*" );
          
          UTILS_MSG( mpiRank == 0, "Output file does already exist, %s", rmCmd.c_str() );

          system( rmCmd.c_str() );
        }
        else // if the output file already exists, append unique number
        {
          int n = 2;
          stringstream num;
          num << n;

          // append underscore for new output directory
          traceEvtDir += string( "_" );

          // search for unique number to append 
          while( access( (traceEvtDir + num.str() + string( ".otf2" )).c_str(), F_OK ) == 0 ||
                 access( (traceEvtDir + num.str()).c_str(), F_OK ) == 0 )
          {
            n++;
            num.str( "" );
            num.clear();
            num << n;
          }

          outArchiveName = outArchiveName + string( "_" ) + num.str();

          UTILS_MSG( mpiRank == 0,
                     "Output file does already exist, changed to: %s",
                     outArchiveName.c_str() );
        }
      }
      else //output trace directory or file do not exist
      if( !pathToFile.empty() ) // and path is given
      {
        string mkdirCmd = string( "mkdir -p " ) + pathToFile;
          
        UTILS_MSG( mpiRank == 0, "Output directory %s does not exist, %s ", 
                   traceEvtDir.c_str(), mkdirCmd.c_str() );
          
        // create the output directory
        system( mkdirCmd.c_str() );
      }
      
      // if the path is empty (only output file given) -> writing in PWD
      if( pathToFile.empty() )
      {
        pathToFile = string( "." );
      }
    }

    if( !options.predictionFilter.empty() )
    {
      UTILS_MSG_NOBR( mpiRank == 0, "Evaluate runtime impact of " );

      size_t start = 0, end = 0;

      while ((end = options.predictionFilter.find(";", start)) != string::npos) 
      {
        if (end != start) 
        {
          string region = options.predictionFilter.substr(start, end - start);
          predictionFilter.push_back( region );
          UTILS_MSG( mpiRank == 0, "%s ", region.c_str() );
        }
        start = end + 1;
      }

      if (end != start) 
      {
        string region = options.predictionFilter.substr(start);
        predictionFilter.push_back(region);
        UTILS_MSG( mpiRank == 0, "%s ", region.c_str() );
      }
    }

    return true;
  }

  /*
   * 
   */
  void
  Parser::setOutputDirAndFile()
  {
    string otfFilename = options.outOtfFile;
    
    size_t charPos = otfFilename.find_last_of("/");
    
    // if only a file name is given
    if( charPos == string::npos )
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
  Parser::getProgramOptions()
  {
    return options;
  }

  void
  Parser::setDefaultValues()
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
    options.cpaLoopCheck = false;
    options.verbose = 0;
    options.topX = 20;
    options.predictionFilter = "";
    options.ignoreAsyncMpi = false;
    options.ignoreCUDA = false;
    options.createRatingCSV = true;
  }

}
