/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2016-2019,
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
#include "utils/Utils.hpp"
#include "Parser.hpp"

namespace casita
{
  static bool replaceSubstr(std::string& str, const std::string& from, const std::string& to) {
    size_t start_pos = str.rfind(from);
    if(start_pos == std::string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
  }
  
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
  
  ProgramOptions&
  Parser::getOptions()
  {
    return Parser::getInstance().options;
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
  
  bool
  Parser::ignoreOffload()
  {
    return Parser::getInstance().options.ignoreOffload;
  }

  void
  Parser::printHelp()
  {
    cout << "Usage: casita <otf2-file> [options]\n" << endl;
    cout << " -h [--help]              print help message" << endl;
    cout << " -i [--input=]NAME        input OTF2 file" << endl;
    cout << " -o [--output=]NAME       output OTF2 file" << endl;
    cout << " -r [--replace]           replace CASITA trace and summary file" << endl;
    cout << " -v [--verbose=]INTEGER   verbosity level" << endl;
    cout << "    [--no-csv]            write rating to .csv file" << endl;
    cout << " -t  --top=INTEGER        print top optimization candidates" << endl;
    cout << " -f  --filter=List        filter regions (ignored in analysis and output trace)" << endl;
    cout << "     --idle=[0,1,2,3]     write device idle regions to output trace" << endl
         << "                          (0=no, 1=device idle(default), 2=compute idle, 3=both)" << endl;
    cout << "     --blame4device-idle  blame the host for not keeping the device computing" << endl;
    cout << "     --propagate-blame    Wait states propagate their blame" << endl;
    cout << " -e [--extended-blame]    divide blame into different types" << endl;
    cout << " -l  --link-kernels[=0,1,2]  create inter-stream kernel dependencies (default: off)" << endl
         << "                          (0=off, 1=link kernels, 2=link overlapping kernels)" << endl;
    cout << "     --nullstream=INT     use null stream semantic for the given stream" << endl;
    cout << " -p [--path]              print critical paths" << endl;
    cout << "     --force-cp-check[=UINT] check for circular loops during process-local" << endl; 
    cout << "                             critical path detection (value determines depth)" << endl;
    cout << "     --no-errors          ignore non-fatal errors" << endl;
    cout << "     --ignore-impi        handle non-blocking MPI functions as CPU functions" << endl;
    cout << "     --ignore-offloading  handle CUDA/OpenCL functions as CPU functions" << endl;
    cout << "     --ignore-cuda-events do not handle CUDA-event-related functions" << endl;
    cout << " -c  --interval-analysis=UINT  run analysis in intervals (between global MPI" << endl
         << "                          collectives) to reduce memory footprint. The value" << endl
         << "                          (default: 64) sets the number of pending graph nodes" << endl
         << "                          before an analysis run is started." << endl;
  }

  bool
  Parser::processArgs( int mpiRank, int argc, char** argv )
  {
    string opt;
    for( int i = 1; i < argc; i++ )
    {
      opt = string( argv[i] );

      // if the first argument does not start with a dash interpret as input file
      if( i == 1 && opt.find_first_of( "-" ) != 0 )
      {
        options.inFileName = string( argv[1] );
      }
      else if( opt.compare( string( "-i" ) ) == 0 ||
               opt.compare( string( "--input" ) ) == 0 )
      {
        if( ++i < argc && argv[i][0] != '-' )
        {
          options.inFileName = string( argv[i] );
        }
      }
      else if( opt.find( "--input=" ) != string::npos )
      {
        options.inFileName = opt.erase( 0, string( "--input=" ).length() );
      }

        //  output file
      else if( opt.compare( string( "-o" ) ) == 0 ||
               opt.compare( string( "--output" ) ) == 0 )
      {
        // if argument to ouptput does not start with a dash, interpret it as 
        // output trace file name
        if( ++i < argc && argv[i][0] != '-' )
        {
          options.outFileName = string( argv[i] );
        }
        else
        {
          i--;
        }
        
        // if no output file is given use the default (casita.otf2 next to traces.otf2)
        options.createTraceFile = true;
      }
      else if( opt.find( "--output=" ) != string::npos )
      {
        options.outFileName = opt.erase( 0, string( "--output=" ).length() );
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
        if( ++i < argc && argv[i][0] != '-' )
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

      // do not create a csv file for the rating
      else if( opt.find( "--no-csv" ) != string::npos )
      {
        options.createRatingCSV = false;
      }


      // print top X activities
      else if( opt.compare( string( "-t" ) ) == 0 ||
               opt.compare( string( "--top" ) ) == 0 )
      {
        if( ++i < argc && argv[i][0] != '-' )
        {
          options.topX = atoi( argv[i] );
        }
      }
      else if( opt.find( "--top=" ) != string::npos )
      {
        options.topX = atoi( opt.erase( 0, string( "--top=" ).length() ).c_str() );
      }
      
      // get prediction filter
      else if( opt.compare( string( "-f" ) ) == 0 )
      {
        if( ++i < argc && argv[i][0] != '-' )
        {
          options.predictionFilter = string( argv[i] );
        }
      }
      else if( opt.find( "--filter=" ) != string::npos )
      {
        options.predictionFilter = opt.erase( 0, string( "--filter=" ).length() );
      }
      
      // write device [compute] idle
      else if( opt.find( "--idle=" ) != string::npos )
      {
        options.deviceIdle = atoi( opt.erase( 0, string( "--idle=" ).length() ).c_str() );
      }

      // print path
      else if( opt.compare( string( "-p" ) ) == 0 ||
               opt.find( "--path" ) != string::npos )
      {
        options.printCriticalPath = true;
      }
      
      else if( opt.find( "--force-cp-check=" ) != string::npos )
      {
        options.cpaLoopCheck = atoi( opt.erase( 0, string( "--force-cp-check=" ).length() ).c_str() );
        UTILS_MSG( mpiRank == 0, 
                   "[Force loop check in critical-path detection (depth: %" PRIu32 ")]",
                   options.cpaLoopCheck );
      }
      
      // detect and avoid circular loops in process-local CPA
      else if( opt.find( "--force-cp-check" ) != string::npos )
      {
        if( ++i < argc && argv[i][0] != '-' )
        {
          // set the user specified value
          options.cpaLoopCheck = atoi( argv[i] );
        }
        else
        {
          options.cpaLoopCheck = 10;
          i--;
        }
        UTILS_MSG( mpiRank == 0, 
                   "[Force loop check in critical-path detection (depth: %" PRIu32 ")]",
                   options.cpaLoopCheck );
      }

      // no error
      else if( opt.find( "--no-errors=" ) != string::npos )
      {
        options.noErrors = true;
      }

      // do not apply rules for non-blocking MPI
      else if( opt.find( "--ignore-impi" ) != string::npos )
      {
        options.ignoreAsyncMpi = true;
        
        UTILS_MSG( mpiRank == 0, "[Ignore non-blocking MPI calls.]" );
      }
      
      // do not run offloading analysis
      else if( opt.find( "--ignore-offloading" ) != string::npos )
      {
        options.ignoreOffload = true;
        
        UTILS_MSG( mpiRank == 0, "[Ignore offloading events.]" );
      }
      
      // do not run offloading analysis
      else if( opt.find( "--ignore-cuda-events" ) != string::npos )
      {
        options.ignoreCUDAevents = true;
        
        UTILS_MSG( mpiRank == 0, "[Ignore CUDA events.]" );
      }
      
      // enable blaming host activities for not keeping the device busy
      else if( opt.find( "--blame4device-idle" ) != string::npos )
      {
        options.blame4deviceIdle = true;
        UTILS_MSG( mpiRank == 0, "[Blame host streams for device idle.]" )
      }
      
      // enable blaming host activities for not keeping the device busy
      else if( opt.find( "--propagate-blame" ) != string::npos )
      {
        options.propagateBlame = true;
        UTILS_MSG( mpiRank == 0, "[Propagate blame.]" )
      }
      
      // print path
      else if( opt.compare( string( "-e" ) ) == 0 ||
               opt.find( "--extended-blame" ) != string::npos )
      {
        options.extendedBlame = true;
      }
      
      // create dependencies between kernels from different streams
      else if( opt.compare( string( "-l" ) ) == 0 /*|| 
               opt.find( "--link-kernels" ) != string::npos*/ )
      {
        options.linkKernels = 1;
        
        UTILS_MSG( mpiRank == 0, "[Link non-overlapping device kernels.]" );
      }
      
      // try to link overlapping kernels
      else if( opt.find( "--link-kernels=" ) != string::npos )
      {
        options.linkKernels = 
          atoi( opt.erase( 0, string( "--link-kernels=" ).length() ).c_str() );
        
        UTILS_MSG( mpiRank == 0, "[Link device kernels (mode %d).]", options.linkKernels );
      }
      
      // handle the given stream (by ID) as null stream
      else if( opt.find( "--nullstream=" ) != string::npos )
      {
        options.nullStream = 
          atoi( opt.erase( 0, string( "--nullstream=" ).length() ).c_str() );
        
        UTILS_MSG( mpiRank == 0, 
                   "[Handle device stream %d as default stream.]", 
                   options.nullStream );
      }

      // interval analysis
      else if( opt.compare( string( "-c" ) ) == 0 )
      {
        if( ++i < argc && argv[i][0] != '-'  )
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
        UTILS_MSG( mpiRank == 0, "Unrecognized option %s", opt.c_str() );
        return false;
      }
    }

    if( options.inFileName.length() == 0 )
    {
      UTILS_MSG( mpiRank == 0, "No input file specified" );
      return false;
    }
    
    if ( options.inFileName.find( ".otf2" ) == string::npos )
    {
      throw RTException( "No OTF2 input file specified (%s)", 
                         options.inFileName.c_str() );
    }

    return true;
  }

  bool
  Parser::init( int mpiRank, int argc, char** argv )
  throw( runtime_error)
  {

    bool success = false;

    setDefaultValues();

    success = processArgs( mpiRank, argc, argv );

    if( success == false )
    {
      return false;
    }

    // if all arguments have been parsed and an OTF2 output shall be generated
    if( success && ( options.createTraceFile || options.createRatingCSV ) )
    {
      setOutputDirAndFile();
      
      string traceEvtDir = pathToFile;
      if( !pathToFile.empty() )
      {
        traceEvtDir += string( "/" );
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

  /**
   * Parse the output file. 
   */
  void
  Parser::setOutputDirAndFile()
  {
    // do not allow to replace the original trace
    if( options.outFileName == options.inFileName )
    {
      UTILS_WARNING( "The input trace cannot be replaced! "
                     "Using default name 'casita' instead." );
      options.outFileName = "casita.otf2";
    }
    
    // if the out file name is not given simply use the input file and replace 
    // the last occurrence of traces with casita
    if( options.outFileName.empty() )
    {
      options.outFileName = options.inFileName;
      if( !replaceSubstr( options.outFileName, "traces", "casita" ) )
      {
        options.outFileName = "casita.otf2";
        UTILS_WARNING( "Writing output trace in current working directory!" );
      }
    }
    
    size_t charPos = options.outFileName.find_last_of("/");
    
    // if only a file name is given
    if( charPos == string::npos )
    {
      outArchiveName = options.outFileName;
    }
    else // path (relative or absolute) with directory given
    {
      pathToFile     = options.outFileName.substr( 0, charPos );
      outArchiveName = options.outFileName.substr( charPos + 1 );
    }
    
    // remove the .otf2 extension from OTF2 archive name
    charPos = outArchiveName.find_last_of( "." );
    outArchiveName = outArchiveName.substr( 0, charPos );
    
    //UTILS_OUT( "Path %s, File: %s", pathToFile.c_str(), outOtf2ArchiveName.c_str() );
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
    options.inFileName = "";
    options.mergeActivities = true;
    options.noErrors = false;
    options.analysisInterval = 64;
    //options.outOtfFile = "casita.otf2";
    options.replaceCASITAoutput = false;
    options.printCriticalPath = false;
    options.cpaLoopCheck = 0;
    options.verbose = 0;
    options.topX = 20;
    options.predictionFilter = "";
    options.deviceIdle = 1;
    options.linkKernels = 0;
    options.nullStream = -1;
    options.ignoreAsyncMpi = false;
    options.ignoreOffload = false;
    options.ignoreCUDAevents = false;
    options.blame4deviceIdle = false;
    options.propagateBlame = false;
    options.extendedBlame = false;
    options.createRatingCSV = true;
  }

}
