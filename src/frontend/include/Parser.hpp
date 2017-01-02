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
 */

#pragma once

#if defined( BOOST_AVAILABLE ) 
#include <boost/program_options/options_description.hpp>
#endif


#include <string>
#include <stdexcept>
#include <vector>
#include <stdint.h>
#include <list>

using namespace std;

namespace casita
{
 #if defined( BOOST_AVAILABLE )
 namespace po = boost::program_options;
 #endif
 
 typedef struct
 {
   string      filename;
   bool        createTraceFile;
   string      outOtfFile;
   bool        replaceCASITAoutput;
   bool        createRatingCSV;
   size_t      topX;
   string      predictionFilter;
   bool        printCriticalPath;
   bool        mergeActivities;
   bool        noErrors;
   bool        ignoreAsyncMpi;
   bool        ignoreCUDA;
   uint32_t    analysisInterval;
   int         verbose;
   int         eventsProcessed;
 } ProgramOptions;
 
 
 class Parser
 {
   public:
     static Parser&
     getInstance( );
     
     static int 
     getVerboseLevel();
     
     static vector < string >&
     getPredictionFilter();

     bool
     initialize( int mpiRank, int argc, char** argv ) throw ( std::runtime_error );
     
     ProgramOptions&
     getProgramOptions();
     
     void
     printHelp();
     
     /**
      * Return the OTF2 archive name (OTF2 file name without extension .otf2).
      */
     string
     getOutArchiveName()
     {
       return outArchiveName;
     }
     
     string
     getPathToFile()
     {
         return pathToFile;
     }
     
   private:
     Parser();

     Parser( Parser& cc );
     
     void    
     setOutputDirAndFile();
     
     bool
     processArgs( int argc, char** argv);
     
     bool
     endsWith( string const& str, string const& ext );
     
     void
     setDefaultValues();

     ProgramOptions options;
     string pathToFile;
     string outArchiveName;
     vector < string > predictionFilter;
 };

}
