/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013 - 2014, 2016 - 2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once


#include <string>
#include <stdexcept>
#include <vector>
#include <stdint.h>
#include <list>

using namespace std;

namespace casita
{
 typedef struct
 {
   string      inFileName;
   bool        createTraceFile;
   string      outFileName;
   bool        replaceCASITAoutput;
   bool        createRatingCSV;
   size_t      topX;
   string      predictionFilter;
   int         deviceIdle;
   int         linkKernels;
   int         nullStream;
   bool        printCriticalPath;
   bool        cpaLoopCheck;
   bool        mergeActivities;
   bool        noErrors;
   bool        ignoreAsyncMpi;
   bool        ignoreOffload;
   bool        blame4deviceIdle;
   bool        extendedBlame;
   uint32_t    analysisInterval;
   int         verbose;
   int         eventsProcessed;
 } ProgramOptions;
 
 
 class Parser
 {
   public:
     static Parser&
     getInstance();
     
     static ProgramOptions&
     getOptions();
     
     static int 
     getVerboseLevel();
     
     static vector < string >&
     getPredictionFilter();
     
     static bool 
     ignoreOffload();

     bool
     init( int mpiRank, int argc, char** argv ) throw ( std::runtime_error );
     
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
     
     string
     getSummaryFileName()
     {
       return pathToFile + string( "/" ) + outArchiveName + string( "_summary.txt" );
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
