/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013 - 2014, 2016 - 2017
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

namespace casita
{
 typedef struct
 {
   bool        createTraceFile;
   bool        printCriticalPath;
   bool        cpaLoopCheck;
   bool        mergeActivities;
   bool        noErrors;
   bool        ignoreAsyncMpi;
   bool        ignoreCUDA;
   uint32_t    analysisInterval;
   bool        createRatingCSV;
   int         verbose;
   size_t      topX;
   int         eventsProcessed;
   int         memLimit;
   std::string outOtfFile;
   std::string filename;
   bool        replaceCASITAoutput;
 } ProgramOptions;
 
 
 class Parser
 {
   public:
     static Parser&
     getInstance();
     
     static int getVerboseLevel();

     bool
     init_with_boost( int argc, char** argv ) throw ( std::runtime_error );

     bool
     init( int mpiRank, int argc, char** argv ) throw ( std::runtime_error );
     
     ProgramOptions&
     getProgramOptions();
     
     void
     printHelp();
     
     /**
      * Return the OTF2 archive name (OTF2 file name without extension .otf2).
      */
     std::string
     getOutArchiveName()
     {
       return outArchiveName;
     }
     
     std::string
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
     endsWith( std::string const& str, std::string const& ext );
     
     void
     setDefaultValues();

     ProgramOptions options;
     std::string pathToFile;
     std::string outArchiveName;
 };

}
