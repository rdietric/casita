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

// #include <boost/program_options/options_description.hpp>

#include <string>
#include <stdexcept>
#include <vector>
#include <map>
#include <stdint.h>
#include <list>

namespace casita
{

  
  enum OptionPrefix 
    {
      INPUT , OUTPUT , HELP,VERBOSE
    };

  typedef std::map < std::string, char* >  OptionsMap;
  typedef std::map < std::string, OptionPrefix > OptionPrefixMap;

 typedef struct
 {
   bool        createOTF;
   bool        printCriticalPath;
   bool        mergeActivities;
   bool        noErrors;
   bool        ignoreAsyncMpi;
   int         verbose;
   int         eventsProcessed;
   int         memLimit;
   std::string outOtfFile;
   std::string filename;
 } ProgramOptions;

 class Parser
 {
   public:
     static Parser&
     getInstance( );

     bool
     init( int argc, char** argv ) throw ( std::runtime_error );

     ProgramOptions&
     getProgramOptions( );

     bool
     setProgramOptions(int argc, char** argv);
     
     void
     printHelp();

   OptionsMap optionsMap;
   OptionPrefixMap optionPrefixMap;

   private:
     Parser( );

   
     Parser( Parser& cc );

     bool
     endsWith( std::string const& str, std::string const& ext );

     ProgramOptions options;

 };

}
