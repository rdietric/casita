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
#include <stdlib.h>
#include <unistd.h> //for getcwd, access
#include "Parser.hpp"

namespace casita
{


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

 Parser&
 Parser::getInstance( )
 {
   static Parser instance;
   return instance;
 }

 void
 Parser::printHelp(){
   std::cout << "Usage: casita <otf-file> [options]\n" << std::endl;
   std::cout << "  -h [--help]           print help message" << std::endl;
   std::cout << "  -i [--input]   arg    input OTF file" << std::endl;
   std::cout << "  -o [--output]  arg    output OTF file" << std::endl;
   std::cout << "  -v [--verbose] arg    verbosity level\n" << std::endl;
 }


 bool
 Parser::setProgramOptions(int argc, char** argv){
  
   optionPrefixMap["-i"] = INPUT;
   optionPrefixMap["--input"] = INPUT;
   optionPrefixMap["-o"] = OUTPUT;
   optionPrefixMap["--output"] = OUTPUT;
   optionPrefixMap["-h"] = HELP;
   optionPrefixMap["--help"]=HELP;
   optionPrefixMap["-v"] = VERBOSE;
   optionPrefixMap["--verbose"] = VERBOSE;

   
   if (argc > 2 && argc%2 == 1)
     {
       
       for (int i = 1;i < argc;i += 2){
	 optionsMap[ argv[i]] = argv[i+1];
       }
     }
       
   else if (argc == 2 && endsWith( std::string(argv[1]) ,".otf2")){
     optionsMap["-i"]= argv[1];
   }
       
   else {
     printHelp();
     return false;
   }

   std::string filePath;

   for (OptionsMap::iterator it=optionsMap.begin();it != optionsMap.end();it++)
     
     {
       switch(optionPrefixMap[it->first])
	 {
       
	 case INPUT:
	   options.filename = it->second;
	   break;
       
	 case OUTPUT:
	   options.outOtfFile = it->second;
	   options.createOTF = true;

	   break;

	 case VERBOSE:
	   options.verbose = atoi(it->second);
	   break;

	 case HELP:
	   printHelp();
	   return false;
	 }
     }
     
   return true;
 }

 


 bool
 Parser::init( int argc, char** argv ) throw ( std::runtime_error )
 {
   bool noSummary = false;
   bool noHelp = true;

   options.createOTF         = false;
   options.eventsProcessed   = 0;
   options.filename          = "";
   options.mergeActivities   = true;
   options.noErrors          = false;
   options.outOtfFile        = "";
   options.printCriticalPath = false;
   options.verbose           = 0;
   options.ignoreAsyncMpi    = false;
 
   try
     {
       noHelp = setProgramOptions(argc,argv);
     }
   catch (...)
     {
       return false;
     }
    
   return noHelp;
     
 }
 ProgramOptions&
 Parser::getProgramOptions( )
 {
   return options;
 }

}
