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

namespace casita
{

 class AnalysisEngine;
 class GraphNode;

 class AbstractRule
 {
   public:

     AbstractRule( const char* name, int priority ) :
       priority( priority ),
       name( name )
     {

     }

     virtual
     ~AbstractRule( )
     {

     }

     const char*
     getName( )
     {
       return name;
     }

     int
     getPriority( )
     {
       return priority;
     }

     virtual bool
     applyRule( AnalysisEngine* analysis, GraphNode* node ) = 0;

   private:
     int priority;
     const char* name;
 };

}
