/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "AbstractRule.hpp"
#include "AnalysisEngine.hpp"

namespace casita
{
 namespace opencl
 {
  class AnalysisParadigmOpenCL;

  class IOpenCLRule :
    public AbstractRule
  {
    public:
      IOpenCLRule( const char* name, int priority ) :
        AbstractRule( name, priority )
      {

      }

      virtual
      ~IOpenCLRule( )
      {
      }

      bool
      applyRule( AnalysisEngine* analysis, GraphNode* node )
      {
        return apply( (AnalysisParadigmOpenCL*)analysis->getAnalysisParadigm(
                        PARADIGM_OPENCL ), node );
      }

    protected:
      virtual bool
      apply( AnalysisParadigmOpenCL* analysis, GraphNode* node ) = 0;

  };
 }
}
