/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014,
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
 namespace cuda
 {
  class AnalysisParadigmCUDA;

  class ICUDARule :
    public AbstractRule
  {
    public:
      ICUDARule( const char* name, int priority ) :
        AbstractRule( name, priority )
      {

      }

      virtual
      ~ICUDARule( )
      {
      }

      bool
      applyRule( AnalysisEngine* analysis, GraphNode* node )
      {
        return apply( (AnalysisParadigmCUDA*)analysis->getAnalysisParadigm(
                        PARADIGM_CUDA ), node );
      }

    protected:
      virtual bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node ) = 0;

  };
 }
}
