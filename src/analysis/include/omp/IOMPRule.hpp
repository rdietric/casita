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
#include "OMPRulesCommon.hpp"

namespace casita
{
 namespace omp
 {
  class AnalysisParadigmOMP;

  class IOMPRule :
    public AbstractRule
  {
    public:
      IOMPRule( const char* name, int priority ) :
        AbstractRule( name, priority )
      {

      }

      virtual
      ~IOMPRule( )
      {
      }

      bool
      applyRule( AnalysisEngine* analysis, GraphNode* node )
      {
        return apply( (AnalysisParadigmOMP*)analysis->getAnalysis(
                        PARADIGM_OMP ), node );
      }

    protected:
      virtual bool
      apply( AnalysisParadigmOMP* analysis, GraphNode* node ) = 0;

  };
 }
}
