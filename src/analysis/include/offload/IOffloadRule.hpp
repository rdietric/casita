/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2017,
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
 namespace offload
 {
  class AnalysisParadigmOffload;

  class IOffloadRule :
    public AbstractRule
  {
    public:
      IOffloadRule( const char* name, int priority ) :
        AbstractRule( name, priority )
      {

      }

      virtual
      ~IOffloadRule()
      {
      }

      bool
      applyRule( AnalysisEngine* analysis, GraphNode* node )
      {
        return apply( (AnalysisParadigmOffload*)analysis->getAnalysis(
                      PARADIGM_OFFLOAD ), node );
      }

    protected:
      virtual bool
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* node ) = 0;

  };
 }
}
