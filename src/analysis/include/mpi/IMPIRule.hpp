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

#include <mpi.h>

#include "AbstractRule.hpp"
#include "AnalysisEngine.hpp"
#include "MPIRulesCommon.hpp"

namespace casita
{
 namespace mpi
 {
  class AnalysisParadigmMPI;

  class IMPIRule :
    public AbstractRule
  {
    public:
      IMPIRule( const char* name, int priority ) :
        AbstractRule( name, priority )
      {

      }

      virtual
      ~IMPIRule( )
      {
      }

      bool
      applyRule( AnalysisEngine* analysis, GraphNode* node )
      {
        return apply( (AnalysisParadigmMPI*)analysis->getAnalysis(
                        PARADIGM_MPI ), node );
      }

    protected:
      virtual bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node ) = 0;

  };
 }
}
