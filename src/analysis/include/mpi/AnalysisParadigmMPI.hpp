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

#include <stack>
#include <map>
#include <vector>
#include <mpi.h>

#include "IAnalysisParadigm.hpp"

namespace casita
{
 namespace mpi
 {
  class AnalysisParadigmMPI :
    public IAnalysisParadigm
  {
    public:

      AnalysisParadigmMPI( AnalysisEngine* analysisEngine,
                           uint32_t        mpiRank,
                           uint32_t        mpiSize );

      virtual
      ~AnalysisParadigmMPI( );

      Paradigm
      getParadigm( );
      
      //void
      //handlePostEnter ( GraphNode* node );

      void
      handlePostLeave( GraphNode* node );

      private:
        
        uint32_t mpiRank;
        uint32_t mpiSize;
  };

 }
}
