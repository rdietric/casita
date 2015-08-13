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

#include "IMPIRule.hpp"
#include "AnalysisParadigmMPI.hpp"

namespace casita
{
 namespace mpi
 {

  class WaitAllRule :
    public IMPIRule
  {
    public:

      WaitAllRule( int priority ) :
        IMPIRule( "WaitAllRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        // applied at MPI_WaitAll leave
        if ( !node->isMPIWaitall( ) || !node->isLeave( ) )
        {
          return false;
        }

        // Wait for all requests that are associated to this MPI_Waitall
        analysis->getCommon()->getStream( node->getStreamId() )
                                       ->waitForPendingMPIRequests( node );

        return true;
      }
  };
 }
}
