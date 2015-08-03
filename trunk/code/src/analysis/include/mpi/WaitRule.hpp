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

  class WaitRule :
    public IMPIRule
  {
    public:

      WaitRule( int priority ) :
        IMPIRule( "WaitRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmMPI* analysis, GraphNode* node )
      {
        /* applied at MPI_Wait leave */
        if ( !node->isMPIWait( ) || !node->isLeave( ) )
        {
          return false;
        }
        
        // MPI request handle is stored in the data field of the MPI_Wait leave node
        uint64_t* requestID = (uint64_t* ) node->getData();

        analysis->waitForPendingMPIRequest( *requestID );
        
        //TODO: free data?

        return true;
      }
  };
 }
}
