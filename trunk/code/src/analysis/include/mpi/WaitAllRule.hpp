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
        /* applied at MPI_WaitAll leave */
        if ( !node->isMPIWaitAll( ) || !node->isLeave( ) )
        {
          return false;
        }

        std::cout << "[" << node->getStreamId( ) << "] WAITALL " << node->getUniqueName( ) << " START" << std::endl;

        MPI_Status      status;
        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        uint64_t numberOfWaits         = commonAnalysis->getNumberOfPendingMPICommForWaitAll( );

        for ( uint64_t i = 0; i < numberOfWaits; i++ )
        {
          MPI_Request* request = commonAnalysis->getPendingMPIRequest( );
          if ( request )
          {
            MPI_Wait( request, &status );
          }
          else
          { throw RTException( "Not enough pending MPI_Wait." );
          }
        }

        return true;
      }
  };
 }
}
