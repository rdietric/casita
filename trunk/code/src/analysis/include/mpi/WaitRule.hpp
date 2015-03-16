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
        /* applied at MPI_IRecv leave */
        if ( !node->isMPIWait( ) || !node->isLeave( ) )
        {
          return false;
        }

        std::cout << "[" << node->getStreamId( ) << "] WAIT " << node->getUniqueName( ) << " START" << std::endl;

        MPI_Status      status;
        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        MPI_Request*    request1       = commonAnalysis->getPendingMPIRequest( );
        if ( request1 )
        {
          MPI_Wait( request1, &status );
        }
        else
        { throw RTException( "Not enough pending MPI_Wait." );
        }
        /*        if(request1) */
        /*            delete request1; */

        std::cout << "[" << node->getStreamId( ) << "] WAIT second " << node->getUniqueName( ) << " START" << std::endl;

        MPI_Request* request2 = commonAnalysis->getPendingMPIRequest( );
        if ( request2 )
        {
          MPI_Wait( request2, &status );
        }
        else
        { throw RTException( "Not enough pending MPI_Wait." );
        }

        /* \TODO this causes an exception */
        /* if(request2) */
        /*    delete request2; */

        std::cout << "[" << node->getStreamId( ) << "] WAIT " << node->getUniqueName( ) << " DONE" << std::endl;

        return true;
      }
  };
 }
}
