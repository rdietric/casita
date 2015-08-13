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
        // applied at MPI_Wait leave
        if ( !node->isMPIWait( ) || !node->isLeave( ) )
        {
          return false;
        }
        
        EventStream::MPIIcommRecord* record = 
                (EventStream::MPIIcommRecord* ) node->getData( );
        
        if( NULL != record )
        {
          analysis->getCommon()->getStream( node->getStreamId() )
                               ->waitForPendingMPIRequest( record->requestId );
        }
        else
        {
          std::cerr << "[" << node->getStreamId() << "] MPI_Wait rule: record is NULL!" << std::endl;
        }

        return true;
      }
  };
 }
}
