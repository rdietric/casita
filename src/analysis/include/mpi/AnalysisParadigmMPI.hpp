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

      void
      handlePostLeave( GraphNode* node );
      
      void
      addPendingMPIRequest( uint64_t requestId, MPI_Request request );
      
      void
      addPendingMPIRequestId( uint64_t requestId, 
                              std::pair< MPI_Request, MPI_Request > requests);
      
      void
      waitForAllPendingMPIRequests( );
      
      /**
       * Safely complete MPI request that are associated with the request ID.
       * (Only if the request ID is the pending list.)
       * 
       * @param requestId OTF2 request for replayed non-blocking communication to be completed.
       * 
       * @return true, if the handle was found, otherwise false
       */
      bool
      waitForPendingMPIRequest( uint64_t requestId );

      typedef std::map< uint64_t, std::pair< MPI_Request, MPI_Request > > MPIRequestMap;

      private:
          
        /**< Map of uncompleted/pending MPI requests 
            (key: OTF2 request ID, value: MPI_Request handle */
        MPIRequestMap pendingMPIRequests;
        
        uint32_t mpiRank;
        uint32_t mpiSize;
  };

 }
}
