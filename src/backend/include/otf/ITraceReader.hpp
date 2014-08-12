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

#include <string>
#include <map>
#include <vector>
#include "IKeyValueList.hpp"

namespace casita
{
 namespace io
 {

  class ITraceReader;

  enum MPIType
  {
    MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_ONEANDALL
  };

  typedef void ( *HandleEnter )( ITraceReader* reader, uint64_t time,
                                 uint32_t functionId, uint64_t processId,
                                 IKeyValueList* list );
  typedef void ( *HandleLeave )( ITraceReader* reader, uint64_t time,
                                 uint32_t functionId, uint64_t processId,
                                 IKeyValueList* list );
  typedef void ( *HandleDefProcess )( ITraceReader* reader, uint32_t stream,
                                      uint64_t processId, uint64_t parentId,
                                      const char* name,
                                      IKeyValueList* list, bool isCUDA,
                                      bool isCUDANull );
  typedef void ( *HandleProcessMPIMapping )( ITraceReader* reader,
                                             uint64_t processId,
                                             uint32_t mpiRank );
  typedef void ( *HandleDefFunction )( ITraceReader* reader, uint64_t streamId,
                                       uint32_t functionId, const char* name,
                                       uint32_t functionGroupId );
  typedef void ( *HandleDefKeyValue )( ITraceReader* reader, uint64_t streamId,
                                       uint32_t key, const char* name,
                                       const char* description );
  typedef void ( *HandleMPIComm )( ITraceReader* reader, MPIType mpiType,
                                   uint64_t processId, uint64_t partnerId,
                                   uint32_t root, uint32_t tag );
  typedef void ( *HandleMPICommGroup )( ITraceReader* reader, uint32_t group,
                                        uint32_t numProcs,
                                        const uint64_t* procs );

  class ITraceReader
  {
    public:

      ITraceReader( void* userData ) :
        handleEnter( NULL ),
        handleLeave( NULL ),
        handleDefProcess( NULL ),
        handleDefFunction( NULL ),
        handleDefKeyValue( NULL ),
        handleProcessMPIMapping( NULL ),
        handleMPIComm( NULL ),
        handleMPICommGroup( NULL ),
        userData( userData )
      {
      }

      virtual
      ~ITraceReader( )
      {
      }

      virtual void
      open( const std::string otfFilename, uint32_t maxFiles ) = 0;

      virtual void
      close( ) = 0;

      virtual void
      readEvents( ) = 0;

      virtual void
      readEventsForProcess( uint64_t id ) = 0;

      virtual void
      readDefinitions( ) = 0;

      virtual void
      readCommunication( ) = 0;

      virtual std::string
      getKeyName( uint32_t id ) = 0;

      virtual std::string
      getFunctionName( uint32_t id ) = 0;

      virtual std::string
      getProcessName( uint64_t id ) = 0;

      virtual std::vector< uint32_t >
      getKeys( const std::string keyName ) = 0;

      virtual int32_t
      getFirstKey( const std::string keyName ) = 0;

      virtual uint64_t
      getTimerResolution( ) = 0;

      void*
      getUserData( )
      {
        return userData;
      }

      HandleEnter handleEnter;
      HandleLeave handleLeave;
      HandleDefProcess handleDefProcess;
      HandleDefFunction handleDefFunction;
      HandleDefKeyValue handleDefKeyValue;
      HandleProcessMPIMapping handleProcessMPIMapping;
      HandleMPIComm handleMPIComm;
      HandleMPICommGroup handleMPICommGroup;

    private:
      void* userData;
  };
 }
}
