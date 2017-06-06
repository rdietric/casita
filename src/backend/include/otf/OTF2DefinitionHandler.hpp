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

#include <otf2/otf2.h>

#include <map>

//#include "AnalysisEngine.hpp"

#define DEVICE_IDLE_STRING "deviceIdle"
#define DEVICE_COMPUTE_IDLE_STRING "deviceComputeIdle"

namespace casita
{
  namespace io
  {
    typedef struct
    {
      const char*     name;
      OTF2_Paradigm   paradigm;
      OTF2_RegionRole role;
    }RegionInfo;

    class OTF2DefinitionHandler
    {
      public:
        OTF2DefinitionHandler();
        virtual ~OTF2DefinitionHandler();
        
        uint64_t
        getTimerResolution();

        void
        setTimerResolution( uint64_t ticksPerSecond );

        uint64_t
        getTimerOffset();

        void
        setTimerOffset( uint64_t offset );

        uint64_t
        getTraceLength();

        void
        setTraceLength( uint64_t length );
        
        void
        storeString( uint32_t stringRef, const char* string );
        
        //!< get a new OTF2 string reference
        uint32_t
        getNewStringRef( const char* string );
        
        bool
        haveStringRef( uint32_t stringRef ) const;
        
        const char*
        getName( uint32_t stringRef );
        
        void
        addRegion( OTF2_RegionRef regionRef, OTF2_Paradigm paradigm, 
                   OTF2_RegionRole regionRole, OTF2_StringRef stringRef );
        
        uint32_t
        createNewRegion( const char* string, OTF2_Paradigm paradigm );
        
        RegionInfo&
        getRegionInfo( const uint32_t regionRef );
     
        const char*
        getRegionName( uint32_t id );
        
        void
        setInternalRegions();
        
        uint32_t
        getWaitStateRegionId() const;

        uint32_t
        getForkJoinRegionId() const;
        
      private:
        //<! pointer to the one and only analysis engine
        //AnalysisEngine* analysis;
        
        //!< resolution of the trace's timer (ticks per second)
        uint64_t timerResolution;
        uint64_t timerOffset;
        uint64_t traceLength;

        //!< maps OTF2 IDs to strings (global definitions), maps are ordered by key
        std::map< uint32_t, const char* > stringRefMap;
        
        //!< store required definitions for a function ID (OTF2 region reference)
        std::map< uint32_t, RegionInfo > regionInfoMap;
        
        //!< OTF2 region reference for internal wait state region
        uint32_t waitStateFuncId;
        
        //!< OTF2 region reference for internal Fork/Join
        uint32_t ompForkJoinRef;
        
        // maximum metric class and member IDs that has been read by the event reader
        uint32_t maxMetricClassId;
        uint32_t maxMetricMemberId;

        // maximum attribute ID that has been read by the event reader
        uint32_t maxAttributeId;
    };

  }
}
