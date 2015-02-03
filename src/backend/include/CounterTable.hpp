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

#include <stdint.h>
#include <map>
#include <set>
#include <otf2/otf2.h>

namespace casita
{
 enum CounterType
 {
   CTR_BLAME = 0,                /* local blame for visualization */
   CTR_WAITSTATE,                /* local waiting time */
   CTR_CRITICALPATH,
   CTR_OMP_REGION_ID,
   CTR_OMP_PARENT_REGION_ID,
   CTR_OMP_IGNORE_BARRIER,

   CTR_NUM_DEFAULT_CTRS = 6
 };

 typedef struct
 {
   CounterType     type;
   const char*     name;
   bool            hasDefault;
   bool            isInternal;
   uint64_t        defaultValue;
   OTF2_MetricMode otfMode;
 } CtrTableEntry;

 static const CtrTableEntry COUNTER_TABLE[] =
 {
   { CTR_BLAME, "Blame", true, false, 0,
     OTF2_METRIC_ABSOLUTE_LAST },
   { CTR_WAITSTATE, "Waiting Time", true, false, 0,
     OTF2_METRIC_ABSOLUTE_LAST },
   { CTR_CRITICALPATH, "Critical Path", true, false, 0,
     OTF2_METRIC_ABSOLUTE_LAST },
   { CTR_OMP_REGION_ID, "OpenMP 4.0 Region ID", true, true, 0,
     OTF2_METRIC_ABSOLUTE_POINT },
   { CTR_OMP_PARENT_REGION_ID, "OpenMP 4.0 Parent Region ID", true, true, 0,
     OTF2_METRIC_ABSOLUTE_POINT },
   { CTR_OMP_IGNORE_BARRIER, "OpenMP 4.0 Collapsed Barrier", true, true, 0,
     OTF2_METRIC_ABSOLUTE_POINT }
 };

 class CounterTable
 {
   private:
     typedef std::map< uint32_t, CtrTableEntry* > CtrEntryMap;

   public:

     typedef std::set< uint32_t > CtrIdSet;

     CounterTable( ) :
       maxCtrId( 0 )
     {

     }

     virtual
     ~CounterTable( )
     {
       for ( CtrEntryMap::const_iterator iter = counters.begin( );
             iter != counters.end( ); ++iter )
       {
         delete iter->second;
       }
     }

     void
     addDefaultCounter( uint32_t ctrId, CounterType ctrType )
     {
       addCounter( ctrId, ctrType,
                   COUNTER_TABLE[ctrType].name,
                   COUNTER_TABLE[ctrType].hasDefault,
                   COUNTER_TABLE[ctrType].isInternal,
                   COUNTER_TABLE[ctrType].defaultValue,
                   COUNTER_TABLE[ctrType].otfMode );
     }

     void
     addCounter( uint32_t ctrId, CounterType ctrType,
                 const char* name, bool hasDefault, bool isInternal,
                 uint64_t defaultValue, OTF2_MetricMode otfMode )
     {
       CtrTableEntry* entry = new CtrTableEntry( );
       entry->type         = ctrType;
       entry->name         = name;
       entry->hasDefault   = hasDefault;
       entry->isInternal   = isInternal;
       entry->defaultValue = (uint64_t)defaultValue;
       entry->otfMode      = otfMode;

       counters[ctrId]     = entry;
       ctrIDs.insert( ctrId );

       maxCtrId = std::max( maxCtrId, ctrId );
     }

     CtrTableEntry*
     getCounter( uint32_t ctrId ) const
     {
       CtrEntryMap::const_iterator iter = counters.find( ctrId );
       if ( iter != counters.end( ) )
       {
         return iter->second;
       }
       else
       {
         return NULL;
       }
     }

     uint32_t
     getCtrId( CounterType ctrType ) const
     {
       for ( CtrEntryMap::const_iterator iter = counters.begin( );
             iter != counters.end( ); ++iter )
       {
         if ( iter->second->type == ctrType )
         {
           return iter->first;
         }
       }
       return 0;
     }

     uint32_t
     getNewCtrId( )
     {
       /* starting with 0 (Ids in OTF2 need to start with 0) */
       return maxCtrId++;
     }

     const CtrIdSet&
     getAllCounterIDs( ) const
     {
       return ctrIDs;
     }

   private:
     uint32_t    maxCtrId;
     CtrEntryMap counters;
     CtrIdSet    ctrIDs;
 };
}
