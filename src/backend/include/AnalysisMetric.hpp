/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2015,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * This file defines a table for several counters that are computed and written
 * to an OTF2 File
 *
 */

#pragma once

#include <stdint.h>

#include <map>
#include <set>
#include <limits>

#include <otf2/otf2.h>

#define BLAME_COUNTER 1

namespace casita
{
  enum MetricType
  {
    BLAME = 0,         // amount of caused waiting time
    WAITING_TIME = 1,  // waiting time of a region
    CRITICAL_PATH = 2, // is a location/stream on the critical path
    OMP_REGION_ID = 3,        // internal
    OMP_PARENT_REGION_ID = 4, // internal
    OMP_IGNORE_BARRIER = 5,   // internal

    NUM_DEFAULT_METRICS = 6 // has to be the number of elements in METRIC_TABLE
  };
  
  enum MetricMode
  {
    ATTRIBUTE = 0,
    COUNTER_ABSOLUT_NEXT,
    COUNTER_ABSOLUT_LAST,
    METRIC_MODE_UNKNOWN
  };
 
 typedef struct
 {
   MetricType  type;
   const char* name;
   const char* description;
   MetricMode  metricMode;
   bool        isInternal;
   uint32_t    otf2DefId;
 } MetricEntry;
 
 // Definition of metrics and attributes:
 //
 // Blame is computed for each CPU event based on the blame that has been
 // assigned to edges during the analysis phase. It is currently written as
 // counter, as we use the time difference between the current and the last 
 // written event. Hence, we do not need to track the region stack.
 //
 // Waiting time can be assigned to regions as attribute, as it will only occur
 // on events that are also graph nodes (paradigms events). Regions that are 
 // wait states typically do not have a nested region. For cuCtxSynchronize the 
 // BUFFER_FLUSH region is nested. Use inclusive mode in Vampir, when creating 
 // a metric from this attribute. 
 //
 // The critical path is written as a counter in absolute next mode. It is 
 // written, whenever a stream changes between being critical or not. Compared
 // to the blame counter it does not need to be written with every event. 
 //
 // This table does not define the OTF2 definition ID!
 static const MetricEntry METRIC_TABLE[] =
 {
#if defined(BLAME_COUNTER)
   { BLAME,         "Blame",         
                    "Amount of caused waiting time", 
                    COUNTER_ABSOLUT_LAST, false },
#else
   { BLAME,         "Blame",         
                    "Amount of caused waiting time", ATTRIBUTE,            false },
#endif
   { WAITING_TIME,  "Waiting Time",  
                    "Time in a wait state",          ATTRIBUTE,            false },
   { CRITICAL_PATH, "Critical Path", 
                    "On the critical path boolean",  COUNTER_ABSOLUT_NEXT, false },
   // internal metrics
   { OMP_REGION_ID,        "OpenMP 4.0 Region ID",         
                           "", METRIC_MODE_UNKNOWN, true },
   { OMP_PARENT_REGION_ID, "OpenMP 4.0 Parent Region ID",  
                           "", METRIC_MODE_UNKNOWN, true },
   { OMP_IGNORE_BARRIER,   "OpenMP 4.0 Collapsed Barrier", 
                           "", METRIC_MODE_UNKNOWN, true }
 };

 class AnalysisMetric
 {
    private:
      //! < key: metric type, value: OTF2 attribute or metric ID/ref
      typedef std::map< MetricType, uint32_t > MetricTypeIdMap;

    public:

      typedef std::set< MetricType > MetricIdSet;

     // Construct of class AnalysisMetric (used in GraphEngine)
     // \todo: make singleton?
     AnalysisMetric( ) :
       maxCtrId( 0 ),
       maxAttrId( 0 )
     {

     }

     virtual
     ~AnalysisMetric( )
     {
     }
     
     const MetricEntry*
     getMetric( MetricType metricId ) const
     {
       if( metricId < NUM_DEFAULT_METRICS )
       {
         return &(METRIC_TABLE[metricId]);
       }
       else
       {
         return NULL;
       }
     }
     
     /**
      * Get the OTF2 metric ID for the given metric type.
      * 
      * @param metric internal metric type
      * @return OTF2 metric ID
      */
     uint32_t
     getMetricId( MetricType metric )
     {
       return otf2Ids[ metric ];
     }

     uint32_t
     getNewCounterId( )
     {
       // starting with 0 (Ids in OTF2 need to start with 0)
       return maxCtrId++;
     }
     
      uint32_t
      getNewAttributeId( )
      {
        return maxAttrId++;
      }

     const MetricIdSet&
     getAllCounterIds( ) const
     {
       return ctrIDs;
     }
     
     const MetricIdSet&
     getAllMetricIds( ) const
     {
       return metricIds;
     }
     
      void
      addAttributeId( uint32_t attrId )
      {
        maxAttrId = std::max( maxAttrId, attrId );
      }
      
      /**
       * Create a new unique OTF2 metric ID and add it as value to the internal
       * metric type.
       * 
       * @param metricId internal metric type
       * 
       * @return the new OTF2 metric or attribute ID
       */
      uint32_t
      newOtf2Id( MetricType metricId )
      {
        const MetricEntry* entry = getMetric( metricId );
        if( entry->metricMode == ATTRIBUTE )
        {          
          maxAttrId++;
          
          otf2Ids[metricId] = maxAttrId;
          
          //attributeIds.insert( metricId );
          metricIds.insert( metricId );
          
          return maxAttrId;
        }
        else if( entry->metricMode != METRIC_MODE_UNKNOWN )
        {          
          maxCtrId++;
          
          otf2Ids[metricId] = maxCtrId;
          
          ctrIDs.insert( metricId );
          metricIds.insert( metricId );
          
          return maxCtrId;
        }
        else
        {
          return std::numeric_limits< uint32_t >::max( );
        }
      }

   private:
     uint32_t        maxCtrId;
     uint32_t        maxAttrId;
     MetricTypeIdMap otf2Ids;
     MetricIdSet     ctrIDs;
     MetricIdSet     metricIds;
 };
}