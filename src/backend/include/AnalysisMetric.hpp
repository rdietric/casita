/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2015-2018,
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

namespace casita
{
  enum MetricType
  {
    BLAME = 0,         // amount of caused waiting time
    WAITING_TIME = 1,  // waiting time of a region
    CRITICAL_PATH = 2, // is a location/stream on the critical path
    
    NUM_OUTPUT_METRICS = 3,
    
    BLAME4IDLE = 4,    // blame received for not keeping the device busy (internal)
    OMPT_REGION_ID = 5,        // internal
    OMP_IGNORE_BARRIER = 6,    // internal
    OMP_FIRST_OFFLOAD_EVT = 7  // internal
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
   MetricType  type;        //!< metric type
   const char* name;        //!< name of the metric
   const char* description; //!< description of the metric
   MetricMode  metricMode;  //!< write mode of the metric (e.g. next, last, etc.)
   OTF2_Type   valueType;   //!< OTF2 value type (uint64_t, float, etc.)
   const char* unit;        //!< base unit of the metric value
   bool        isInternal;  //!< internal metrics are not written to the trace
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
   { BLAME,         "Blame",         
                    "Amount of caused waiting time", 
                    COUNTER_ABSOLUT_LAST, OTF2_TYPE_DOUBLE, "seconds", false },
   { WAITING_TIME,  "Waiting Time",
                    "Time in a wait state",
                    ATTRIBUTE, OTF2_TYPE_DOUBLE, "seconds", false },
   { CRITICAL_PATH, "Critical Path", 
                    "On the critical path boolean",
                    COUNTER_ABSOLUT_NEXT, OTF2_TYPE_UINT64, "boolean", false },
   // internal metrics
   { BLAME4IDLE,    "Blame4DeviceIdle",         
                    "Amount of caused idle time on the device", 
                    COUNTER_ABSOLUT_LAST, OTF2_TYPE_DOUBLE, "seconds", false },
   { OMPT_REGION_ID,       "OMPT Region ID",
                           "", METRIC_MODE_UNKNOWN, OTF2_TYPE_UINT64, "", true },
   { OMP_IGNORE_BARRIER,   "OpenMP Target Collapsed Barrier", 
                           "", METRIC_MODE_UNKNOWN, OTF2_TYPE_UINT8, "", true }
 };

 class AnalysisMetric
 {
    public:

      typedef std::set< MetricType > MetricIdSet;

      // Construct of class AnalysisMetric (used in GraphEngine)
      // \todo: make singleton
      AnalysisMetric() :
        maxMetricClassId( 0 ),
        maxMetricMemberId( 0 ),
        maxAttrId( 0 )
      {

      }

      virtual
      ~AnalysisMetric()
      {
      }

      const MetricEntry*
      getMetric( MetricType metricId ) const
      {
        return &(METRIC_TABLE[ metricId ]);
      }
      
      /**
       * Get name of the given metric type.
       * 
       * @param metricId
       * @return 
       */
      static const char*
      getMetricName( MetricType metricId )
      {
        return METRIC_TABLE[ metricId ].name;
      }
      
      /**
       * Get description of the given metric type.
       * 
       * @param metricId
       * @return 
       */
      static const char*
      getMetricDescription( MetricType metricId )
      {
        return METRIC_TABLE[ metricId ].description;
      }
      
      static const OTF2_Type*
      getMetricValueType( MetricType metricId )
      {
        return &( METRIC_TABLE[ metricId ].valueType );
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
      getNewMetricMemberId()
      {
        // starting with 0 (Ids in OTF2 need to start with 0)
        return ++maxMetricMemberId;
      }

      const MetricIdSet&
      getAllCounterIds() const
      {
        return ctrIDs;
      }

      const MetricIdSet&
      getAllMetricIds() const
      {
        return metricIds;
      }
      
      void
      addMetricMemberId( uint32_t memberId )
      {
        maxMetricMemberId = std::max( maxMetricMemberId, memberId );
      }
      
      /**
       * Metric classes and metric instances share the same ID space in OTF2.
       * 
       * @param ctrId
       */
      void
      addMetricClassId( uint32_t classId )
      {
        maxMetricClassId = std::max( maxMetricClassId, classId );
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
          
          otf2Ids[ metricId ] = maxAttrId;
          
          //attributeIds.insert( metricId );
          metricIds.insert( metricId );
          
          return maxAttrId;
        }
        else if( entry->metricMode != METRIC_MODE_UNKNOWN )
        {
          maxMetricClassId++;
          
          otf2Ids[ metricId ] = maxMetricClassId;
          
          ctrIDs.insert( metricId );
          metricIds.insert( metricId );
          
          return maxMetricClassId;
        }
        else
        {
          return std::numeric_limits< uint32_t >::max();
        }
      }
      /*
      void
      addStringRef( MetricType metricId, uint32_t stringRef )
      {
        metric2StringRefMap[metricId] = stringRef;
      }
      
      uint32_t
      getStringRef( MetricType metricId )
      {
        if( metric2StringRefMap.count( metricId ) > 0 )
        {
          return metric2StringRefMap[metricId];
        }
        else
        {
          return std::numeric_limits< uint32_t >::max();
        }
      }
      */
   private:
     //! < key: metric type, value: OTF2 attribute or metric ID/ref
     typedef std::map< MetricType, uint32_t > MetricTypeIdMap;
      
     uint32_t        maxMetricClassId;
     uint32_t        maxMetricMemberId;
     uint32_t        maxAttrId;
     MetricTypeIdMap otf2Ids;
     //MetricTypeIdMap metric2StringRefMap;
     MetricIdSet     ctrIDs;
     MetricIdSet     metricIds;
 };
}
