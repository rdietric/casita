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

#include <map>
#include <set>
#include <stack>
#include <mpi.h>

#include "GraphEngine.hpp"
#include "AbstractRule.hpp"
#include "MPIAnalysis.hpp"
#include "graph/EventNode.hpp"
#include "otf/ITraceReader.hpp"
#include "otf/IParallelTraceWriter.hpp"

/* shared with VampirTrace */
#define VT_CUPTI_CUDA_STREAMREF_KEY "CUDA_STREAM_REF_KEY"
#define VT_CUPTI_CUDA_EVENTREF_KEY "CUDA_EVENT_REF_KEY"
#define VT_CUPTI_CUDA_CURESULT_KEY "CUDA_DRV_API_RESULT_KEY"

/* shared with Score-P */
#define SCOREP_CUPTI_CUDA_STREAMREF_KEY "CUDA_STREAM_REF"
#define SCOREP_CUPTI_CUDA_EVENTREF_KEY "CUDA_EVENT_REF"
#define SCOREP_CUPTI_CUDA_CURESULT_KEY "CUDA_DRV_API_RESULT"

#define SCOREP_POMP_TARGET_LOCATIONREF_KEY "OMP_TARGET_LOCATION_REF"
#define SCOREP_POMP_TARGET_REGION_ID_KEY "OMP_TARGET_REGION_ID"
#define SCOREP_POMP_TARGET_PARENT_REGION_ID_KEY "OMP_TARGET_PARENT_REGION_ID"

namespace casita
{

 class AnalysisEngine :
   public GraphEngine
 {
   public:

     typedef struct
     {
       EventNode* node;
       std::set< uint32_t > tags;
     } StreamWaitTagged;

     typedef std::map< uint64_t, uint64_t > IdIdMap;
     typedef std::map< uint64_t, EventNode* > IdEventNodeMap;
     typedef std::map< uint64_t, EventNode::EventNodeList > IdEventsListMap;
     typedef std::list< StreamWaitTagged* > NullStreamWaitList;
     typedef std::map< uint64_t, GraphNode::GraphNodeList > IdNodeListMap;
     typedef std::stack< GraphNode* > OmpNodeStack;
     typedef std::map< uint64_t, OmpNodeStack > pendingOMPKernelStackMap;
     typedef std::map< uint64_t, GraphNode* > OmpEventMap;
     typedef std::map< uint64_t,
                       std::vector< std::pair< uint64_t,
                                               GraphNode* > > >
     OmpStreamRegionsMap;

     AnalysisEngine( uint32_t mpiRank, uint32_t mpiSize );

     virtual
     ~AnalysisEngine( );

     static bool
     getFunctionType( uint32_t id,
                      const char* name,
                      EventStream* stream,
                      FunctionDescriptor* descr );

     MPIAnalysis&
     getMPIAnalysis( );

     uint32_t
     getMPIRank( ) const;

     void
     addFunction( uint32_t funcId, const char* name );

     uint32_t
     getNewFunctionId( );

     void
     setWaitStateFunctionId( uint32_t id );

     const char*
     getFunctionName( uint32_t id );

     GraphNode*
     newGraphNode( uint64_t time, uint64_t streamId,
                   const std::string name, Paradigm paradigm,
                   NodeRecordType recordType, int nodeType );

     GraphNode*
     addNewGraphNode( uint64_t time,
                      EventStream* stream,
                      const char* name,
                      Paradigm paradigm,
                      NodeRecordType recordType,
                      int nodeType );

     void
     addRule( AbstractRule* rule );

     void
     removeRules( );

     bool
     applyRules( GraphNode* node, bool verbose );

     EventStream*
     getNullStream( ) const;

     void
     setLastEventLaunch( EventNode* eventLaunchLeave );

     EventNode*
     consumeLastEventLaunchLeave( uint32_t eventId );

     EventNode*
     getLastEventLaunchLeave( uint32_t eventId ) const;

     void
     setEventProcessId( uint32_t eventId, uint64_t streamId );

     uint64_t
     getEventProcessId( uint32_t eventId ) const;

     void
     addPendingKernelLaunch( GraphNode* launch );

     GraphNode*
     consumePendingKernelLaunch( uint64_t kernelStreamId );

     void
     addStreamWaitEvent( uint64_t deviceProcId, EventNode* streamWaitLeave );

     EventNode*
     getFirstStreamWaitEvent( uint64_t deviceStreamId );

     EventNode*
     consumeFirstStreamWaitEvent( uint64_t deviceStreamId );

     void
     linkEventQuery( EventNode* eventQueryLeave );

     void
     removeEventQuery( uint32_t eventId );

     GraphNode*
     getLastLaunchLeave( uint64_t timestamp, uint64_t deviceStreamcId ) const;

     GraphNode*
     getLastLeave( uint64_t timestamp, uint64_t streamId ) const;

     void
     reset( );

     void
     saveParallelEventGroupToFile( std::string filename,
                                   std::string origFilename,
                                   bool enableWaitStates,
                                   bool verbose );

     io::IParallelTraceWriter::ActivityGroupMap& getActivityGroupMap()
     {
         return writer->getActivityGroupMap();
     }
     
     double
     getRealTime( uint64_t t );

     GraphNode*
     getPendingParallelRegion( );

     void
     setPendingParallelRegion( GraphNode* node );

     GraphNode*
     getOmpCompute( uint64_t streamId );

     void
     setOmpCompute( GraphNode* node, uint64_t streamId );
    
     const GraphNode::GraphNodeList&
     getBarrierEventList( bool device, int matchingId = 0 );

     void
     addBarrierEventToList( GraphNode* node, bool device, int matchingId = 0 );

     void
     clearBarrierEventList( bool device, int matchingId = 0 );

     void
     setOmpTargetBegin( GraphNode* node );

     GraphNode*
     consumeOmpTargetBegin( uint64_t streamId );

     void
     setOmpTargetFirstEvent( GraphNode* node );

     GraphNode*
     consumeOmpTargetFirstEvent( uint64_t streamId );

     void
     setOmpTargetLastEvent( GraphNode* node );

     GraphNode*
     consumeOmpTargetLastEvent( uint64_t streamId );

     void
     pushOmpTargetRegion( GraphNode* node, uint64_t regionId );

     void
     popOmpTargetRegion( GraphNode* node );

     void
     findOmpTargetParentRegion( GraphNode* node, uint64_t parentRegionId );

   private:

    io::IParallelTraceWriter* writer;
       
     MPIAnalysis mpiAnalysis;

     std::vector< AbstractRule* > rules;
     /* maps event ID to (cuEventRecord) leave node */
     IdEventNodeMap eventLaunchMap;

     /* maps event ID to (cuEventQuery) leave node */
     IdEventNodeMap eventQueryMap;

     /* maps (device) stream ID to list of (cuStreamWaitEvent)
      * leave nodes */
     IdEventsListMap streamWaitMap;

     /* maps event ID to (device) stream ID */
     IdIdMap eventProcessMap;
     NullStreamWaitList nullStreamWaits;
     IdNodeListMap pendingKernelLaunchMap;

     /* log the OMP enter events, needed to resolve
      * nested function calls */
     pendingOMPKernelStackMap ompBackTraceStackMap;

     /* remember last omp event per stream -> needed to resolve
      * nested function calls */
     OmpEventMap lastOmpEventMap;

     /* remember opened parallel region TODO: implement
      * that as stack for nested parallelism */
     GraphNode* pendingParallelRegion;

     /* keep track of omp kernels between parallel regions */
     OmpEventMap ompComputeTrackMap;

     /* collect barriers from different streams */
     GraphNode::GraphNodeList ompBarrierListHost;
     IdNodeListMap ompBarrierListDevice;

     /* keep track of last OMP Target Begin on each event stream */
     OmpEventMap ompTargetRegionBeginMap;

     /* keep track of the first event on each device stream after an
      *OMP target begin */
     OmpEventMap ompTargetDeviceFirstEventMap;

     /* keep track of the last event on each device stream before an
      *OMP target end */
     OmpEventMap ompTargetDeviceLastEventMap;

     OmpStreamRegionsMap ompTargetStreamRegionsMap;

     std::map< uint32_t, std::string > functionMap;
     uint32_t maxFunctionId;
     uint32_t waitStateFuncId;

     static bool
     rulePriorityCompare( AbstractRule* r1, AbstractRule* r2 );

     size_t
     getNumAllDeviceStreams( );

 };

}
