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

#include <mpi.h>
#include <cmath>
#include <cuda.h>

#include "otf/OTF1ParallelTraceWriter.hpp"
#include "graph/EventNode.hpp"
#include "CounterTable.hpp"
#include "common.hpp"
#include "FunctionTable.hpp"
#include "otf/ITraceReader.hpp"
#include "EventStream.hpp"

using namespace casita;
using namespace casita::io;

#define OTF_CHECK( cmd ) \
  { \
   int _status = cmd; \
   if ( !_status ) { \
     throw RTException( "OTF command '%s' returned error %d", #cmd, _status );} \
  }

#define MPI_CHECK( cmd ) \
  { \
    int mpi_result = cmd; \
    if ( mpi_result != MPI_SUCCESS ) { \
      throw RTException( "MPI error %d in call %s", mpi_result, #cmd );} \
  }

OTF1ParallelTraceWriter::OTF1ParallelTraceWriter( const char* streamRefKeyName,
                                                  const char* eventRefKeyName,
                                                  const char* funcResultKeyName,
                                                  uint32_t mpiRank,
                                                  uint32_t mpiSize,
                                                  const char* originalFilename )
  :
    IParallelTraceWriter( streamRefKeyName, eventRefKeyName, funcResultKeyName,
                          mpiRank, mpiSize ),
    totalNumStreams( 0 ),
    fileMgr( NULL ),
    kvList( NULL ),
    globalWriter( NULL ),
    streamRefKey( 1 ),
    eventRefKey( 2 ),
    funcResultKey( 3 ),
    processNodes( NULL ),
    enableWaitStates( false ),
    iter( NULL ),
    lastGraphNode( NULL ),
    cTable( NULL ),
    graph( NULL ),
    verbose( false )
{
  mpiNumProcesses = new int[mpiSize];
  outputFilename.assign( "" );
  this->originalFilename.assign( originalFilename );
  this->originalFilename.erase( this->originalFilename.size( ) - 4, 4 );
}

OTF1ParallelTraceWriter::~OTF1ParallelTraceWriter( )
{
}

void
OTF1ParallelTraceWriter::open( const std::string otfFilename, uint32_t maxFiles,
                               uint32_t numStreams )
{
  fileMgr = OTF_FileManager_open( maxFiles );

  std::string baseFilename;
  baseFilename.assign( "" );
  if ( strstr( originalFilename.c_str( ), ".otf" ) != NULL )
  {
    baseFilename.append( originalFilename.c_str( ),
                         originalFilename.length( ) - 4 );
  }
  else
  {
    baseFilename.append( originalFilename.c_str( ), originalFilename.length( ) );
  }

  reader = OTF_Reader_open( baseFilename.c_str( ), fileMgr );

  if ( !reader )
  {
    throw RTException( "Failed to open OTF1 trace file %s", baseFilename.c_str( ) );
  }

  OTF_Reader_setRecordLimit( reader, OTF_READ_MAXRECORDS );

  kvList = OTF_KeyValueList_new( );

  MPI_CHECK( MPI_Allgather( &numStreams, 1, MPI_UNSIGNED,
                            mpiNumProcesses, 1, MPI_INT, MPI_COMM_WORLD ) );
  for ( uint32_t i = 0; i < mpiSize; ++i )
  {
    totalNumStreams += mpiNumProcesses[i];
  }

  outputFilename.append( otfFilename.c_str( ), otfFilename.length( ) - 4 );

  copyGlobalDefinitions( );

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

}

void
OTF1ParallelTraceWriter::close( )
{
  /* close global writer */
  if ( mpiRank == 0 )
  {
    OTF_CHECK( OTF_Writer_close( globalWriter ) );
    copyMasterControl( );
  }

  /* close all local writers */
  for ( std::map< uint32_t, OTF_WStream_ptr >::const_iterator iter =
          processWStreamMap.begin( );
        iter != processWStreamMap.end( ); ++iter )
  {
    OTF_CHECK( OTF_WStream_close( iter->second ) );
  }

  OTF_KeyValueList_close( kvList );
  OTF_FileManager_close( fileMgr );
}

/*
 * Copy definitions from original trace to new one
 */
void
OTF1ParallelTraceWriter::copyGlobalDefinitions(  )
{
  
  OTF_Reader* reader = OTF_Reader_open( originalFilename.c_str( ), fileMgr );

  OTF_HandlerArray* handlers = OTF_HandlerArray_open( );
  
  if(mpiRank == 0)
  {
    globalWriter = OTF_Writer_open(
        outputFilename.c_str( ), totalNumStreams, fileMgr );

    OTF_HandlerArray_setHandler(
      handlers,
      (OTF_FunctionPointer*)
      otf1HandleDefTimerResolution,
      OTF_DEFTIMERRESOLUTION_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                                         this,
                                         OTF_DEFTIMERRESOLUTION_RECORD );

    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefProcess,
                                 OTF_DEFPROCESS_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFPROCESS_RECORD );

    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefProcessGroup,
                                 OTF_DEFPROCESSGROUP_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                                         this,
                                         OTF_DEFPROCESSGROUP_RECORD );

    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefFunction,
                                 OTF_DEFFUNCTION_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFFUNCTION_RECORD );

    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefFunctionGroup,
                                 OTF_DEFFUNCTIONGROUP_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                                         this,
                                         OTF_DEFFUNCTIONGROUP_RECORD );

    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefAttributeList,
                                 OTF_DEFATTRLIST_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFATTRLIST_RECORD );
    OTF_HandlerArray_setHandler( handlers,
                                 (OTF_FunctionPointer*)otf1HandleDefKeyValue,
                                 OTF_DEFKEYVALUE_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFKEYVALUE_RECORD );
    OTF_HandlerArray_setHandler(
      handlers,
      (OTF_FunctionPointer*)
      otf1HandleDefProcessOrGroupAttributes,
      OTF_DEFPROCESSORGROUPATTR_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                                         this,
                                         OTF_DEFPROCESSORGROUPATTR_RECORD );
  } else
  { 
    OTF_HandlerArray_setHandler(handlers,
                        (OTF_FunctionPointer*)
                        otf1HandleDefTimerResolution,
                        OTF_DEFTIMERRESOLUTION_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                        this,
                        OTF_DEFTIMERRESOLUTION_RECORD );
    
    OTF_HandlerArray_setHandler( handlers,
                        (OTF_FunctionPointer*)otf1HandleDefProcessGroup,
                        OTF_DEFPROCESSGROUP_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                        this,
                        OTF_DEFPROCESSGROUP_RECORD );
    
    OTF_HandlerArray_setHandler( handlers,
                        (OTF_FunctionPointer*)otf1HandleDefFunction,
                        OTF_DEFFUNCTION_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFFUNCTION_RECORD );
    
    OTF_HandlerArray_setHandler( handlers,
                        (OTF_FunctionPointer*)otf1HandleDefAttributeList,
                        OTF_DEFATTRLIST_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_DEFATTRLIST_RECORD );
    
    OTF_HandlerArray_setHandler(
                        handlers,
                        (OTF_FunctionPointer*)
                        otf1HandleDefProcessOrGroupAttributes,
                        OTF_DEFPROCESSORGROUPATTR_RECORD );
    OTF_HandlerArray_setFirstHandlerArg( handlers,
                                         this,
                                         OTF_DEFPROCESSORGROUPATTR_RECORD );
  }

  if ( OTF_Reader_readDefinitions( reader, handlers ) == OTF_READ_ERROR )
  {
    throw RTException( "Failed to read OTF definitions part" );
  }
  OTF_HandlerArray_close( handlers );

  OTF_CHECK( OTF_Reader_close( reader ) );

  if(mpiRank == 0)
  {
    OTF_CHECK( OTF_Writer_writeDefKeyValue( globalWriter, 0, streamRefKey, OTF_UINT32,
                                            streamRefKeyName,
                                            "Referenced CUDA stream" ) );
    OTF_CHECK( OTF_Writer_writeDefKeyValue( globalWriter, 0, eventRefKey, OTF_UINT32,
                                            eventRefKeyName,
                                            "Referenced CUDA event" ) );
    OTF_CHECK( OTF_Writer_writeDefKeyValue( globalWriter, 0, funcResultKey, OTF_UINT32,
                                            funcResultKeyName,
                                            "CUDA API function result" ) );
  }
}

void
OTF1ParallelTraceWriter::copyMasterControl( )
{
  OTF_MasterControl* mc = OTF_MasterControl_new( fileMgr );

  OTF_CHECK( OTF_MasterControl_read( mc, originalFilename.c_str( ) ) );
  OTF_CHECK( OTF_MasterControl_write( mc, outputFilename.c_str( ) ) );

  OTF_MasterControl_close( mc );
}

/*
 * OTF2: just create event writer for this process
 * OTF1: create event stream writer and write "begin process" event
 */
void
OTF1ParallelTraceWriter::writeDefProcess( uint64_t id, uint64_t parentId,
                                          const char* name, ProcessGroup pg )
{

  uint32_t otf1_id = (uint32_t)id;

  /* create local writer for process id */
  OTF_WStream_ptr wstream = OTF_WStream_open(
    outputFilename.c_str( ), otf1_id, fileMgr );
  processWStreamMap[otf1_id] = wstream;

  
  OTF_WStream_writeBeginProcess( wstream, 0, otf1_id );
}

/*
 * Write self-defined metrics/counter to new trace file
 */
void
OTF1ParallelTraceWriter::writeDefCounter( uint32_t id,
                                          const char* name,
                                          int properties )
{
  if ( mpiRank == 0 )
  {
    OTF_CHECK( OTF_Writer_writeDefCounter( globalWriter, 0, id, name,
                                           properties, 0, 0 ) );
  }

}

/*
 * Read all events from original trace for this process and combine them with the
 * events and counter values from analysis
 */
void
OTF1ParallelTraceWriter::writeProcess( uint64_t processId,
                                       EventStream::SortedGraphNodeList* nodes,
                                       bool enableWaitStates,
                                       GraphNode* pLastGraphNode,
                                       bool verbose,
                                       CounterTable* ctrTable,
                                       Graph* graph )
{
    cpuNodes = 0;
    currentStackLevel = 0;
    currentlyRunningCPUFunctions.clear();
    lastTimeOnCriticalPath[processId] = 0;
  lastProcessedNodePerProcess[processId] = new GraphNode( 0,
                                processId,
                                "START",
                                PARADIGM_ALL,
                                RECORD_ATOMIC,
                                MISC_PROCESS );
    
  OTF1Event event;
  event.location = processId;
  event.type = OTF1_MISC;
  event.time = 0;
  lastCPUEventPerProcess[processId] = event;
  
  if ( verbose )
  {
    std::cout << "[" << mpiRank << "] Start writing for process " <<
    processId << std::endl;
  }
  
  /* create writer for this process */
  processNodes = nodes;
  this->enableWaitStates = enableWaitStates;
  iter = processNodes->begin( );
  /* skip first processNode for main processes, since it did not
   * appear in original trace */
  if ( ( *iter )->getType( ) == MISC_PROCESS )
  {
    iter++;
  }
  lastGraphNode = pLastGraphNode;
  this->verbose = verbose;
  this->graph = graph;
  cTable = ctrTable;

  uint32_t otf1_id = (uint32_t)processId;

  OTF_HandlerArray* handlers = OTF_HandlerArray_open( );

  /* Set event handlers */
  OTF_HandlerArray_setHandler( handlers,
                               (OTF_FunctionPointer*)otf1HandleRecvMsg,
                               OTF_RECEIVE_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_RECEIVE_RECORD );
  OTF_HandlerArray_setHandler( handlers,
                               (OTF_FunctionPointer*)otf1HandleSendMsg,
                               OTF_SEND_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_SEND_RECORD );
  OTF_HandlerArray_setHandler(
    handlers,
    (OTF_FunctionPointer*)
    otf1HandleBeginCollectiveOperation,
    OTF_BEGINCOLLOP_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_BEGINCOLLOP_RECORD );
  OTF_HandlerArray_setHandler(
    handlers,
    (OTF_FunctionPointer*)
    otf1HandleEndCollectiveOperation,
    OTF_ENDCOLLOP_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_ENDCOLLOP_RECORD );
  OTF_HandlerArray_setHandler( handlers, (OTF_FunctionPointer*)otf1HandleRMAEnd,
                               OTF_RMAEND_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_RMAEND_RECORD );
  OTF_HandlerArray_setHandler( handlers, (OTF_FunctionPointer*)otf1HandleRMAGet,
                               OTF_RMAGET_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_RMAGET_RECORD );
  OTF_HandlerArray_setHandler( handlers, (OTF_FunctionPointer*)otf1HandleRMAPut,
                               OTF_RMAPUT_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_RMAPUT_RECORD );
  OTF_HandlerArray_setHandler( handlers, (OTF_FunctionPointer*)otf1HandleEnter,
                               OTF_ENTER_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_ENTER_RECORD );
  OTF_HandlerArray_setHandler( handlers, (OTF_FunctionPointer*)otf1HandleLeave,
                               OTF_LEAVE_RECORD );
  OTF_HandlerArray_setFirstHandlerArg( handlers, this, OTF_LEAVE_RECORD );

  OTF_RStream* rStream = OTF_Reader_getStream( reader, otf1_id );
  OTF_RStream_setRecordLimit( rStream, OTF_READ_MAXRECORDS );
  OTF_RStream_readEvents( rStream, handlers );

  OTF_HandlerArray_close( handlers );
}

/* processing the next CPU event, calculating blame 
 * 
 */
bool OTF1ParallelTraceWriter::processCPUEvent(OTF1Event event)
{
    OTF_WStream_ptr wstream = processWStreamMap[event.location];
    GraphNode* node;
    
    node = *iter;
    
    OTF1Event bufferedCPUEvent = lastCPUEventPerProcess[event.location];
    GraphNode * lastProcessedNode  = lastProcessedNodePerProcess[node->getStreamId()];
    // Any buffered CPU-Events?
    if( lastProcessedNode &&
            bufferedCPUEvent.time > 
            lastProcessedNode->getTime())
    {
        
        if(bufferedCPUEvent.type != OTF1_ENTER 
                && bufferedCPUEvent.type != OTF1_LEAVE)
            return true;
        // write counter for critical path for all pending cpu events

        uint64_t ctrVal =0;
        bool valid;
        if(lastProcessedNode && 
                (lastProcessedNode->getCounter(cTable->getCtrId( CTR_CRITICALPATH ), &valid) == 1) &&
                (node->getCounter(cTable->getCtrId( CTR_CRITICALPATH ), &valid) == 1)
                && (lastTimeOnCriticalPath[event.location] != 0))
        {
            ctrVal = 1;
            for(std::list<uint32_t>::iterator fIter = currentlyRunningCPUFunctions.begin();
                    fIter != currentlyRunningCPUFunctions.end(); fIter++)
            {
                activityGroupMap[*fIter].totalDurationOnCP += event.time 
                        - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[event.location]);
                //std::cout << "[" << mpiRank << "] + Time on CP " << event.time 
                //        - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[event.location]) << " to " 
                //    << regionNameList[*fIter] 
                //    << " last Enter: " << activityGroupMap[*fIter].lastEnter
                //    << " last on CP: " << lastTimeOnCriticalPath[event.location]
                //    << std::endl;
            }
            lastTimeOnCriticalPath[event.location] = event.time;
        }
        
        // Write buffered CPU-event
        if(bufferedCPUEvent.type == OTF1_ENTER)
        {
            OTF_CHECK( OTF_WStream_writeEnter( wstream, bufferedCPUEvent.time,
                                         bufferedCPUEvent.regionRef,
                                         bufferedCPUEvent.location, 0 ) );
        }
        if(bufferedCPUEvent.type == OTF1_LEAVE)
        {
            OTF_CHECK( OTF_WStream_writeLeave( wstream, bufferedCPUEvent.time, 
                                        bufferedCPUEvent.regionRef,
                                        bufferedCPUEvent.location, 0 ) );
        }
        
        OTF_CHECK( OTF_WStream_writeCounter( wstream, bufferedCPUEvent.time,
                                             bufferedCPUEvent.location,
                                             cTable->getCtrId( CTR_CRITICALPATH ),
                                             ctrVal ) );

        if(bufferedCPUEvent.type == OTF1_LEAVE && (bufferedCPUEvent.regionRef != event.regionRef))
        {
            if(lastTimeOnCriticalPath[event.location] != 0)
            {
                if(activityGroupMap[bufferedCPUEvent.regionRef].lastEnter > 
                        lastProcessedNode->getTime())
                {
                    //std::cout << "[" << mpiRank << "] time on CP " << 
                    //        ctrVal* (bufferedCPUEvent.time -
                    //            activityGroupMap[bufferedCPUEvent.regionRef].lastEnter) 
                    //        << "from LAST Enter"  
                    //<< " to leave " << regionNameList[bufferedCPUEvent.regionRef] << std::endl;
                    activityGroupMap[bufferedCPUEvent.regionRef].totalDurationOnCP += 
                            ctrVal* (bufferedCPUEvent.time -
                                activityGroupMap[bufferedCPUEvent.regionRef].lastEnter);
                } else
                {
                    //std::cout << "[" << mpiRank << "] time on CP " << 
                    //        ctrVal* (bufferedCPUEvent.time -
                    //            lastProcessedNode->getTime())
                    //        << " from NODE " << lastProcessedNode->getUniqueName()  
                    //<< " to leave " << regionNameList[bufferedCPUEvent.regionRef] << std::endl;
                    activityGroupMap[bufferedCPUEvent.regionRef].totalDurationOnCP += 
                            ctrVal* (bufferedCPUEvent.time -
                                lastProcessedNode->getTime());
                }
            }
        }
                    
    }
    
    if(bufferedCPUEvent.type == OTF1_ENTER )
        activityGroupMap[bufferedCPUEvent.regionRef].lastEnter = bufferedCPUEvent.time;
    else if(bufferedCPUEvent.type == OTF1_LEAVE )
    {
        activityGroupMap[bufferedCPUEvent.regionRef].totalDuration += bufferedCPUEvent.time - activityGroupMap[bufferedCPUEvent.regionRef].lastEnter;
    }
    
    return true;
}

/*
 * Process next node in list from analysis
 */
bool
OTF1ParallelTraceWriter::processNextNode( OTF1Event event )
{
    uint64_t time = event.time;
    if(event.type == OTF1_LEAVE)
    {
        event.regionRef = eventStack.top();
        eventStack.pop();
    }
    
    if(activityGroupMap.find(event.regionRef) == activityGroupMap.end())
    {
        activityGroupMap[event.regionRef].functionId = event.regionRef;
        activityGroupMap[event.regionRef].numInstances = 0;
    }
    
    if(verbose > VERBOSE_ANNOY)
        std::cout << "[" << mpiRank << "] process " << event.regionRef << " " << regionNameList[event.regionRef] << " " << event.type 
            << " at " << event.time << std::endl;
    
    if(event.type == OTF1_ENTER )
        activityGroupMap[event.regionRef].numInstances++;
    
    uint32_t funcId = event.regionRef;
    
    
    if(iter == processNodes->end())   
    {
        std::cout << "[" << mpiRank << "] reached end of processNodes at " << event.time << std::endl;
        if(event.type == OTF1_ENTER )
        {
            activityGroupMap[event.regionRef].lastEnter = event.time;
            currentlyRunningCPUFunctions.push_back(event.regionRef);
        } else if(event.type == OTF1_LEAVE )
        {
            activityGroupMap[event.regionRef].totalDuration += event.time - activityGroupMap[event.regionRef].lastEnter;
            currentlyRunningCPUFunctions.pop_back();
        }
        return false;
    }

    GraphNode* node = *iter;
  
  /* 
    * CPU events are not in nodes, hence they have to be written and countervalues have to be calculated first.
    * Since we need the time between a node and its successor we buffer one cpu event at each time. 
    */
   OTF1Event bufferedCPUEvent = lastCPUEventPerProcess[event.location];
   GraphNode * lastProcessedNode  = lastProcessedNodePerProcess[node->getStreamId()];
   
   // Any buffered CPU-Events to be processed?
   if( lastProcessedNode && (bufferedCPUEvent.time > 
       lastProcessedNode->getTime()) )
   {
        processCPUEvent(event);
   }

   // CPU_nodes are to be buffered
   FunctionDescriptor desc;
   bool isDeviceStream = deviceStreamMap[event.location];
   if(!FunctionTable::getAPIFunctionType(regionNameList[funcId].c_str() , &desc, isDeviceStream, false) 
           || desc.paradigm == PARADIGM_VT)
   {
       if(verbose)
           std::cout << "[" << mpiRank << "] Buffer: [" << event.location << "] function: " << event.regionRef << std::endl;
       
       if(event.type == OTF1_ENTER) 
           activityGroupMap[event.regionRef].lastEnter = event.time;

       // Add Blame for area since last event
        assignBlame(event.time, event.location);
        
        // Keep track of cpuFunctions
        if(event.type == OTF1_ENTER)
        {
            currentlyRunningCPUFunctions.push_back(event.regionRef);
        }
        if(event.type == OTF1_LEAVE)
        {
            currentlyRunningCPUFunctions.pop_back();
        }
       
       cpuNodes++;

       lastCPUEventPerProcess[event.location] = event;

       return true;
   }

   if(verbose)
       std::cout << "[" << mpiRank << "] processed " << cpuNodes << " cpuNodes" << std::endl;

   cpuNodes = 0;

  if ( time != node->getTime( ) )
  {
    if ( verbose )
    {
      std::cout <<
      "Node written from original trace, since there is an inconsistency: TIME: "
                << time << " - " << node->getTime( )
                << "funcId " << funcId << " - " << node->getFunctionId( ) <<
      std::endl;
    }
    if(event.type == OTF1_ENTER )
        activityGroupMap[event.regionRef].lastEnter = event.time;
    else if(event.type == OTF1_LEAVE )
    {
        activityGroupMap[event.regionRef].totalDuration += event.time - activityGroupMap[event.regionRef].lastEnter;
    }

    return false;
  }

  /* find next connected node on critical path */
  GraphNode* futureCPNode = NULL;
  Graph::EdgeList outEdges;
  if ( graph->hasOutEdges( (GraphNode*)node ) )
  {
    outEdges = graph->getOutEdges( (GraphNode*)node );
  }

  Graph::EdgeList::const_iterator edgeIter = outEdges.begin( );
  uint64_t timeNextCPNode = 0;
  uint32_t cpCtrId = cTable->getCtrId( CTR_CRITICALPATH );

  while ( edgeIter != outEdges.end( ) && !outEdges.empty( ) )
  {
    GraphNode* edgeEndNode = ( *edgeIter )->getEndNode( );

    if ( ( edgeEndNode->getCounter( cpCtrId, NULL ) == 1 ) &&
         ( ( timeNextCPNode > edgeEndNode->getTime( ) ) || timeNextCPNode == 0 ) )
    {
      futureCPNode = edgeEndNode;
      timeNextCPNode = futureCPNode->getTime( );
    }
    ++edgeIter;
  }

  if ( node->isEnter( ) || node->isLeave( ) )
  {
    if ( ( !node->isPureWaitstate( ) ) || enableWaitStates )
    {
      if ( verbose )
      {
        printf( "[%u] [%12lu:%12.8fs] %60s in %8lu (FID %lu)\n",
                mpiRank,
                node->getTime( ),
                (double)( node->getTime( ) ) / (double)timerResolution,
                node->getUniqueName( ).c_str( ),
                node->getStreamId( ),
                node->getFunctionId( ) );
      }

      writeNode( node, *cTable,
                 node == lastGraphNode, futureCPNode );
    }
  }

  lastProcessedNodePerProcess[node->getStreamId()] = node;
  /* set iterator to next node in sorted list */
  iter++;
  
  if(event.type == OTF1_ENTER )
    activityGroupMap[event.regionRef].lastEnter = event.time;
  else if(event.type == OTF1_LEAVE )
  {
    activityGroupMap[event.regionRef].totalDuration += event.time - activityGroupMap[event.regionRef].lastEnter;
  }

  if(graph->hasOutEdges(node) && (node->getId() != lastNodeCheckedForEdgesId))
  {
      const Graph::EdgeList & edges = graph->getOutEdges(node);
      for(Graph::EdgeList::const_iterator edgeIter = edges.begin(); edgeIter != edges.end(); edgeIter++)
      {
          if(((*edgeIter)->getCPUBlame() > 0) )
          {
            openEdges.push_back(*edgeIter);
          }
      }
      lastNodeCheckedForEdgesId = node->getId();
  }
  
  return true;

}

/*
 * Write the nodes that were read at program start, and processed during analysis
 * write corresponding counter values of computed metrics for each node
 */
void
OTF1ParallelTraceWriter::writeNode( GraphNode* node,
                                    CounterTable& ctrTable,
                                    bool lastProcessNode,
                                    const GraphNode* futureNode )
{ 
  uint32_t processId = (uint32_t)node->getStreamId( );
  OTF_WStream_ptr wstream = processWStreamMap[processId];
  uint64_t nodeTime = node->getTime( );

  // Add Blame for area since last event
  OTF1Event bufferedCPUEvent = lastCPUEventPerProcess[node->getStreamId()];
  GraphNode * lastProcessedNode  = lastProcessedNodePerProcess[node->getStreamId()];
  
  // Add Blame for area since last event
  if(!(currentStackLevel > currentlyRunningCPUFunctions.size()) && node->isEnter())
     assignBlame(node->getTime(), node->getStreamId());
  
  if ( node->isEnter( ) || node->isLeave( ) )
  {

    if ( (uint32_t)node->getReferencedStreamId( ) != 0 )
    {
      OTF_KeyValueList_appendUint32( kvList, streamRefKey,
                                     (uint32_t)node->getReferencedStreamId( ) );
    }

    if ( node->isEventNode( ) )
    {
      EventNode* eNode = (EventNode*)node;
      OTF_KeyValueList_appendUint32( kvList, eventRefKey, eNode->getEventId( ) );
      CUresult cuResult = CUDA_ERROR_NOT_READY;
      if ( eNode->getFunctionResult( ) == EventNode::FR_SUCCESS )
      {
        cuResult = CUDA_SUCCESS;
      }
      OTF_KeyValueList_appendUint32( kvList, funcResultKey, cuResult );
    }

    if ( node->isEnter( ) )
    {
      OTF_CHECK( OTF_WStream_writeEnter( wstream, nodeTime,
                                         (uint32_t)node->getFunctionId( ),
                                         processId, 0 ) );
    }
    else
    {
      OTF_CHECK( OTF_WStream_writeLeave( wstream, nodeTime, 0,
                                         processId, 0 ) );
    }

  }

  CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs( );
  for ( CounterTable::CtrIdSet::const_iterator cIter = ctrIdSet.begin( );
        cIter != ctrIdSet.end( ); ++cIter )
  {
    bool valid = false;
    uint32_t ctrId = *cIter;
    CtrTableEntry* counter = ctrTable.getCounter( ctrId );
    if ( counter->isInternal )
    {
      continue;
    }

    CounterType ctrType = counter->type;
    if ( ctrType == CTR_WAITSTATE_LOG10 || ctrType == CTR_BLAME_LOG10 )
    {
      continue;
    }

    uint64_t ctrVal = node->getCounter( ctrId, &valid );

    if ( valid || counter->hasDefault )
    {
      if ( !valid )
      {
        ctrVal = counter->defaultValue;
      }

      if ( ctrType == CTR_WAITSTATE )
      {
        uint64_t ctrValLog10 = 0;
        if ( ctrVal > 0 )
        {
          ctrValLog10 = std::log10( (double)ctrVal );
        }

        OTF_CHECK( OTF_WStream_writeCounter( wstream, node->getTime( ),
                                             processId,
                                             ctrTable.getCtrId(
                                               CTR_WAITSTATE_LOG10 ),
                                             ctrValLog10 ) );
      }

      if ( ctrType == CTR_BLAME )
      {
          if(node->isLeave())
          {
              if(graph->hasOutEdges(node))
              {
                  
                  const Graph::EdgeList& edges = graph->getOutEdges(node);
                  GraphNode * closestNode = edges.front()->getEndNode();
                  // get closest Node
                  for(Graph::EdgeList::const_iterator edgeIter = edges.begin();
                          edgeIter != edges.end(); edgeIter++)
                  {
                      if(closestNode->getTime() > (*edgeIter)->getEndNode()->getTime())
                          closestNode = (*edgeIter)->getEndNode();
                  }
                  //Add blame to edge with closes Node
                  for(Graph::EdgeList::const_iterator edgeIter = edges.begin();
                          edgeIter != edges.end(); edgeIter++)
                  {
                      if(closestNode == (*edgeIter)->getEndNode())
                      {
                          (*edgeIter)->addCPUBlame(ctrVal);
                      }
                  }
              }
              continue;
          }
          
          activityGroupMap[node->getFunctionId()].totalBlame += ctrVal;
        uint64_t ctrValLog10 = 0;
        if ( ctrVal > 0 )
        {
          ctrValLog10 = std::log10( (double)ctrVal );
        }

        OTF_CHECK( OTF_WStream_writeCounter( wstream, node->getTime( ),
                                             processId,
                                             ctrTable.getCtrId( CTR_BLAME_LOG10 ),
                                             ctrValLog10 ) );
      }

      if ( ctrType == CTR_CRITICALPATH_TIME )
      {
        activityGroupMap[node->getFunctionId()].totalDurationOnCP += ctrVal;
        /* We used to write the counter for time on critical path. We don't do this anymore.*/
          continue;
      }

      OTF_CHECK( OTF_WStream_writeCounter( wstream, node->getTime( ),
                                           processId, ctrId, ctrVal ) );

      if ( ( ctrType == CTR_CRITICALPATH ) && ( ctrVal == 1 ) )
      {
        if(lastTimeOnCriticalPath[node->getStreamId()] != 0 )
        {
          for(std::list<uint32_t>::iterator fIter = currentlyRunningCPUFunctions.begin();
                  fIter != currentlyRunningCPUFunctions.end(); fIter++)
          {
              activityGroupMap[*fIter].totalDurationOnCP += node->getTime() 
                      - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[node->getStreamId()]);
              //std::cout << "[" << mpiRank << "] + Time on CP " << node->getTime() 
              //        - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[node->getStreamId()]) << " to " 
              //    << regionNameList[*fIter] << std::endl;
          }
        }
        
        lastTimeOnCriticalPath[node->getStreamId()] = node->getTime();
          
        if ( lastProcessNode )
        {
          OTF_CHECK( OTF_WStream_writeCounter( wstream, node->getTime( ),
                                               processId, ctrId, 0 ) );
          lastTimeOnCriticalPath[node->getStreamId()] = 0;
        }

        /* make critical path stop in current process if next cp node
         * in different process */
        if ( ( node->isLeave( ) ) && (( futureNode == NULL ) ||
             ( futureNode->getStreamId( ) != processId )) )
        {
          OTF_CHECK( OTF_WStream_writeCounter( wstream, node->getTime( ),
                                               processId, ctrId, 0 ) );
          
          lastTimeOnCriticalPath[node->getStreamId()] = 0;
        }

      }
      
      if ( ( ctrType == CTR_CRITICALPATH ) && ( ctrVal == 0 ) )
      {
        if(lastTimeOnCriticalPath[node->getStreamId()] != 0 )
        {
          for(std::list<uint32_t>::iterator fIter = currentlyRunningCPUFunctions.begin();
                  fIter != currentlyRunningCPUFunctions.end(); fIter++)
          {
              activityGroupMap[*fIter].totalDurationOnCP += node->getTime() 
                      - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[node->getStreamId()]);
              //std::cout << "[" << mpiRank << "] + Time on CP " << node->getTime() 
              //        - std::max(activityGroupMap[*fIter].lastEnter, lastTimeOnCriticalPath[node->getStreamId()]) << " to " 
              //    << regionNameList[*fIter] << std::endl;
          }
        }
        
        lastTimeOnCriticalPath[node->getStreamId()] = 0;
      }
    }
  }
}

/*
 * Method handling blame distribution to events.
 */
void 
OTF1ParallelTraceWriter::assignBlame(uint64_t currentTime, uint64_t currentStream)
{
  // Add Blame for area since last event
  OTF1Event bufferedCPUEvent = lastCPUEventPerProcess[currentStream];
  GraphNode * lastProcessedNode  = lastProcessedNodePerProcess[currentStream];
  OTF_WStream_ptr wstream = processWStreamMap[currentStream];
  
  if(!(bufferedCPUEvent.type == OTF1_ENTER) && !(bufferedCPUEvent.type == OTF1_LEAVE))
      return;

    bool valid;
    uint32_t fId = currentlyRunningCPUFunctions.back();
     uint64_t totalBlame = 0;
     uint64_t edgeBlame = 0;
     uint64_t blameAreaStart = std::max(bufferedCPUEvent.time, lastProcessedNode->getTime());
     uint64_t timeDiff = currentTime - blameAreaStart;
     for(Graph::EdgeList::iterator edgeIter = openEdges.begin();
             edgeIter != openEdges.end(); )
     {

         Edge * edge = *edgeIter;
         if(edge->getEndNode()->getTime() > lastProcessedNode->getTime())
         {
            edgeBlame += edge->getCPUBlame();
            //std::cout << "[" << mpiRank << "] Blame " << edge->getCPUBlame() << " from " << edge->getStartNode()->getUniqueName()
            //        << " -> " << edge->getEndNode()->getUniqueName() << std::endl;
            if((edge->getDuration() > 0) && (edge->getStartNode()->getTime() < currentTime ))
                totalBlame += (double)(edge->getCPUBlame()) *
                    (double)timeDiff/
                    (double)(edge->getDuration());
            edgeIter++;
         } else
         {
             openEdges.erase(edgeIter);
         }
    }

    activityGroupMap[fId].totalBlame += totalBlame;

    //std::cout << "[" << mpiRank << "] write blame " << totalBlame << " to " << blameAreaStart << " fid " << fId << " "
    //        << idStringMap[regionNameIdList[fId]] << " on " << bufferedCPUEvent.location << " timeDiff " << timeDiff << std::endl;

    OTF_CHECK( OTF_WStream_writeCounter( wstream, blameAreaStart,
                                               bufferedCPUEvent.location, cTable->getCtrId(
                                                        CTR_BLAME ), totalBlame ) );
    if(totalBlame > 0)
        totalBlame = std::log10((double) totalBlame);

    OTF_CHECK( OTF_WStream_writeCounter( wstream, blameAreaStart,
                                           bufferedCPUEvent.location, cTable->getCtrId(
                                                    CTR_BLAME_LOG10 ), totalBlame ) );

}

/*
 * /////////////////////// Callbacks to re-write definition records of original trace file ///////////////////
 * Every callbacks has the writer object within @var{userData} and writes record immediately after reading
 */
int
OTF1ParallelTraceWriter::otf1HandleDefProcess( void* userData,
                                               uint32_t stream,
                                               uint32_t processId,
                                               const char* name,
                                               uint32_t parent,
                                               OTF_KeyValueList_struct* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_Writer_writeDefProcess( writer->globalWriter, stream,
                                         processId, name, parent ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefProcessGroup( void* userData,
                                                    uint32_t stream,
                                                    uint32_t procGroup,
                                                    const char* name,
                                                    uint32_t numberOfProcs,
                                                    const uint32_t* procs,
                                                    OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  for(uint32_t i=0; i< numberOfProcs; i++)
  {
      writer->processGroupProcessListMap[procGroup].push_back(procs[i]);
  }
  
  if(writer->mpiRank == 0)
        OTF_CHECK( OTF_Writer_writeDefProcessGroupKV( writer->globalWriter,
                                                stream, procGroup, name,
                                                numberOfProcs, procs, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefProcessOrGroupAttributes(
  void* userData,
  uint32_t stream,
  uint32_t
  proc_token,
  uint32_t
  attr_token,
  OTF_KeyValueList
  * list )
{

  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  AttrListMap::iterator attrIter = writer->attrListMap.find( attr_token );
  if ( attrIter == writer->attrListMap.end( ) )
  {
    throw std::runtime_error( "Attribute list not found" );
  }

  std::vector< OTF_ATTR_TYPE >& attrList = attrIter->second;
  
  bool isCUDA = false;
  for ( std::vector< OTF_ATTR_TYPE >::iterator iter = attrList.begin( );
        iter != attrList.end( ); ++iter )
  {
    if ( *iter == OTF_ATTR_IsCUDAThread )
    {
      isCUDA = true;
    }
  }

  if(isCUDA)
  {
    for(std::list<uint32_t>::iterator iter = writer->processGroupProcessListMap[proc_token].begin();
            iter != writer->processGroupProcessListMap[proc_token].end(); iter++ )
    {
        writer->deviceStreamMap[*iter] = true;
    }
  }
  
  if(writer->mpiRank == 0)
        OTF_CHECK( OTF_Writer_writeDefProcessOrGroupAttributesKV( writer->
                                                            globalWriter,
                                                            stream, proc_token,
                                                            attr_token, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefAttributeList( void* userData,
                                                     uint32_t stream,
                                                     uint32_t attr_token,
                                                     uint32_t num,
                                                     OTF_ATTR_TYPE* array,
                                                     OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  for ( uint32_t i = 0; i < num; ++i )
  {
    writer->attrListMap[attr_token].push_back( array[i] );
  }
  
  if(writer->mpiRank == 0)
        OTF_CHECK( OTF_Writer_writeDefAttributeListKV( writer->globalWriter,
                                                 stream, attr_token, num, array,
                                                 list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefFunction( void* userData,
                                                uint32_t stream,
                                                uint32_t func,
                                                const char* name,
                                                uint32_t funcGroup,
                                                uint32_t source,
                                                OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  writer->regionNameList[func] = name;
  
  if(writer->mpiRank == 0)
    OTF_CHECK( OTF_Writer_writeDefFunctionKV( writer->globalWriter, stream,
                                            func, name, funcGroup, source, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefFunctionGroup( void* userData,
                                                     uint32_t stream,
                                                     uint32_t funcGroup,
                                                     const char* name,
                                                     OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_Writer_writeDefFunctionGroupKV( writer->globalWriter,
                                                 stream, funcGroup, name, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefKeyValue( void* userData,
                                                uint32_t stream,
                                                uint32_t key,
                                                OTF_Type type,
                                                const char* name,
                                                const char* description,
                                                OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_Writer_writeDefKeyValueKV( writer->globalWriter,
                                            stream, key, type, name,
                                            description, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleDefTimerResolution( void* userData,
                                                       uint32_t stream,
                                                       uint64_t ticksPerSecond,
                                                       OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  writer->timerResolution = ticksPerSecond;

  if(writer->mpiRank == 0)
        OTF_CHECK( OTF_Writer_writeDefTimerResolution( writer->globalWriter,
                                                 stream, ticksPerSecond ) );

  return OTF_RETURN_OK;
}

/*
 * /////////////////////// Callbacks to re-write enter/leave and communication records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 * Enter and leave callbacks call "processNextNode()" to write node with metrics
 */
int
OTF1ParallelTraceWriter::otf1HandleEnter( void* userData,
                                          uint64_t time,
                                          uint32_t functionId,
                                          uint32_t processId,
                                          uint32_t source,
                                          OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  /* write next node in List */
  OTF1Event event;
  event.location = processId;
  event.regionRef = functionId;
  event.time = time;
  event.type = OTF1_ENTER;
  
  writer->eventStack.push(functionId);
  
  if ( !writer->processNextNode( event ) )
  {
    OTF_CHECK( OTF_WStream_writeEnterKV( writer->processWStreamMap[processId],
                                   time, functionId,
                                   processId, source, list ) );
  }

  return OTF_RETURN_OK;

}

int
OTF1ParallelTraceWriter::otf1HandleLeave( void* userData,
                                          uint64_t time,
                                          uint32_t functionId,
                                          uint32_t processId,
                                          uint32_t source,
                                          OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  /* write next node in List */
  OTF1Event event;
  event.location = processId;
  event.regionRef = functionId;
  event.time = time;
  event.type = OTF1_LEAVE;
  
  if ( !writer->processNextNode( event ) )
  {
    OTF_CHECK( OTF_WStream_writeLeaveKV( writer->processWStreamMap[processId],
                                   time, functionId,
                                   processId, source, list ) );
  }

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleSendMsg( void* userData,
                                            uint64_t time,
                                            uint32_t sender,
                                            uint32_t receiver,
                                            uint32_t group,
                                            uint32_t type,
                                            uint32_t length,
                                            uint32_t source,
                                            OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeSendMsgKV( writer->processWStreamMap[sender],
                                         time, sender, receiver,
                                         group, type, length, source, list ) );

  return OTF_RETURN_OK;

}

int
OTF1ParallelTraceWriter::otf1HandleRecvMsg( void* userData,
                                            uint64_t time,
                                            uint32_t receiver,
                                            uint32_t sender,
                                            uint32_t group,
                                            uint32_t type,
                                            uint32_t length,
                                            uint32_t source,
                                            OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeRecvMsgKV( writer->processWStreamMap[receiver],
                                         time, receiver, sender,
                                         group, type, length, source, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleBeginCollectiveOperation( void* userData,
                                                             uint64_t time,
                                                             uint32_t process,
                                                             uint32_t collOp,
                                                             uint64_t
                                                             matchingId,
                                                             uint32_t procGroup,
                                                             uint32_t rootProc,
                                                             uint64_t sent,
                                                             uint64_t received,
                                                             uint32_t scltoken,
                                                             OTF_KeyValueList*
                                                             list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  int OTF_WStream_writeBeginCollectiveOperationKV( OTF_WStream * wstream,
                                                   uint64_t time,
                                                   uint32_t process,
                                                   uint32_t collOp,
                                                   uint64_t matchingId,
                                                   uint32_t procGroup,
                                                   uint32_t rootProc,
                                                   uint64_t sent,
                                                   uint64_t received,
                                                   uint32_t scltoken,
                                                   OTF_KeyValueList * list );

  OTF_CHECK( OTF_WStream_writeBeginCollectiveOperation( writer->
                                                        processWStreamMap[
                                                          process], time,
                                                        process, collOp,
                                                        matchingId, procGroup,
                                                        rootProc, sent,
                                                        received, scltoken ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleEndCollectiveOperation( void* userData,
                                                           uint64_t time,
                                                           uint32_t process,
                                                           uint64_t matchingId,
                                                           OTF_KeyValueList*
                                                           list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeEndCollectiveOperation( writer->processWStreamMap
                                                      [process], time, process,
                                                      matchingId ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleRMAEnd( void* userData,
                                           uint64_t time,
                                           uint32_t process,
                                           uint32_t remote,
                                           uint32_t communicator,
                                           uint32_t tag,
                                           uint32_t source,
                                           OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeRMAEndKV( writer->processWStreamMap[process],
                                        time, process, remote,
                                        communicator, tag, source, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleRMAGet( void* userData,
                                           uint64_t time,
                                           uint32_t process,
                                           uint32_t origin,
                                           uint32_t target,
                                           uint32_t communicator,
                                           uint32_t tag,
                                           uint64_t bytes,
                                           uint32_t source,
                                           OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeRMAGetKV( writer->processWStreamMap[process],
                                        time, process, origin,
                                        target, communicator, tag, bytes,
                                        source, list ) );

  return OTF_RETURN_OK;
}

int
OTF1ParallelTraceWriter::otf1HandleRMAPut( void* userData,
                                           uint64_t time,
                                           uint32_t process,
                                           uint32_t origin,
                                           uint32_t target,
                                           uint32_t communicator,
                                           uint32_t tag,
                                           uint64_t bytes,
                                           uint32_t source,
                                           OTF_KeyValueList* list )
{
  OTF1ParallelTraceWriter* writer = (OTF1ParallelTraceWriter*)userData;

  OTF_CHECK( OTF_WStream_writeRMAPutKV( writer->processWStreamMap[process],
                                        time, process, origin,
                                        target, communicator, tag, bytes,
                                        source, list ) );

  return OTF_RETURN_OK;
}
