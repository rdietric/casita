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
 * - create trace reader and trigger reading
 * - merge statistics from different streams (blame, time on CP, total length CP)
 * - trigger computation of critical path:
 * - getCriticalPath()
 *      * 1 MPI process  -> getCriticalPathIntern()-> graph.getLongestPath()
 *      * >1 MPI process -> reverseReplayMPICriticalPath()
 *                       -> getCriticalLocalSections() -> getCriticalPathIntern() for all sections created in replay, add nodes to set of critical nodes
 *      * Compute global length of CP (TODO: right now this takes the difference between first and last global timestamp...)
 * - runAnalysis() -> goes through all nodes (non-CPU) and applies rules for a given paradigm
 * - printAllActivities() -> print the summary for time on CP, Blame, etc.
 */

#include <sys/types.h>
#include <sys/time.h>

#include <omp.h>

#include "Runner.hpp"

#include "otf/IKeyValueList.hpp"
#include "otf/ITraceReader.hpp"

#include "otf/OTF2TraceReader.hpp"

using namespace casita;
using namespace casita::io;

Runner::Runner( int mpiRank, int mpiSize ) :
  mpiRank( mpiRank ),
  mpiSize( mpiSize ),
  analysis( mpiRank, mpiSize ),
  options( Parser::getInstance( ).getProgramOptions( ) ),
  callbacks( options, analysis ),
  globalLengthCP( 0 )
{
  if ( options.noErrors )
  {
    ErrorUtils::getInstance( ).setNoExceptions( );
  }

  if ( options.verbose )
  {
    UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_BASIC,
               "Enabled verbose output for error utils\n" );
    ErrorUtils::getInstance( ).setVerbose( );
  }
}

Runner::~Runner( )
{
}

ProgramOptions&
Runner::getOptions( )
{
  return options;
}

AnalysisEngine&
Runner::getAnalysis( )
{
  return analysis;
}

uint64_t
Runner::getGlobalLengthCP( )
{
  return globalLengthCP;
}

void
Runner::startAnalysisRun( )
{
  ITraceReader* traceReader = NULL;

  UTILS_MSG( mpiRank == 0, "[0] Reading %s", options.filename.c_str( ) );

  if ( strstr( options.filename.c_str( ), ".otf2" ) != NULL )
  {
    traceReader = new OTF2TraceReader( &callbacks, mpiRank, mpiSize );
  }

  if ( !traceReader )
  {
    throw RTException( "Could not create trace reader" );
  }

  // set the OTF2 callback handlers
  traceReader->handleDefProcess        = CallbackHandler::handleDefProcess;
  traceReader->handleDefFunction       = CallbackHandler::handleDefFunction;
  traceReader->handleEnter             = CallbackHandler::handleEnter;
  traceReader->handleLeave             = CallbackHandler::handleLeave;
  traceReader->handleProcessMPIMapping = CallbackHandler::handleProcessMPIMapping;
  traceReader->handleMPIComm           = CallbackHandler::handleMPIComm;
  traceReader->handleMPICommGroup      = CallbackHandler::handleMPICommGroup;
  traceReader->handleMPIIsend          = CallbackHandler::handleMPIIsend;
  traceReader->handleMPIIrecv          = CallbackHandler::handleMPIIrecv;
  traceReader->handleMPIIrecvRequest   = CallbackHandler::handleMPIIrecvRequest;
  traceReader->handleMPIIsendComplete  = CallbackHandler::handleMPIIsendComplete;

  traceReader->open( options.filename, 10 );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading definitions ... " );
  
  // read the OTF2 definitions and initialize some maps and variables of the trace reader
  if( false == traceReader->readDefinitions( ) )
  {
    throw RTException( "Error while reading definitions!" );
  }
    
  // TODO:
  analysis.getMPIAnalysis( ).createMPICommunicatorsFromMap( );
  
  // TODO: 
  analysis.setWaitStateFunctionId( analysis.getNewFunctionId( ) );

  uint64_t timerResolution = traceReader->getTimerResolution( );
  analysis.setTimerResolution( timerResolution );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Timer resolution = %llu",
             (long long unsigned)( timerResolution ) );

  if ( timerResolution < 1000000000 ) /* 1GHz */
  {
    UTILS_MSG( mpiRank == 0, 
               "[0] Warning: your timer resolution is very low (< 1 GHz)!" );
  }

  // setup reading events
  traceReader->setupEventReader( options.ignoreAsyncMpi );
  
  // read events from the trace, build a graph and do the analysis
  processTrace( traceReader );
  
  UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_BASIC &&
             options.analysisInterval,
             "[0] Global collectives found: %u", analysis.getMPIAnalysis().globalCollectiveCounter );

  traceReader->close( );
  delete traceReader;
}

/**
 * Processes the input trace and write the output trace. 
 * (in intervals if specified)
 * 1) Reads the trace 
 *  1.1) Generates a graph
 *  1.2) Prepares nodes with according dependencies
 * 2) Performs the analysis for each paradigm
 *   2.1) Wait state and blame analysis
 * 3) Performs critical path analysis
 * 4) Writes the output trace with additional counters
 * 
 * @param traceReader a fully set up trace reader (definitions are read)
 */
void
Runner::processTrace( ITraceReader* traceReader )
{
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading events ..." );
  
  ////////////////////////////////////////////////////////
  // reading events, until the end of the trace is reached
  
  // minimum number of graph nodes required to start analysis
  uint32_t analysis_intervals = 0;
  
  bool events_available = false;
  bool otf2_def_written = false;
  uint32_t interval_node_id = 0;
  uint64_t total_events_read = 0;
  do
  {
    uint64_t events_read = 0;
    // read events until global collective (with global event reader)
    events_available = traceReader->readEvents( &events_read );
    total_events_read += events_read;
    
    //\todo: separate function?
    // for interval analysis
    if( events_available )
    {
      bool start_analysis = false;
      uint32_t nodeSizes[mpiSize];
      
      // get the last node's ID
      uint32_t last_node_id = analysis.getGraph().getNodes().back()->getId();
      uint32_t current_pending_nodes = last_node_id - interval_node_id;
      interval_node_id = last_node_id;
      
      // gather pending nodes on all processes
      MPI_CHECK( MPI_Allgather( &current_pending_nodes, 1, MPI_UNSIGNED_LONG,
                                nodeSizes, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD ) );

      // check all processes for a reasonable number of analyzable nodes
      for ( int i = 0; i < mpiSize; ++i )
      {
        if ( options.analysisInterval < nodeSizes[i] )
        {
          start_analysis = true;
          break;
        }
      }
      
      // if we didn't find a process with enough work, continue reading
      if( !start_analysis )
      {
        continue;
      }
      
      // increase counter of analysis intervals
      ++analysis_intervals;
    }

    // perform analysis for these events
    
    // get a sorted list of nodes
    EventStream::SortedGraphNodeList allNodes;
    analysis.getAllNodes( allNodes );
    
    // apply analysis to all nodes of a certain paradigm
    // availability of a certain paradigm is checked when the trace is read
    // analysis creates dependency edges, identifies wait states, distributes blame
    if ( analysis.haveParadigm( PARADIGM_CUDA ) )
    {
      runAnalysis( PARADIGM_CUDA, allNodes );
    }
    
    if ( analysis.haveParadigm( PARADIGM_OMP ) )
    {
      runAnalysis( PARADIGM_OMP, allNodes );
    }
    
    //\todo: implement check for MPI paradigm
    runAnalysis( PARADIGM_MPI,  allNodes );
    
    // \todo: to run the CPA we need all nodes on all processes to be analyzed?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    UTILS_MSG( mpiRank == 0 && !options.analysisInterval, 
               "[0] Computing the critical path" );
    
    // check for pending non-blocking MPI!
    analysis.checkPendingMPIRequests( );

    // initiate the detection of the critical path
    computeCriticalPath( );
    
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    
    // write the first already analyzed part (with local event readers and writers)
    
    // write OTF2 definitions for this MPI rank (only once)
    if( !otf2_def_written )
    {
      // the following function also reads the OTF2 again and computes blame for CPU functions
      analysis.writeOTF2Definitions( options.outOtfFile,
                                     options.filename,
                                     options.createOTF, // let the trace writer know if it should write an OTF output
                                     options.ignoreAsyncMpi,
                                     options.verbose );
      
      UTILS_MSG( mpiRank == 0, "[0] Writing result to %s", options.outOtfFile.c_str( ) );

      otf2_def_written = true;
    }
    
    if( options.createOTF )
    {
      MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    }

    if( analysis.writeOTF2EventStreams( options.verbose ) == false && events_available )
    {
      UTILS_MSG( true, "[%u] Reader and writer are not synchronous! Aborting ...", 
                    mpiRank );
      events_available = false;
    }
    
    // clear the sorted graph node list
    allNodes.clear();
    
    // deletes all previous nodes and create an intermediate start point
    if( events_available )
    {
      // create intermediate graph (reset and clear graph objects)
      analysis.createIntermediateBegin();
      
      // reset MPI-related objects
      analysis.getMPIAnalysis( ).reset();
    }
    
    // necessary?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  } while( events_available );
  
  UTILS_MSG( options.verbose >= VERBOSE_BASIC, 
             "[%u] Total number of processed events: %llu", 
             mpiRank, total_events_read );
  
  UTILS_MSG( mpiRank == 0 && options.analysisInterval,
             "[0] Number of analysis intervals: %u", ++analysis_intervals );
}

void
Runner::mergeActivityGroups( )
{
  IParallelTraceWriter::ActivityGroupMap* activityGroupMap =
    analysis.getActivityGroupMap( );
  
  assert( activityGroupMap );

  uint64_t globalBlame    = 0;
  uint64_t lengthCritPath =
    analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL )->getTime( ) -
    analysis.getSourceNode( )->getTime( );

  /* compute some final metrics */
  for ( IParallelTraceWriter::ActivityGroupMap::iterator groupIter =
          activityGroupMap->begin( ); groupIter != activityGroupMap->end( ); )
  {
    std::map< uint32_t,
              IParallelTraceWriter::ActivityGroup >::iterator iter = groupIter;
    groupIter->second.fractionCP =
      (double)( groupIter->second.totalDurationOnCP ) / (double)lengthCritPath;
    globalBlame += groupIter->second.totalBlame;
    groupIter = ++iter;
  }

  /* phase 2: MPI all-reduce */
  const int MPI_NUM_TAG   = 21;
  const int MPI_ENTRIES_TAG = 22;
  
  //\todo: Would a broadcast fit better here?
  // send/receive groups to master/from other MPI streams
  if ( 0 == mpiRank )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Combining results from %d analysis processes", mpiSize );

    // receive from all other MPI streams
    for ( int rank = 1; rank < mpiSize; ++rank )
    {
      // receive number of entries
      uint32_t numEntries = 0;
      MPI_CHECK( MPI_Recv( &numEntries, 1, MPI_UNSIGNED, rank, MPI_NUM_TAG, 
                           MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );

      // receive the entries
      if ( numEntries > 0 )
      {
        IParallelTraceWriter::ActivityGroup* buf =
          new IParallelTraceWriter::ActivityGroup[numEntries];
        MPI_CHECK( MPI_Recv( buf,
                   numEntries * sizeof( IParallelTraceWriter::ActivityGroup ),
                   MPI_BYTE,
                   rank,
                   MPI_ENTRIES_TAG,
                   MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );

        /* combine with own activity groups */
        IParallelTraceWriter::ActivityGroupMap::iterator groupIter;
        for ( uint32_t i = 0; i < numEntries; ++i )
        {
          IParallelTraceWriter::ActivityGroup* group = &( buf[i] );
          uint32_t fId = group->functionId;
          groupIter = activityGroupMap->find( fId );

          if ( groupIter != activityGroupMap->end( ) )
          {
            groupIter->second.numInstances      += group->numInstances;
            groupIter->second.totalBlame        += group->totalBlame;
            groupIter->second.totalDuration     += group->totalDuration;
            groupIter->second.totalDurationOnCP += group->totalDurationOnCP;
            groupIter->second.fractionCP        += group->fractionCP;
            groupIter->second.numUnifyStreams++;
          }
          else
          {
            ( *activityGroupMap )[fId].functionId        = fId;
            ( *activityGroupMap )[fId].numInstances      = group->numInstances;
            ( *activityGroupMap )[fId].fractionCP        = group->fractionCP;
            ( *activityGroupMap )[fId].totalBlame        = group->totalBlame;
            ( *activityGroupMap )[fId].totalDuration     = group->totalDuration;
            ( *activityGroupMap )[fId].totalDurationOnCP = group->totalDurationOnCP;
            ( *activityGroupMap )[fId].numUnifyStreams   = group->numUnifyStreams;
          }

          globalBlame += group->totalBlame;
        }

        delete[]buf;
      }
    }

    for ( IParallelTraceWriter::ActivityGroupMap::iterator groupIter =
            activityGroupMap->begin( );
          groupIter != activityGroupMap->end( ); ++groupIter )
    {
      std::map< uint32_t,
                IParallelTraceWriter::ActivityGroup >::iterator iter =
        groupIter;
      groupIter->second.fractionCP /= (double)( iter->second.numUnifyStreams );
      if ( globalBlame > 0 )
      {
        groupIter->second.fractionBlame =
          (double)( groupIter->second.totalBlame ) / (double)globalBlame;
      }
      else
      {
        groupIter->second.fractionBlame = 0.0;
      }
    }
  }
  else
  {
    uint32_t i          = 0;
    uint32_t numEntries = activityGroupMap->size( );

    // send number of entries to root rank
    MPI_CHECK( MPI_Send( &numEntries, 1, MPI_UNSIGNED, 0, 
                         MPI_NUM_TAG, MPI_COMM_WORLD ) );

    IParallelTraceWriter::ActivityGroup* buf =
      new IParallelTraceWriter::ActivityGroup[numEntries];

    for ( IParallelTraceWriter::ActivityGroupMap::iterator groupIter =
            activityGroupMap->begin( );
          groupIter != activityGroupMap->end( ); ++groupIter )
    {
      memcpy( &( buf[i] ), &( groupIter->second ),
              sizeof( IParallelTraceWriter::ActivityGroup ) );
      ++i;
    }

    // send entries to root rank
    MPI_CHECK( MPI_Send( buf,
               numEntries * sizeof( IParallelTraceWriter::ActivityGroup ),
               MPI_CHAR,
               0, 
               MPI_ENTRIES_TAG,
               MPI_COMM_WORLD ) );

    delete[]buf;
  }
}

/**
 * Compute critical path, i.e. all nodes on the critical path, add counters.
 * Depending on the number of processes this is done via MPI reverse-replay
 * and following parallel computation for critical sections between MPI nodes
 * on each process.
 * If there's only one process, the longest path is calculated serially.
 * (One process can have multiple event streams)
 */
void
Runner::computeCriticalPath( )
{
  EventStream::SortedGraphNodeList criticalNodes;
  
  if ( mpiSize > 1 )
  {
    MPIAnalysis::CriticalSectionsList sectionsList;
    
    // perform MPI reverse replay using blocking edges; create a list of sections
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Start MPI revers-replay %s ...",
               (options.criticalPathSecureMPI) ? "(with slave feedback)" : "" );
    reverseReplayMPICriticalPath( sectionsList );
    
    // detect the critical path within all sections individually
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Start critical-path detection for sections ..." );
    getCriticalLocalNodes( sectionsList, criticalNodes );
    
    sectionsList.clear();
  }
  else
  {
    // compute local critical path on root, only
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Single process: defaulting to CUDA/OMP only mode" );

    Graph& subGraph = analysis.getGraph( );

    getCriticalPathIntern( analysis.getSourceNode( ),
                           analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL ),
                           criticalNodes, subGraph );
    
    subGraph.cleanup( true );
  }

  /* compute the time-on-critical-path counter */
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[%u] Computing additional counters", mpiRank );

  const uint32_t cpCtrId = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  for ( EventStream::SortedGraphNodeList::const_iterator iter = criticalNodes.begin( );
        iter != criticalNodes.end( ); ++iter )
  {
    ( *iter )->setCounter( cpCtrId, 1 );
  }
  
  if ( options.mergeActivities )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[%u] Calculate total length of critical path", mpiRank );

    uint64_t firstTimestamp = 0;
    uint64_t lastTimestamp  = analysis.getLastGraphNode( )->getTime( );

    // compute total program runtime, i.e. total length of the critical path
    if ( criticalNodes.size( ) > 0 )
    {
      GraphNode* firstNode = NULL;
      GraphNode* lastNode  = NULL;

      for ( EventStream::SortedGraphNodeList::const_iterator iter =
              criticalNodes.begin( );
            iter != criticalNodes.end( ); ++iter )
      {
        GraphNode* currentEvent = *iter;
        if ( !firstNode || ( Node::compareLess( currentEvent, firstNode ) ) )
        {
          firstNode = currentEvent;
        }

        if ( !lastNode || ( Node::compareLess( lastNode, currentEvent ) ) )
        {
          lastNode = currentEvent;
        }

      }

      firstTimestamp = firstNode->getTime( );
      if ( firstNode->isMPIInit( ) && firstNode->isLeave( ) )
      {
        firstTimestamp = firstNode->getPartner( )->getTime( );
      }
      lastTimestamp  = lastNode->getTime( );
    }

    if ( mpiSize > 1 )
    {
      MPI_CHECK( MPI_Allreduce( &firstTimestamp,
                     &firstTimestamp,
                     1,
                     MPI_UNSIGNED_LONG_LONG,
                     MPI_MIN,
                     MPI_COMM_WORLD ) );
      MPI_CHECK( MPI_Allreduce( &lastTimestamp,
                     &lastTimestamp,
                     1,
                     MPI_UNSIGNED_LONG_LONG,
                     MPI_MAX,
                     MPI_COMM_WORLD ) );
    }

    globalLengthCP = lastTimestamp - firstTimestamp;
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Critical path length = %f",
               analysis.getRealTime( globalLengthCP ) );

    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  }
}

/**
 * Compute the critical path between two nodes of the same process
 * (might have multiple event streams).
 *
 * @param start
 * @param end
 * @param cpNodes - list to store nodes of computed cp in
 * @param subGraph - subgraph between the two nodes
 */
void
Runner::getCriticalPathIntern( GraphNode*                        start,
                               GraphNode*                        end,
                               EventStream::SortedGraphNodeList& cpNodes,
                               Graph&                            subGraph )
{
  UTILS_MSG( options.printCriticalPath, "\n[%u] Longest path (%s,%s):",
             mpiRank,
             start->getUniqueName( ).c_str( ),
             end->getUniqueName( ).c_str( ) );

  GraphNode::GraphNodeList criticalPath;
  subGraph.getLongestPath( start, end, criticalPath );

  uint32_t i = 0;
  for ( GraphNode::GraphNodeList::const_iterator cpNode = criticalPath.begin( );
        cpNode != criticalPath.end( ); ++cpNode )
  {
    GraphNode* node = *cpNode;
    cpNodes.push_back( node );

    UTILS_MSG( options.printCriticalPath, "[%u] %u: %s (%f)",
               mpiRank, i,
               node->getUniqueName( ).c_str( ),
               analysis.getRealTime( node->getTime( ) ) );

    i++;
  }
}

/**
 * Compute critical path for nodes between two critical MPI nodes on the
 * same process.
 *
 * @param sections list of critical sections for this process
 * @param criticalNodes - list of local critical nodes
 */
void
Runner::getCriticalLocalNodes( MPIAnalysis::CriticalSectionsList& sections,
                               EventStream::SortedGraphNodeList& criticalNodes )
{
  uint32_t   mpiRank    = analysis.getMPIRank( );
  Graph*     subGraph   = analysis.getGraph( PARADIGM_ALL );

  uint32_t   lastSecCtr = 0;

#pragma omp parallel for
  for ( uint32_t i = 0; i < sections.size(); ++i )
  {
    MPIAnalysis::CriticalPathSection* section = &( sections[i] );

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%u] computing local critical path between MPI nodes [%u, %u] on process %lu",
               mpiRank,
               section->nodeStartID,
               section->nodeEndID,
               section->streamID );

    /* find local MPI nodes with their IDs */
    GraphNode* startNode = NULL, * endNode = NULL;
    const EventStream::SortedGraphNodeList& mpiNodes =
      analysis.getStream( sections.data()->streamID )->getNodes( );
      //analysis.getStream( sections.front().streamID )->getNodes( );
    for ( EventStream::SortedGraphNodeList::const_iterator iter =
            mpiNodes.begin( );
          iter != mpiNodes.end( ); ++iter )
    {
      if ( !( *iter )->isMPI( ) )
      {
        continue;
      }

      if ( startNode && endNode )
      {
        break;
      }

      GraphNode* node = (GraphNode*)( *iter );
      if ( node->getId( ) == section->nodeStartID )
      {
        startNode = node;
        continue;
      }

      if ( node->getId( ) == section->nodeEndID )
      {
        endNode = node;
        continue;
      }
    }

    if ( !startNode || !endNode )
    {
      throw RTException( "[%u] Did not find local nodes for node IDs %u and %u",
                         mpiRank, section->nodeStartID, section->nodeEndID );
    }
    else
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL,
                 "[%u] node mapping: %u = %s (%f), %u = %s (%f)",
                 mpiRank, section->nodeStartID,
                 startNode->getUniqueName( ).c_str( ),
                 analysis.getRealTime( startNode->getTime( ) ),
                 section->nodeEndID, endNode->getUniqueName( ).c_str( ),
                 analysis.getRealTime( endNode->getTime( ) ) );
    }

    if ( startNode->isEnter( ) )
    {
      startNode = startNode->getPartner( );
    }

    if ( endNode->isLeave( ) )
    {
      endNode = endNode->getPartner( );
    }

    GraphNode* startLocalNode = startNode->getLinkRight( );
    GraphNode* endLocalNode   = endNode->getLinkLeft( );

    if ( ( !startLocalNode ||
           !endLocalNode ) ||
         ( startLocalNode->getTime( ) >= endLocalNode->getTime( ) ) )
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL,
                 "[%u] No local path possible between MPI nodes %s (link right %p, %s) "
                 "and %s (link left %p, %s)",
                 mpiRank,
                 startNode->getUniqueName( ).c_str( ),
                 startLocalNode, startLocalNode->getUniqueName( ).c_str( ),
                 endNode->getUniqueName( ).c_str( ),
                 endLocalNode, endLocalNode->getUniqueName( ).c_str( ) );

      UTILS_MSG( options.verbose > VERBOSE_ALL && startLocalNode && endLocalNode,
                 "[%u] local nodes %s and %s",
                 mpiRank,
                 startLocalNode->getUniqueName( ).c_str( ),
                 endLocalNode->getUniqueName( ).c_str( ) );

      continue;
    }

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%u] MPI/local mapping: %u > %s, %u > %s",
               mpiRank, section->nodeStartID,
               startLocalNode->getUniqueName( ).c_str( ),
               section->nodeEndID, endLocalNode->getUniqueName( ).c_str( ) );

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%u] Computing local critical path (%s, %s)", mpiRank,
               startLocalNode->getUniqueName( ).c_str( ),
               endLocalNode->getUniqueName( ).c_str( ) );

    EventStream::SortedGraphNodeList sectionLocalNodes;
    sectionLocalNodes.push_back( startNode );

    getCriticalPathIntern( startLocalNode,
                           endLocalNode,
                           sectionLocalNodes,
                           *subGraph );

    sectionLocalNodes.push_back( endNode );
    if ( endNode->isMPIFinalize( ) && endNode->isEnter( ) )
    {
      sectionLocalNodes.push_back( endNode->getPartner( ) );
    }

    #pragma omp critical
    {
      criticalNodes.insert( criticalNodes.end( ),
                            sectionLocalNodes.begin( ), sectionLocalNodes.end( ) );
    }

    if ( options.verbose >= VERBOSE_BASIC && !options.analysisInterval &&
        mpiRank == 0 && omp_get_thread_num( ) == 0 &&
        ( i - lastSecCtr > sections.size() / 10 ) )
    {
      UTILS_MSG( true, "[0] %lu%% ",
                 (size_t)( 100.0 * (double)i / (double)(sections.size()) ) );
      
      fflush( NULL );
      lastSecCtr = i;
    }
  }

  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0 && !options.analysisInterval,
             "[0] 100%%" );

  delete subGraph;
}

/**
 * Find globally last MPI node.
 *
 * @param node - store found last MPI node in this variable
 */
void
Runner::findLastMpiNode( GraphNode** node )
{
  uint64_t   lastMpiNodeTime = 0;
  int        lastMpiRank     = mpiRank;
  uint64_t   nodeTimes[mpiSize];
  *node = NULL;

  GraphNode* myLastMpiNode   = analysis.getLastGraphNode( PARADIGM_MPI );
  if ( myLastMpiNode )
  {
    lastMpiNodeTime = myLastMpiNode->getTime( );
  }

  MPI_CHECK( MPI_Allgather( &lastMpiNodeTime, 1, MPI_UNSIGNED_LONG_LONG,
                            nodeTimes,
                            1, MPI_UNSIGNED_LONG_LONG, MPI_COMM_WORLD ) );

  for ( int i = 0; i < mpiSize; ++i )
  {
    if ( nodeTimes[i] > lastMpiNodeTime )
    {
      lastMpiNodeTime = nodeTimes[i];
      lastMpiRank     = i;
    }
  }
  if ( lastMpiRank == mpiRank )
  {
    *node = myLastMpiNode;
    myLastMpiNode->setCounter( analysis.getCtrTable( ).getCtrId(
                                 CTR_CRITICALPATH ), 1 );

    UTILS_DBG_MSG( DEBUG_CPA_MPI,
               "[%u] critical path reverse replay starts at node %s (%f)",
               mpiRank, myLastMpiNode->getUniqueName( ).c_str( ),
               analysis.getRealTime( myLastMpiNode->getTime( ) ) );

  }
}

/**
 * Perform reverse replay for all MPI nodes, detect wait states
 * and the critical path
 *
 * @param sectionsList - critical sections between MPI regions
 *                       on the critical path will be stored in this list
 */
void
Runner::reverseReplayMPICriticalPath( MPIAnalysis::CriticalSectionsList& sectionsList )
{
  const uint32_t NO_MSG         = 0;
  const uint32_t PATH_FOUND_MSG = 1;
  
  const int MPI_CPA_TAG    = 19;
  const int MPI_CPA_FB_TAG = 20;
  
  // we need an additional buffer entry for master/slave feedback communication
  const size_t   BUFFER_SIZE    = ( options.criticalPathSecureMPI ) ? 4 : 3;

  // decide on global last MPI node to start with
  GraphNode*     currentNode    = NULL;
  GraphNode*     lastNode       = NULL;
  GraphNode*     sectionEndNode = NULL;
  bool isMaster     = false;

  findLastMpiNode( &currentNode );
  if ( currentNode )
  {
    isMaster       = true;
    sectionEndNode = currentNode;
  }

  // mpiGraph is an allocated graph object with a vector of all nodes of the 
  // given paradigm (\TODO this might be extremely memory intensive)
  Graph*   mpiGraph = analysis.getGraph( PARADIGM_MPI );
  uint32_t cpCtrId  = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  uint64_t sendBfr[BUFFER_SIZE];
  uint64_t recvBfr[BUFFER_SIZE];

  while ( true )
  {
    if ( isMaster )
    {
      // master
      if ( lastNode )
      {
        UTILS_MSG( options.verbose >= VERBOSE_ANNOY,
                   "[%u] isMaster, currentNode = %s (%f), lastNode = %s (%f)",
                   mpiRank, currentNode->getUniqueName( ).c_str( ),
                   analysis.getRealTime( currentNode->getTime( ) ),
                   lastNode->getUniqueName( ).c_str( ),
                   analysis.getRealTime( lastNode->getTime( ) ) );
      }
      else
      {
        UTILS_MSG( options.verbose >= VERBOSE_ANNOY,
                   "[%u] isMaster, currentNode = %s (%f)",
                   mpiRank, currentNode->getUniqueName( ).c_str( ),
                   analysis.getRealTime( currentNode->getTime( ) ) );
      }

      UTILS_MSG( lastNode && ( lastNode->getId( ) <= currentNode->getId( ) ),
                 "[%u] ! [Warning] current node ID %u (%s) is not strictly decreasing"
                 "; last node ID %u (%s)", 
                 mpiRank, currentNode->getId( ), currentNode->getUniqueName().c_str(),
                 lastNode->getId( ), lastNode->getUniqueName().c_str() );

      if ( currentNode->isLeave( ) )
      {
        // isLeave
        Edge* activityEdge = analysis.getEdge(
          currentNode->getPartner( ), currentNode );

        // CP changes stream on blocking edges or if we are at MPI_Init()
        if ( activityEdge->isBlocking( ) || currentNode->isMPIInit( ) )
        {
          // create section for local processing later
          if ( sectionEndNode && ( currentNode != sectionEndNode ) )
          {
            MPIAnalysis::CriticalPathSection section;
            section.streamID    = currentNode->getStreamId( );
            section.nodeStartID = currentNode->getId( );
            section.nodeEndID   = sectionEndNode->getId( );
            sectionsList.push_back( section );

            currentNode->setCounter( cpCtrId, 1 );
          }
          
          // communicate with slaves to decide on new master
          GraphNode* commMaster  = currentNode->getGraphPair( ).second;
          bool nodeHasRemoteInfo = false;
          analysis.getMPIAnalysis( ).getRemoteNodeInfo( commMaster,
                                                        &nodeHasRemoteInfo );
          if ( !nodeHasRemoteInfo )
          {
            commMaster = currentNode->getGraphPair( ).first;
          }
          
          UTILS_DBG_MSG( DEBUG_CPA_MPI,
                         "[%u]  found wait state for %s (%f), changing stream at %s",
                         mpiRank, currentNode->getUniqueName( ).c_str( ),
                         analysis.getRealTime( currentNode->getTime( ) ),
                         commMaster->getUniqueName( ).c_str( ) );

          // find communication partners
          std::set< uint32_t > mpiPartnerRanks =
            analysis.getMPIAnalysis( ).getMpiPartnersRank( commMaster );
          
          // Communicate with all slaves to find new master.
          // prepare send buffer
          sendBfr[0] = commMaster->getId( );
          sendBfr[1] = commMaster->getStreamId( );
          sendBfr[2] = NO_MSG;
            
          // Make sure that not all ranks are slave then!
          sendBfr[BUFFER_SIZE - 1] = 0; // we did not find a master yet
          for ( std::set< uint32_t >::const_iterator iter = mpiPartnerRanks.begin( );
                iter != mpiPartnerRanks.end( ); ++iter )
          {
            uint32_t commMpiRank = *iter;

            // do not communicate with myself
            if ( commMpiRank == (uint32_t)mpiRank )
            {
              continue;
            }
            
            UTILS_DBG_MSG( DEBUG_CPA_MPI,
                           "[%u]  testing remote MPI worker %u for remote edge to my node %u on stream %lu",
                           mpiRank,
                           commMpiRank,
                           (uint32_t)sendBfr[0],
                           sendBfr[1] );

            //TODO: MPI_Bcast( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, mpiRank, comm) 
            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, MPI_CPA_TAG, MPI_COMM_WORLD ) );
            
            // if no master has been found yet, receive from next slave
            if ( options.criticalPathSecureMPI && 
                 sendBfr[BUFFER_SIZE - 1] == 0 )
            {
              MPI_CHECK( MPI_Recv( recvBfr, 1, MPI_UNSIGNED_LONG_LONG,
                                   commMpiRank, MPI_CPA_FB_TAG, 
                                   MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );
            
              // if we found a new master
              if( recvBfr[0] )
              {
                UTILS_MSG( options.verbose == VERBOSE_ALL,
                           "[%u] Rank %u will be the new master.", 
                           mpiRank, commMpiRank); 
                
                sendBfr[BUFFER_SIZE - 1] = 1;
              }
            }
          }
          
          // free the set
          mpiPartnerRanks.clear();
                    
          // if no new master has been found, continue main loop as master
          if ( options.criticalPathSecureMPI && sendBfr[BUFFER_SIZE - 1] == 0 )
          {
            // this should not happen, but we do not want to abort here
            
            currentNode->setCounter( cpCtrId, 1 );
            lastNode    = currentNode;
            currentNode = activityEdge->getStartNode( );
            
            UTILS_MSG( true , "[%u] No new master could be found! "
                              "Continuing on node %s ", mpiRank, 
                              currentNode->getUniqueName( ).c_str( ) );
          }
          else // continue main loop as slave
          {
            // make myself a new slave
            isMaster = false;
            sectionEndNode = NULL;
          }
        }
        else
        {
          currentNode->setCounter( cpCtrId, 1 );
          lastNode    = currentNode;
          currentNode = activityEdge->getStartNode( );
          // continue main loop as master
        }
      } /* END: currentNode->isLeave( ) */
      else // isEnter (master)
      {
        // found the MPI_Init enter or an atomic node
        if ( currentNode->isMPIInit( ) || currentNode->isAtomic( ) )
        {
          currentNode->setCounter( cpCtrId, 1 );

          // notify all slaves that we are done
          UTILS_MSG( options.verbose >= VERBOSE_BASIC && !options.analysisInterval, 
                     "[%u] Critical path reached global collective %s. "
                     "Asking all slaves to terminate", mpiRank, 
                     currentNode->getUniqueName().c_str() );

          // send "path found" message to all ranks
          sendBfr[0] = 0;
          sendBfr[1] = 0;
          sendBfr[2] = PATH_FOUND_MSG;

          std::set< uint64_t > mpiPartners =
            analysis.getMPIAnalysis( ).getMPICommGroup( 0 ).procs;
          
          for ( std::set< uint64_t >::const_iterator iter = mpiPartners.begin( );
                iter != mpiPartners.end( ); ++iter )
          {
            int commMpiRank = analysis.getMPIAnalysis( ).getMPIRank( *iter );
            
            // not to myself
            if ( commMpiRank == mpiRank )
            {
              continue;
            }

            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, MPI_CPA_TAG, MPI_COMM_WORLD ) );
          }

          // leave main loop
          break;
        }

        // master: find previous MPI node on the same stream
        bool foundPredecessor          = false;
        const Graph::EdgeList& inEdges = mpiGraph->getInEdges( currentNode );
        for ( Graph::EdgeList::const_iterator iter = inEdges.begin( );
              iter != inEdges.end( ); ++iter )
        {
          Edge* intraEdge = *iter;
          if ( intraEdge->isIntraStreamEdge( ) )
          {
            currentNode->setCounter( cpCtrId, 1 );
            lastNode         = currentNode;
            currentNode      = intraEdge->getStartNode( );
            foundPredecessor = true;
            
            // continue main loop as master 
            break;
          }
        }

        if ( !foundPredecessor )
        {
          throw RTException( "[%u] No ingoing intra-stream edge for node %s",
                             currentNode->getUniqueName( ).c_str( ) );
        }
      } // END: isEnter (master)
    } // END: master
    else
    { 
      // slave
      UTILS_DBG_MSG( DEBUG_CPA_MPI, "[%u] Slave receives... ", mpiRank);
      
      MPI_Status status;
      MPI_CHECK( MPI_Recv( recvBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                           MPI_ANY_SOURCE,
                           MPI_CPA_TAG, MPI_COMM_WORLD, &status ) );
      
      // if the master send "found message" we can leave the while loop
      if ( recvBfr[2] == PATH_FOUND_MSG )
      {
        UTILS_MSG( options.verbose >= VERBOSE_ALL,
                   "[%u] * terminate requested by master", mpiRank );
        break;
      }
   
      // find local node for remote node id and decide if we can continue here
      UTILS_DBG_MSG( DEBUG_CPA_MPI,
                     "[%u]  tested by remote MPI worker %u for remote edge to its node %u on stream %lu",
                     mpiRank,
                     status.MPI_SOURCE,
                     (uint32_t)recvBfr[0],
                     recvBfr[1] );

      // check if remote edge exists
      MPIAnalysis::MPIEdge mpiEdge;
      bool haveEdge = analysis.getMPIAnalysis( ).getRemoteMPIEdge( 
                                  (uint32_t)recvBfr[0], recvBfr[1], mpiEdge );

      // if we did not find a new master yet, send back whether we are the new master
      if ( options.criticalPathSecureMPI && recvBfr[BUFFER_SIZE - 1] == 0 )
      {
        //send information back to current master, if this rank has an edge
        int mpiMaster = analysis.getMPIAnalysis().getMPIRank( recvBfr[1] );
        sendBfr[0] = haveEdge;
        //std::cerr << "[" << mpiRank << "] Slave sends to " << mpiMaster 
        //          << " has Edge: " << haveEdge << std::endl;
        MPI_CHECK( MPI_Send( sendBfr, 1, MPI_UNSIGNED_LONG_LONG,
                             mpiMaster,
                             MPI_CPA_FB_TAG, MPI_COMM_WORLD ) );
      }
      
      if ( haveEdge )
      {
        GraphNode::GraphNodePair& slaveActivity =
          mpiEdge.localNode->getGraphPair( );
        
        // todo: sanity check MPI edge
        if( mpiEdge.localNode )
        {
          if( !(mpiEdge.localNode->getGraphPair().first) )
          {
            UTILS_DBG_MSG( true, "[%u] MPI remote edge first node is NULL!", mpiRank);
          }
          
          if( !(mpiEdge.localNode->getGraphPair().second) )
          {
            UTILS_DBG_MSG( true, "[%u] MPI remote edge second node is NULL!", mpiRank);
          }
        }
        else
        {
          UTILS_DBG_MSG( true, "[%u] MPI remote edge local node is NULL!", mpiRank);
        }
        
        //this rank is the new master
        isMaster       = true;
        slaveActivity.second->setCounter( cpCtrId, 1 );
        lastNode       = slaveActivity.second;
        currentNode    = slaveActivity.first;

        sectionEndNode = lastNode;

        UTILS_DBG_MSG( DEBUG_CPA_MPI,
                       "[%u] becomes new master at node %s, lastNode = %s\n",
                       mpiRank,
                       currentNode->getUniqueName( ).c_str( ),
                       lastNode->getUniqueName( ).c_str( ) );
        
        // continue main loop as master
      }
    }
  }
  
  // allocated before this loop and not bound to any other object
  delete mpiGraph;

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
}

/**
 * Apply all paradigm-specific rules to all nodes of that paradigm
 *
 * @param paradigm
 * @param allNodes
 */
void
Runner::runAnalysis( Paradigm                          paradigm,
                     EventStream::SortedGraphNodeList& allNodes )
{
  if ( !options.analysisInterval && mpiRank == 0 )
  {
    switch ( paradigm )
    {
      case PARADIGM_CUDA:
        UTILS_MSG( true, "[0] Running analysis: CUDA" );
        break;

      case PARADIGM_MPI:
        UTILS_MSG( true, "[0] Running analysis: MPI" );
        break;

      case PARADIGM_OMP:
        UTILS_MSG( true, "[0] Running analysis: OMP" );
        break;

      default:
        UTILS_MSG( true, "[0] No analysis for unknown paradigm %d", paradigm );
        return;
    }
  }

  size_t ctr       = 0, last_ctr = 0;
  size_t num_nodes = allNodes.size( );

  for ( EventStream::SortedGraphNodeList::const_iterator nIter = allNodes.begin( );
        nIter != allNodes.end( ); ++nIter )
  {
    GraphNode* node = *nIter;
    ctr++;

    //\todo: do we really want this?
    if ( !( node->getParadigm( ) & paradigm ) )
    {
      continue;
    }

    analysis.applyRules( node, paradigm, options.verbose >= VERBOSE_BASIC );

    // print process every 5 percent (TODO: depending on number of events per paradigm)
    if ( mpiRank == 0 && options.verbose >= VERBOSE_BASIC && !options.analysisInterval &&
         ( ctr - last_ctr > num_nodes / 20 ) )
    {
      UTILS_MSG( true, "[0] %lu%% ",
                 (size_t)( 100.0 * (double)ctr / (double)num_nodes ) );
      fflush( NULL );
      last_ctr = ctr;
    }
  }

  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0 && !options.analysisInterval, 
             "[0] 100%%" );

#ifdef DEBUG
  analysis.runSanityCheck( mpiRank );
#endif
}

/**
 * Print the summary statistics for regions with highest critical blame
 */
void
Runner::printAllActivities( )
{
  IParallelTraceWriter::ActivityGroupMap* activityGroupMap =
    analysis.getActivityGroupMap( );

  if ( mpiRank == 0 )
  {
    printf( "\n%50s %10s %10s %11s %12s %21s %9s\n",
            "Activity Group",
            "Instances",
            "Time",
            "Time on CP",
            "Fraction CP",
            "Fraction Global Blame",
            "Rating" );

    std::set< IParallelTraceWriter::ActivityGroup,
              IParallelTraceWriter::ActivityGroupCompare > sortedActivityGroups;

    for ( IParallelTraceWriter::ActivityGroupMap::iterator iter =
            activityGroupMap->begin( );
          iter != activityGroupMap->end( ); ++iter )
    {
      iter->second.fractionCP = 0.0;
      if ( iter->second.totalDurationOnCP > 0 )
      {
        iter->second.fractionCP =
          (double)iter->second.totalDurationOnCP / (double)globalLengthCP;
      }

      sortedActivityGroups.insert( iter->second );
    }

    const size_t max_ctr = 20;
    size_t ctr           = 0;
    for ( std::set< IParallelTraceWriter::ActivityGroup,
                    IParallelTraceWriter::ActivityGroupCompare >::
          const_iterator iter =
            sortedActivityGroups.begin( );
          iter != sortedActivityGroups.end( ) && ( ctr < max_ctr );
          ++iter )
    {
      ++ctr;
      printf( "%50.50s %10u %10f %11f %11.2f%% %20.2f%%  %7.6f\n",
              analysis.getFunctionName( iter->functionId ),
              iter->numInstances,
              analysis.getRealTime( iter->totalDuration ),
              analysis.getRealTime( iter->totalDurationOnCP ),
              100.0 * iter->fractionCP,
              100.0 * iter->fractionBlame,
              iter->fractionCP +
              iter->fractionBlame );
    }
  }
}
