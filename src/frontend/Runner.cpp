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
 *      * >1 MPI process -> detectCriticalPathMPIP2P()
 *                       -> getCriticalLocalSections() -> getCriticalPathIntern() for all sections created in replay, add nodes to set of critical nodes
 *      * Compute global length of CP (TODO: right now this takes the difference between first and last global timestamp...)
 * - runAnalysis() -> goes through all nodes (non-CPU) and applies rules for a given paradigm
 * - printAllActivities() -> print the summary for time on CP, Blame, etc.
 */

#include <sys/types.h>

#include <time.h>       /* clock_t, clock, CLOCKS_PER_SEC */

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
  
  if ( options.mergeActivities ) 
  {
    // critical path start stream and first critical node time
    criticalPathStart.first = std::numeric_limits< uint64_t >::max( );
    criticalPathStart.second = std::numeric_limits< uint64_t >::max( );
    
    // critical path end stream and last critical node time
    criticalPathEnd.first = std::numeric_limits< uint64_t >::max( );
    criticalPathEnd.second = 0; 
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

  UTILS_MSG( mpiRank == 0, "Reading %s", options.filename.c_str( ) );

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

  if ( timerResolution < 1000000000 ) // 1GHz
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
             "[0] Global collectives found: %u", 
             analysis.getMPIAnalysis().globalCollectiveCounter );

  // close the trace reader
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
  if ( !traceReader )
  {
    throw RTException( "Trace reader is not available!" );
  }
  
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading events ..." );
  
  ////////////////////////////////////////////////////////
  // reading events, until the end of the trace is reached
  
  // minimum number of graph nodes required to start analysis
  
  
  bool events_available = false;
  bool otf2_def_written = false;
  
  uint32_t interval_node_id   = 0;
  uint32_t analysis_intervals = 0;
  uint64_t total_events_read  = 0;
  
  clock_t time_events_read   = 0;
  clock_t time_events_write  = 0;
  clock_t time_analysis_mpi  = 0;
  clock_t time_analysis_omp  = 0;
  clock_t time_analysis_cuda = 0;
  clock_t time_analysis_cp   = 0;
  do
  {
    
    // read events until global collective (with global event reader)
    clock_t time_tmp = clock();
    
    uint64_t events_read = 0;
    events_available = traceReader->readEvents( &events_read );
    total_events_read += events_read;
    
    time_events_read += clock() - time_tmp;
    
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
      time_tmp = clock( );
      runAnalysis( PARADIGM_CUDA, allNodes );
      time_analysis_cuda += clock() - time_tmp;
    }
    
    if ( analysis.haveParadigm( PARADIGM_OMP ) )
    {
      time_tmp = clock( );
      runAnalysis( PARADIGM_OMP, allNodes );
      time_analysis_omp += clock() - time_tmp;
    }

    //\todo: implement check for MPI paradigm
    if ( mpiSize > 1 )
    {
      time_tmp = clock( );
      runAnalysis( PARADIGM_MPI,  allNodes );
      time_analysis_mpi += clock() - time_tmp;
    }
      
    // \todo: to run the CPA we need all nodes on all processes to be analyzed?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    UTILS_MSG( mpiRank == 0 && !options.analysisInterval, 
               "[0] Computing the critical path" );
    
    // check for pending non-blocking MPI!
    analysis.checkPendingMPIRequests( );

    time_tmp = clock( );
    
    // initiate the detection of the critical path
    computeCriticalPath( );
    
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    
    time_analysis_cp += clock() - time_tmp;
    
    // write the first already analyzed part (with local event readers and writers)
    
    time_tmp = clock( );
    
    // write OTF2 definitions for this MPI rank (only once)
    if( !otf2_def_written )
    {
      // write the OTF2 output trace definitions and setup the OTF2 input reader
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

    // writes the OTF2 event streams and computes blame for CPU functions
    if( analysis.writeOTF2EventStreams( options.verbose ) == false && events_available )
    {
      UTILS_MSG( true, "[%u] Reader and writer are not synchronous! Aborting ...", 
                       mpiRank );
      events_available = false;
    }
    
    time_events_write += clock() - time_tmp;
    
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
  
  // find the global critical path length
  if ( options.mergeActivities )
  {
    findGlobalLengthCP( );
  }
  
  if( mpiRank == 0 )
  {
    // Time consumption output for individual analysis steps
    UTILS_MSG( options.verbose >= VERBOSE_TIME, 
               "[0] Trace reading (and graph construction) took %f seconds.", 
               ( (float) time_events_read ) / CLOCKS_PER_SEC );

    UTILS_MSG( options.verbose >= VERBOSE_TIME && mpiSize > 1, 
               "[0] MPI analysis took %f seconds.", 
               ( (float) time_analysis_mpi ) / CLOCKS_PER_SEC );

    UTILS_MSG( options.verbose >= VERBOSE_TIME && analysis.haveParadigm( PARADIGM_OMP ), 
               "[0] OpenMP analysis took %f seconds.", 
               ( (float) time_analysis_omp ) / CLOCKS_PER_SEC );

    UTILS_MSG( options.verbose >= VERBOSE_TIME && analysis.haveParadigm( PARADIGM_CUDA ), 
               "[0] CUDA analysis took %f seconds.", 
               ( (float) time_analysis_cuda ) / CLOCKS_PER_SEC );

    UTILS_MSG( options.verbose >= VERBOSE_TIME, 
               "[0] Critical-path analysis took %f seconds.", 
               ( (float) time_analysis_cp ) / CLOCKS_PER_SEC );
    
    UTILS_MSG( options.verbose >= VERBOSE_TIME, 
               "[0] Trace writing (and CPU blame assignment) took %f seconds.", 
               ( (float) time_events_write ) / CLOCKS_PER_SEC );

    UTILS_MSG( options.analysisInterval,
               "Number of analysis intervals: %u", ++analysis_intervals );
  }
  
  UTILS_MSG( options.verbose >= VERBOSE_BASIC, 
               "[%u] Total number of processed events: %llu", 
               mpiRank, total_events_read );
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
    analysis.getSourceNode( )->getTime( ); // the source node has time "0"
  
  //\todo: use global length of the critical path! Do not use source node, 
  //       but first real node. Do not use local last graph node, but global
  
  /*UTILS_MSG( true, "CP length (%llu - %llu) = %llu", 
             analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL )->getTime( ),
             analysis.getSourceNode( )->getTime( ),
             lengthCritPath );*/

  // compute total process-local blame and process-local CP fraction of activity groups
  for ( IParallelTraceWriter::ActivityGroupMap::iterator groupIter =
          activityGroupMap->begin( ); groupIter != activityGroupMap->end( ); 
        ++groupIter )
  {
    //IParallelTraceWriter::ActivityGroupMap::iterator iter = groupIter;
    //UTILS_MSG( true, "Group ID %u", groupIter->second.functionId);
    
    // set the local CP fraction for this activity type
    groupIter->second.fractionCP =
      (double)( groupIter->second.totalDurationOnCP ) / (double)lengthCritPath;
    
    // add the activity groups blame to the total global blame
    globalBlame += groupIter->second.totalBlame;
    
    //groupIter = ++iter;
  }

  // ************* Phase 2: MPI all-reduce *************** //

  const int MPI_ENTRIES_TAG = 22;
  
  // send/receive groups to master/from other MPI streams
  if ( 0 == mpiRank )
  {
    UTILS_MSG_NOBR( options.verbose >= VERBOSE_BASIC,
                    " Combining results from %d analysis processes", mpiSize );
  
    // receive number of entries
    uint32_t numEntriesSend = 0; // we do not evaluate this for root rank 0
    uint32_t numEntriesRecv[mpiSize];
    MPI_CHECK( MPI_Gather( &numEntriesSend, 1, MPI_UNSIGNED, 
                           numEntriesRecv, 1, MPI_UNSIGNED, 
                           0, MPI_COMM_WORLD ) );
    
    // receive from all other MPI streams
    for ( int rank = 1; rank < mpiSize; ++rank )
    {
      uint32_t numEntries = numEntriesRecv[rank];

      // receive the entries
      if ( numEntries > 0 )
      {
        IParallelTraceWriter::ActivityGroup* buf =
          new IParallelTraceWriter::ActivityGroup[numEntries];
        //\todo: Could be replaced by MPI_Gatherv
        MPI_CHECK( MPI_Recv( buf,
                   numEntries * sizeof( IParallelTraceWriter::ActivityGroup ),
                   MPI_BYTE,
                   rank,
                   MPI_ENTRIES_TAG,
                   MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );

        // combine with own activity groups and generate a global metrics
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

    // for all activity groups: set the global CP fraction
    for ( IParallelTraceWriter::ActivityGroupMap::iterator groupIter =
            activityGroupMap->begin( );
          groupIter != activityGroupMap->end( ); ++groupIter )
    {
      // if we have at least two streams combined
      if( groupIter->second.numUnifyStreams > 1 )
        groupIter->second.fractionCP /= (double)( groupIter->second.numUnifyStreams );
      
      //UTILS_MSG(true, "CP fraction (%s): %lf", 
      //          callbacks.getAnalysis().getFunctionName(groupIter->first), 
      //          groupIter->second.fractionCP);
      
      
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
    uint32_t numEntriesRecv; // is ignored for the sender
    MPI_CHECK( MPI_Gather( &numEntries, 1, MPI_UNSIGNED, 
                           &numEntriesRecv, 1, MPI_UNSIGNED, 
                           0, MPI_COMM_WORLD ) );

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
 * 
 * This function is called for every analysis interval.
 * 
 */
void
Runner::computeCriticalPath( )
{
  // This list will not be sorted! Hence, it does not matter in which order the
  // section list is processed
  EventStream::SortedGraphNodeList criticalNodes;
  
  if ( mpiSize > 1 )
  {
    MPIAnalysis::CriticalSectionsList sectionsList;
    
    // perform MPI reverse replay using blocking edges; create a list of sections
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "Start critical-path detection for MPI %s ...",
               (options.criticalPathSecureMPI) ? "(with slave feedback)" : "" );
    detectCriticalPathMPIP2P( sectionsList, criticalNodes );
    
    // detect the critical path within all sections individually
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "Start critical-path detection for sections ..." );
    if( sectionsList.size() > 0 )
    {
      getCriticalLocalNodes( sectionsList, criticalNodes );
      
      sectionsList.clear();
    }
  }
  else
  {
    // compute local critical path on root, only
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "Single process: defaulting to CUDA/OMP only mode" );

    Graph& subGraph = analysis.getGraph( );

    getCriticalPathIntern( analysis.getSourceNode( ),
                           analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL ),
                           criticalNodes, subGraph );
    
    subGraph.cleanup( true );
  }
  
  if ( criticalNodes.size( ) == 0 )
    return;

  // compute the time-on-critical-path counter
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Set time-on-critical-path for nodes" );
  
  // set all critical path counter to '1' for all critical nodes
  // AND find the timely first and the last critical node
  const uint32_t cpCtrId = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  for ( EventStream::SortedGraphNodeList::const_iterator iter = criticalNodes.begin( );
        iter != criticalNodes.end( ); ++iter )
  {
    GraphNode* currentNode = ( *iter );
    currentNode->setCounter( cpCtrId, 1 );
    
    // to compute the global length of the critical path:
    // get the time and stream ID of the "timely" first and last process-local critical-node
    if ( options.mergeActivities ) // this is the default
    {
      uint64_t currentTime = currentNode->getTime();

      if( currentTime == 0 )
        continue;

      //\todo: clean up this exception! maybe  not needed any more
      if( currentNode->isMPIInit() && currentNode->isLeave() ){
        currentTime = currentNode->getPartner()->getTime();
      }

      if ( currentTime < criticalPathStart.second )
      {
        criticalPathStart.second = currentTime;
        criticalPathStart.first = currentNode->getStreamId( );
      }

      if ( criticalPathEnd.second < currentTime )
      {
        criticalPathEnd.second = currentTime;
        criticalPathEnd.first = currentNode->getStreamId( );
      }
    }
  }
  
  if ( options.mergeActivities && mpiSize > 1 )
  {
    //\todo: check if needed
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  }
}

/**
 * Determine the global length of the critical path by reducing the first and 
 * last local critical node times with all processes.
 */
void
Runner::findGlobalLengthCP( )
{
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Determine total length of critical path" );
  
  uint64_t firstTime = 
    analysis.getStream( criticalPathStart.first )->getPeriod().first;
  uint64_t lastTime  = 
    analysis.getStream( criticalPathEnd.first )->getPeriod().second;

  
  if( 0 == firstTime || 0 == lastTime )
  {
    UTILS_MSG( true,"[%d] Process is not on the critical path?", mpiRank );
    firstTime = 0;
    lastTime  = analysis.getLastGraphNode( )->getTime( );
  }
    
  // get the global first and last timestamps
  if ( mpiSize > 1 )
  {
    MPI_CHECK( MPI_Allreduce( &firstTime,
                   &firstTime,
                   1,
                   MPI_UNSIGNED_LONG_LONG,
                   MPI_MIN,
                   MPI_COMM_WORLD ) );
    MPI_CHECK( MPI_Allreduce( &lastTime,
                   &lastTime,
                   1,
                   MPI_UNSIGNED_LONG_LONG,
                   MPI_MAX,
                   MPI_COMM_WORLD ) );
  }

  // compute the total length of the critical path
  globalLengthCP = lastTime - firstTime;
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Critical path length = %f sec",
             analysis.getRealTime( globalLengthCP ) );
}

/**
 * Compute the critical path between two nodes of the same process
 * (might have multiple event streams).
 *
 * @param start    local non-MPI start node
 * @param end      local non-MPI end node
 * @param cpNodes  list to store nodes of computed cp in
 * @param subGraph subgraph between the two nodes
 */
void
Runner::getCriticalPathIntern( GraphNode*                        start,
                               GraphNode*                        end,
                               EventStream::SortedGraphNodeList& cpNodes,
                               Graph&                            subGraph )
{
  UTILS_MSG( options.printCriticalPath, "\n[%d] Longest path (%s,%s):",
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

    UTILS_MSG( options.printCriticalPath, "[%d] %u: %s (%f)",
               mpiRank, i,
               node->getUniqueName( ).c_str( ),
               analysis.getRealTime( node->getTime( ) ) );

    i++;
  }
}

/**
 * Compute critical path for nodes between two critical MPI nodes on the
 * same process. Do this for the given critical sections and insert nodes on 
 * the critical path. 
 *
 * @param sections list of critical sections for this process
 * @param criticalNodes - list of local critical nodes
 */
void
Runner::getCriticalLocalNodes( MPIAnalysis::CriticalSectionsList& sections,
                               EventStream::SortedGraphNodeList& criticalNodes )
{
  if( sections.size() == 0 )
  {
    return;
  }
  
  Graph& graph = analysis.getGraph();
  // What's the difference to analysis.getGraph()?
  // --> compare the number of nodes
  //Graph*     subGraph   = analysis.getGraph( PARADIGM_ALL ); 
  uint32_t   lastSecCtr = 0;

  #pragma omp parallel for
  for ( uint32_t i = 0; i < sections.size(); ++i )
  {
    MPIAnalysis::CriticalPathSection* section = &( sections[i] );

    uint32_t sectionStartNodeID = section->startNode->getId();
    uint32_t sectionEndNodeID   = section->endNode->getId();

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%d] computing local critical path between MPI nodes [%u, %u] on process %lu",
               mpiRank,
               sectionStartNodeID,
               sectionEndNodeID,
               section->streamID );
    
    GraphNode* startNode = section->startNode;
    GraphNode* endNode = section->endNode;

    if ( !startNode || !endNode )
    {
      throw RTException( "[%d] Did not find local nodes for node IDs %u and %u",
                         mpiRank, sectionStartNodeID, sectionEndNodeID );
    }
    else
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL,
                 "[%d] node mapping: %u = %s (%f), %u = %s (%f)",
                 mpiRank, sectionStartNodeID,
                 startNode->getUniqueName( ).c_str( ),
                 analysis.getRealTime( startNode->getTime( ) ),
                 sectionEndNodeID, endNode->getUniqueName( ).c_str( ),
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

    if ( ( !startLocalNode || !endLocalNode ) ||
         ( startLocalNode->getTime( ) >= endLocalNode->getTime( ) ) )
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL && startLocalNode && endLocalNode,
                 "[%d] No local path between MPI nodes %s (link right %p, %s) "
                 "and %s (link left %p, %s)", mpiRank,
                 startNode->getUniqueName( ).c_str( ),
                 startLocalNode, startLocalNode->getUniqueName( ).c_str( ),
                 endNode->getUniqueName( ).c_str( ),
                 endLocalNode, endLocalNode->getUniqueName( ).c_str( ) );
      
      // add the critical section nodes (start leave, end enter and leave)
      #pragma omp critical
      {
        criticalNodes.push_back( startNode );
        criticalNodes.push_back( endNode );
        
        // for enter end nodes (not atomic!) also add the leave node
        if( endNode->isEnter( ) )
          criticalNodes.push_back( endNode->getPartner( ) );
      }

      continue;
    }

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%d] Computing local critical path for section (%s,%s): (%s,%s)",
               mpiRank,
               startNode->getUniqueName( ).c_str( ), 
               endNode->getUniqueName( ).c_str( ),
               startLocalNode->getUniqueName( ).c_str( ), 
               endLocalNode->getUniqueName( ).c_str( ) );

    EventStream::SortedGraphNodeList sectionLocalNodes;
    sectionLocalNodes.push_back( startNode );

    getCriticalPathIntern( startLocalNode,
                           endLocalNode,
                           sectionLocalNodes,
                           graph/* *subGraph*/ );

    // add the endNode, which is an enter node
    sectionLocalNodes.push_back( endNode );
    
    // for enter end nodes (not atomic!) also add the leave node
    if ( /*endNode->isMPIFinalize( ) &&*/ endNode->isEnter( ) )
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

  //delete subGraph;
}

/**
 * Find the the last MPI node locally and the globally last MPI rank. 
 * The globally last MPI rank has the highest node time. 
 *
 * @param node the locally last MPI node
 * 
 * @return globally last MPI rank
 */
int
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
  
  *node = myLastMpiNode;
  
  if ( lastMpiRank == mpiRank )
  {
    myLastMpiNode->setCounter( analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH ), 1 );

    UTILS_DBG_MSG( DEBUG_CPA_MPI,
               "[%u] critical path reverse replay starts at node %s (%f)",
               mpiRank, myLastMpiNode->getUniqueName( ).c_str( ),
               analysis.getRealTime( myLastMpiNode->getTime( ) ) );

  }
  
  return lastMpiRank;
}

/**
 * Detect the critical path of the MPI sub graph and generate a list of critical
 * sections that are analyzed locally (but OpenMP parallel).
 * This routine implements a master slave concept. The master looks for 
 * sends messages to its partner MPI ranks if its local edge to the previous
 * MPI node is blocked. The slaves wait for such messages and check whether 
 * they will be the new master (have an edge to the last master node).
 *
 * @param sectionsList  critical sections between MPI regions on the critical 
 *                      path will be stored in this list
 * @param criticalNodes critical nodes can be computed instead of actively 
 *                      waiting for the master
 */
void
Runner::detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionsList,
                                  EventStream::SortedGraphNodeList& criticalNodes)
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
  
  bool isMaster   = false;
  int  masterRank = findLastMpiNode( &currentNode );
  if ( mpiRank == masterRank )
  {
    isMaster       = true;
    sectionEndNode = currentNode;
  }

  // mpiGraph is an allocated graph object with a vector of all nodes of the 
  // given paradigm (\TODO this might be extremely memory intensive)
  Graph*   mpiGraph = analysis.getGraph( PARADIGM_MPI );
  uint32_t cpCtrId  = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  
  uint64_t *sendBfr = new uint64_t[BUFFER_SIZE];
  uint64_t *recvBfr = new uint64_t[BUFFER_SIZE];

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

      //\todo: the intermediate begin has to be the start of a section
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
            section.startNode   = currentNode;
            section.endNode     = sectionEndNode;
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
          
          /////////// Communicate with all slaves to find new master ///////////
          
          // prepare send buffer
          sendBfr[0] = commMaster->getId( );
          sendBfr[1] = commMaster->getStreamId( );
          sendBfr[2] = NO_MSG;
            
          // CPA feedback mode: Make sure that not all ranks are slave then!
          if( options.criticalPathSecureMPI )
          {
            sendBfr[BUFFER_SIZE - 1] = 0; // we did not find a master yet
          }            
          
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

            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, MPI_CPA_TAG, MPI_COMM_WORLD ) );
            
            // if no master has been found yet, receive from next slave
            if ( options.criticalPathSecureMPI && sendBfr[BUFFER_SIZE - 1] == 0 )
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
      else // isEnter (master) or isAtomic
      {
        // found the MPI_Init enter or an atomic node (e.g. the intermediate begin)
        if ( currentNode->isMPIInit( ) || currentNode->isAtomic( ) )
        {
          // create critical section for intermediate begin
          if ( currentNode->isAtomic( ) && currentNode->getParadigm() == PARADIGM_MPI &&
               sectionEndNode && ( currentNode != sectionEndNode ) )
          {
            MPIAnalysis::CriticalPathSection section;
            section.streamID    = currentNode->getStreamId( );
            section.startNode   = currentNode;
            section.endNode     = sectionEndNode;
            sectionsList.push_back( section );
          }
          
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
      //////////////////////////////// slave ////////////////////////////////
      UTILS_DBG_MSG( DEBUG_CPA_MPI, "[%u] Slave receives... ", mpiRank);
      
      MPI_Status status;
      //MPI_CHECK( MPI_Recv( recvBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
      //                     MPI_ANY_SOURCE,
      //                     MPI_CPA_TAG, MPI_COMM_WORLD, &status ) );
      
      // use a non-blocking MPI receive to start local CPA meanwhile
      MPI_Request request_recv = MPI_REQUEST_NULL;
      int finished = 0;
      MPI_CHECK( MPI_Irecv( recvBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                           MPI_ANY_SOURCE,
                           MPI_CPA_TAG, MPI_COMM_WORLD, &request_recv ) );
      
      MPI_CHECK( MPI_Test( &request_recv, &finished, &status) );

      if( !finished )
      {
        if( sectionsList.size() > 0 )
        {
          // compute the last local critical section, which is the last element in the vector
          //getCriticalLocalNodes( sectionsList, criticalNodes );
        }
          
        MPI_CHECK( MPI_Wait( &request_recv, &status ) );
      }
      
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
        
        //\todo: MPI_Isend, next block, wait
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
  
  // delete the allocated buffers
  delete[] sendBfr;
  delete[] recvBfr;
  
  // allocated before this loop and not bound to any other object
  delete mpiGraph;

  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
}

/**
 * Apply all paradigm-specific rules to all nodes of the given paradigm.
 *
 * @param paradigm the paradigm (see Node.hpp)
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
        UTILS_MSG( true, "Running analysis: CUDA" );
        break;

      case PARADIGM_MPI:
        UTILS_MSG( true, "Running analysis: MPI" );
        break;

      case PARADIGM_OMP:
        UTILS_MSG( true, "Running analysis: OMP" );
        break;

      default:
        UTILS_MSG( true, "No analysis for unknown paradigm %d", paradigm );
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
  clock_t time_sanity_check = clock();
  
  analysis.runSanityCheck( mpiRank );
  
  UTILS_MSG( options.verbose >= VERBOSE_TIME && mpiRank == 0 && !options.analysisInterval,
             "[0] Sanity check: %f sec", 
             ( (float) ( clock() - time_sanity_check ) ) / CLOCKS_PER_SEC );
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

    if( globalLengthCP == 0 )
    {
      UTILS_MSG( true, "Global critical path length is 0. Skipping output ..." );
      return;
    }
      
    // for all activity groups
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
