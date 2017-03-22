/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2017,
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

#include <time.h>
#include <vector>       /* clock_t, clock, CLOCKS_PER_SEC */

#include "Runner.hpp"
//#include "../analysis/include/mpi/MPIRulesCommon.hpp"

using namespace casita;
using namespace casita::io;

Runner::Runner( int mpiRank, int mpiSize ) :
  mpiRank( mpiRank ),
  mpiSize( mpiSize ),
  analysis( mpiRank, mpiSize ),
  options( Parser::getInstance().getProgramOptions() ),
  callbacks( analysis ), // construct the CallbackHandler
  writer ( NULL ),
  globalLengthCP( 0 )
{
  if ( options.noErrors )
  {
    ErrorUtils::getInstance().setNoExceptions();
  }

  if ( options.verbose )
  {
    UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_BASIC,
               "Enabled verbose output for error utils\n" );
    ErrorUtils::getInstance().setVerbose();
  }
  
  if ( options.mergeActivities ) 
  {
    // critical path start stream and first critical node time
    criticalPathStart.first = UINT64_MAX;
    criticalPathStart.second = UINT64_MAX;
    
    // critical path end stream and last critical node time
    criticalPathEnd.first = UINT64_MAX;
    criticalPathEnd.second = 0;
  }
}

Runner::~Runner()
{
  // close the OTF2 trace writer
  if ( writer != NULL )
  {
    writer->close();
    delete writer;
  }
}

ProgramOptions&
Runner::getOptions()
{
  return options;
}

AnalysisEngine&
Runner::getAnalysis()
{
  return analysis;
}

void
Runner::startAnalysisRun()
{
  OTF2TraceReader* traceReader = NULL;

  UTILS_MSG( mpiRank == 0, "Reading %s", options.filename.c_str() );

  traceReader = new OTF2TraceReader( &callbacks, mpiRank, mpiSize );

  if ( !traceReader )
  {
    throw RTException( "Could not create trace reader" );
  }

  // set the OTF2 callback handlers
  traceReader->handleDefProcess        = CallbackHandler::handleDefProcess;
  traceReader->handleDefFunction       = CallbackHandler::handleDefFunction;
  traceReader->handleDefAttribute      = CallbackHandler::handleDefAttribute;
  traceReader->handleEnter             = CallbackHandler::handleEnter;
  traceReader->handleLeave             = CallbackHandler::handleLeave;
  traceReader->handleProcessMPIMapping = CallbackHandler::handleProcessMPIMapping;
  traceReader->handleMPIComm           = CallbackHandler::handleMPIComm;
  traceReader->handleMPICommGroup      = CallbackHandler::handleMPICommGroup;
  traceReader->handleMPIIsend          = CallbackHandler::handleMPIIsend;
  traceReader->handleMPIIrecv          = CallbackHandler::handleMPIIrecv;
  traceReader->handleMPIIrecvRequest   = CallbackHandler::handleMPIIrecvRequest;
  traceReader->handleMPIIsendComplete  = CallbackHandler::handleMPIIsendComplete;
  traceReader->handleRmaWinDestroy     = CallbackHandler::handleRmaWinDestroy;
  //traceReader->handleRmaPut            = CallbackHandler::handleRmaPut;
  //traceReader->handleRmaGet            = CallbackHandler::handleRmaGet;
  //traceReader->handleRmaOpCompleteBlocking = CallbackHandler::handleRmaOpCompleteBlocking;

  traceReader->open( options.filename, 10 );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading definitions ... " );
  
  // read the OTF2 definitions and initialize some maps and variables of the trace reader
  if( false == traceReader->readDefinitions() )
  {
    throw RTException( "Error while reading definitions!" );
  }
  
  // OTF2 definitions have been checked for existence of CUDA and OpenCL
  // OpenMP is currently checked during event reading and MPI not at all
  if( analysis.haveParadigm( PARADIGM_CUDA ) )
  {
    analysis.addAnalysisParadigm( new cuda::AnalysisParadigmCUDA( &analysis ) );
  }
  else
  {
    // avoid the generation of CUDA graph nodes
    Parser::getInstance().getProgramOptions().ignoreCUDA = true;
  }
  
  if( analysis.haveParadigm( PARADIGM_OCL ) )
  {
    analysis.addAnalysisParadigm( new opencl::AnalysisParadigmOpenCL( &analysis ) );
  }
  
  if( analysis.haveParadigm( PARADIGM_OMP ) )
  {
    analysis.addAnalysisParadigm( new omp::AnalysisParadigmOMP( &analysis ) );
  }
  
  // TODO:
  analysis.getMPIAnalysis().createMPICommunicatorsFromMap();
  
  // set wait state function (requires definitions to be read)
  analysis.setWaitStateRegion();

  // set the timer resolution in the analysis engine
  uint64_t timerResolution = traceReader->getTimerResolution();
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
  traceReader->close();
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
Runner::processTrace( OTF2TraceReader* traceReader )
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
  
  uint64_t interval_node_id   = 0;
  uint32_t analysis_intervals = 0;
  uint64_t total_events_read  = 0;
  uint64_t events_to_read     = 0; // number of events for the trace writer to read
  
  clock_t time_events_read   = 0;
  clock_t time_events_write  = 0;
  clock_t time_events_flush   = 0;
  clock_t time_analysis_mpi  = 0;
  clock_t time_analysis_omp  = 0;
  clock_t time_analysis_cuda = 0;
  clock_t time_analysis_ocl  = 0;
  clock_t time_analysis_cp   = 0;
  do
  {
    // read events until global collective (with global event reader)
    clock_t time_tmp = clock();
    
    uint64_t events_read = 0;
    events_available = traceReader->readEvents( &events_read );

    total_events_read += events_read;
    events_to_read += events_read;
    
    time_events_read += clock() - time_tmp;
    
    //\todo: separate function?
    // for interval analysis
    if( events_available )
    {
      time_tmp = clock();
      
      bool start_analysis = false;

      // get the last node's ID
      uint64_t last_node_id = analysis.getGraph().getNodes().back()->getId();
      uint64_t current_pending_nodes = last_node_id - interval_node_id;
      interval_node_id = last_node_id;
      
      // \todo: this is expensive, because we add for every global collective an additional MPI_Allreduce
      // gather maximum pending nodes on all processes
      uint64_t maxNodes = 0;
      MPI_CHECK( MPI_Allreduce( &current_pending_nodes, &maxNodes, 1, 
                                MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD ) );

      if ( options.analysisInterval < maxNodes )
      {
        start_analysis = true;
      }
      
      time_events_flush += clock() - time_tmp;
      
      // if we didn't find a process with enough work, continue reading
      if( !start_analysis )
      {
        continue;
      }
      
      // increase counter of analysis intervals
      ++analysis_intervals;
    }

    // perform analysis for these events
    
    // create a sorted list of nodes
    EventStream::SortedGraphNodeList allNodes;
    
    // add sorted nodes from all streams to the list
    analysis.getAllNodes( allNodes );
    
    // apply analysis to all nodes of a certain paradigm
    // availability of a certain paradigm is checked when the trace is read
    // analysis creates dependency edges, identifies wait states, distributes blame
    if ( analysis.haveParadigm( PARADIGM_CUDA ) )
    {
      time_tmp = clock();
      runAnalysis( PARADIGM_CUDA, allNodes );
      time_analysis_cuda += clock() - time_tmp;
    }
    
    if ( analysis.haveParadigm( PARADIGM_OCL ) )
    {
      time_tmp = clock();
      runAnalysis( PARADIGM_OCL, allNodes );
      time_analysis_ocl += clock() - time_tmp;
    }
    
    if ( analysis.haveParadigm( PARADIGM_OMP ) )
    {
      time_tmp = clock();
      runAnalysis( PARADIGM_OMP, allNodes );
      time_analysis_omp += clock() - time_tmp;
    }

    //\todo: implement check for MPI paradigm
    if ( mpiSize > 1 )
    {
      time_tmp = clock();
      runAnalysis( PARADIGM_MPI,  allNodes );
      time_analysis_mpi += clock() - time_tmp;
    }
      
    // \todo: to run the CPA we need all nodes on all processes to be analyzed?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    UTILS_MSG( mpiRank == 0 && !options.analysisInterval, 
               "[0] Computing the critical path" );
    
    // check for pending non-blocking MPI!
    analysis.checkPendingMPIRequests();

    time_tmp = clock();

    // initiate the detection of the critical path
    computeCriticalPath( analysis_intervals <= 1, !events_available );

    //\todo: needed?
    if ( analysis_intervals > 1 && events_available )
    {
      MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    }
    
    time_analysis_cp += clock() - time_tmp;
    
    // write the first already analyzed part (with local event readers and writers)
    
    time_tmp = clock();
    
    // write OTF2 definitions for this MPI rank (only once), not thread safe!
    if( !otf2_def_written )
    {
      // write the OTF2 output trace definitions and setup the OTF2 trace writer
      writer = new OTF2ParallelTraceWriter( &analysis );
      
      UTILS_MSG( mpiRank == 0, "[0] Writing result to %s", 
                 Parser::getInstance().getPathToFile().c_str() );

      otf2_def_written = true;
    }

    // writes the OTF2 event streams and computes blame for CPU functions
    if( writer->writeLocations( events_to_read ) != events_to_read )
    {
      UTILS_MSG( true, "[%d] Reader and writer are not synchronous! Aborting ...", 
                       mpiRank );
      events_available = false;
    }
    
    // reset events to read as they have been read by the trace writer
    events_to_read = 0;
    
    time_events_write += clock() - time_tmp;
    
    // clear the sorted graph node list
    allNodes.clear();
    
    // deletes all previous nodes and create an intermediate start point
    if( events_available )
    {
      // create intermediate graph (reset and clear graph objects)
      time_tmp = clock();
      analysis.createIntermediateBegin();
      time_events_flush += clock() - time_tmp;
    }
    
    // necessary?
    MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  } while( events_available );
  
  // write the last device idle leave events
  writer->handleFinalDeviceIdleLeave();
  
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
    
    UTILS_MSG( options.verbose >= VERBOSE_TIME && analysis.haveParadigm( PARADIGM_OCL ), 
               "[0] OpenCL analysis took %f seconds.", 
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
               "Number of analysis intervals: %"PRIu32" (Cleanup nodes took %f seconds)", 
               ++analysis_intervals, ( (float) time_events_flush ) / CLOCKS_PER_SEC );
  }
  
  UTILS_MSG( options.verbose >= VERBOSE_SOME, 
               "[%u] Total number of processed events: %"PRIu64, 
               mpiRank, total_events_read );
  
  // print the total number of processed events over all processes
  if( options.verbose >= VERBOSE_BASIC )
  {
    uint64_t total_events = 0;
    MPI_CHECK( MPI_Reduce( &total_events_read, &total_events, 1, 
                           MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD ) );
    
    UTILS_MSG( mpiRank == 0, "Total number of processed events: %"PRIu64, 
                             total_events );
  }
}

void
Runner::mergeActivityGroups()
{
  OTF2ParallelTraceWriter::ActivityGroupMap* activityGroupMap =
    writer->getActivityGroupMap();
  
  assert( activityGroupMap );

  uint64_t globalBlame    = 0;
  uint64_t lengthCritPath = globalLengthCP;
  //  analysis.getLastNode()->getTime() - analysis.getSourceNode()->getTime();
  
  /*UTILS_MSG( true, "CP length (%llu - %llu) = %llu",
             analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL )->getTime(),
             analysis.getSourceNode()->getTime(),
             lengthCritPath );*/

  // compute total process-local blame and process-local CP fraction of activity groups
  for ( OTF2ParallelTraceWriter::ActivityGroupMap::iterator groupIter =
          activityGroupMap->begin(); groupIter != activityGroupMap->end(); 
        ++groupIter )
  {
    //IParallelTraceWriter::ActivityGroupMap::iterator iter = groupIter;
    //UTILS_MSG( true, "Group ID %u", groupIter->second.functionId);
    
    // set the local CP fraction for this activity type
    groupIter->second.fractionCP =
      (double)( groupIter->second.totalDurationOnCP ) / (double)lengthCritPath;
    
    // add the activity groups blame to the total global blame
    globalBlame += groupIter->second.totalBlame;
  }

  // ************* Phase 2: MPI all-reduce *************** //
  const int MPI_ENTRIES_TAG = 22;
  
  // send/receive groups to master/from other MPI streams
  if ( 0 == mpiRank )
  {
    UTILS_MSG_NOBR( options.verbose >= VERBOSE_BASIC,
                    " Combining regions from %d analysis processes", mpiSize );
  
    // receive number of regions from other processes
    uint32_t numEntriesSend = 0; // we do not evaluate this for root rank 0
    uint32_t numRegionsRecv[ mpiSize ];
    MPI_CHECK( MPI_Gather( &numEntriesSend, 1, MPI_UINT32_T, 
                           numRegionsRecv,  1, MPI_UINT32_T, 
                           0, MPI_COMM_WORLD ) );
    
    // receive from all other MPI streams
    for ( int rank = 1; rank < mpiSize; ++rank )
    {
      uint32_t numRegions = numRegionsRecv[ rank ];

      // receive the regions from current rank
      if ( numRegions > 0 )
      {
        // allocate array to receive region information
        OTF2ParallelTraceWriter::ActivityGroup* buf =
          new OTF2ParallelTraceWriter::ActivityGroup[ numRegions ];
        
        //\todo: Could be replaced by MPI_Gatherv
        MPI_CHECK( MPI_Recv( buf,
                   numRegions * sizeof( OTF2ParallelTraceWriter::ActivityGroup ),
                   MPI_BYTE, rank, MPI_ENTRIES_TAG,
                   MPI_COMM_WORLD, MPI_STATUS_IGNORE ) );

        // combine with own regions and generate a global metrics
        OTF2ParallelTraceWriter::ActivityGroupMap::iterator groupIter;
        for ( uint32_t i = 0; i < numRegions; ++i )
        {
          OTF2ParallelTraceWriter::ActivityGroup* group = &( buf[i] );
          uint32_t fId = group->functionId;
          groupIter = activityGroupMap->find( fId );

          if ( groupIter != activityGroupMap->end() )
          {
            groupIter->second.numInstances      += group->numInstances;
            groupIter->second.totalBlame        += group->totalBlame;
            groupIter->second.totalDuration     += group->totalDuration;
            groupIter->second.totalDurationOnCP += group->totalDurationOnCP;
            groupIter->second.fractionCP        += group->fractionCP;
            groupIter->second.blameOnCP         += group->blameOnCP;
            groupIter->second.numUnifyStreams++;
          }
          else // create region and set region information
          {
            ( *activityGroupMap )[fId].functionId        = fId;
            ( *activityGroupMap )[fId].numInstances      = group->numInstances;
            ( *activityGroupMap )[fId].fractionCP        = group->fractionCP;
            ( *activityGroupMap )[fId].totalBlame        = group->totalBlame;
            ( *activityGroupMap )[fId].totalDuration     = group->totalDuration;
            ( *activityGroupMap )[fId].totalDurationOnCP = group->totalDurationOnCP;
            ( *activityGroupMap )[fId].blameOnCP         = group->blameOnCP;
            ( *activityGroupMap )[fId].numUnifyStreams   = group->numUnifyStreams;
          }

          // add the region's blame to overall blame
          globalBlame += group->totalBlame;
        }

        delete[]buf;
      }
    }

    // for all activity groups: set the global CP fraction
    for ( OTF2ParallelTraceWriter::ActivityGroupMap::iterator groupIter =
            activityGroupMap->begin();
          groupIter != activityGroupMap->end(); ++groupIter )
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
    // number of local regions
    uint32_t numRegions = activityGroupMap->size();

    // send number of (local) activity groups to root rank
    uint32_t numEntriesRecv; // is ignored for the sender
    MPI_CHECK( MPI_Gather( &numRegions, 1, MPI_UINT32_T, 
                           &numEntriesRecv, 1, MPI_UINT32_T, 
                           0, MPI_COMM_WORLD ) );

    // copy region information form map into array
    OTF2ParallelTraceWriter::ActivityGroup* buf =
      new OTF2ParallelTraceWriter::ActivityGroup[ numRegions ];

    uint32_t i = 0;
    for ( OTF2ParallelTraceWriter::ActivityGroupMap::iterator groupIter =
            activityGroupMap->begin();
          groupIter != activityGroupMap->end(); ++groupIter )
    {
      memcpy( &( buf[i] ), &( groupIter->second ),
              sizeof( OTF2ParallelTraceWriter::ActivityGroup ) );
      ++i;
    }

    // send region information of all local regions to root rank
    MPI_CHECK( MPI_Send( buf,
               numRegions * sizeof( OTF2ParallelTraceWriter::ActivityGroup ),
               MPI_BYTE, 0, MPI_ENTRIES_TAG, MPI_COMM_WORLD ) );

    delete[]buf;
  }
}

void
Runner::mergeStatistics()
{
  UTILS_MSG_NOBR( 0 == mpiRank && options.verbose >= VERBOSE_BASIC,
                  " Combining statistics from %d analysis processes", mpiSize );
  
  Statistics& stats = analysis.getStatistics();

  //\todo: MPI_Gatherv might be better?
  uint64_t statsRecvBuf[ mpiSize * STATS_OFFLOADING ];
  MPI_CHECK( MPI_Gather( stats.getStatsOffloading(), STATS_OFFLOADING, MPI_UINT64_T, 
                         statsRecvBuf,  STATS_OFFLOADING, MPI_UINT64_T, 
                         0, MPI_COMM_WORLD ) );

  if( 0 == mpiRank )
  {
    // add stats from all other MPI streams
    for ( int rank = STATS_OFFLOADING; rank < mpiSize * STATS_OFFLOADING; rank += STATS_OFFLOADING )
    {
      stats.addAllStatsOffloading( &statsRecvBuf[ rank ] );
    }
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
Runner::computeCriticalPath( const bool firstInterval, const bool lastInterval )
{
  // if this is the last analysis interval, find the end of the critical path
  // and determine the total length of the critical path
  if( lastInterval )
  {
    findCriticalPathEnd();
    // this function ends in an MPI_Allgather
  }
  
  // This list will not be sorted! Hence, it does not matter in which order the
  // section list is processed
  EventStream::SortedGraphNodeList criticalNodes;

  if ( mpiSize > 1 )
  {
    MPIAnalysis::CriticalSectionsList sectionsList;
    
    // last interval AND local streams contain the globally last event
    // (criticalPathEnd.first is set in findCriticalPathEnd()
    if( lastInterval && criticalPathEnd.first != UINT64_MAX )
    {
      GraphNode *startNode = analysis.getLastGraphNode( PARADIGM_MPI );
      GraphNode *endNode = analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL );

      // if local paradigms node is after last MPI node
      if( startNode && endNode && startNode != endNode &&
          Node::compareLess( startNode, endNode ) )
      {
        UTILS_MSG( options.verbose >= VERBOSE_BASIC, 
                   "[%d] Last critical analysis interval from %s to %s", mpiRank, 
          startNode->getUniqueName().c_str(), endNode->getUniqueName().c_str() );
        
        getCriticalPathIntern( startNode, endNode, criticalNodes );
      }
    }
    
    // perform MPI reverse replay using blocking edges; create a list of sections
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "Start critical-path detection for MPI ..." );
    detectCriticalPathMPIP2P( sectionsList, criticalNodes );
    
    if( sectionsList.size() > 0 )
    {
      // detect the critical path within all sections individually
      UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
                 "Start critical-path detection for %llu sections ...",
                 sectionsList.size() );
      
      getCriticalLocalNodes( sectionsList, criticalNodes );
      
      sectionsList.clear();
    }
  }
  else
  {
    // compute local critical path on root, only
    UTILS_MSG( options.verbose >= VERBOSE_BASIC,
               "Single process: Detect local critical path (CUDA, OpenCL, OpenMP)" );

    getCriticalPathIntern( analysis.getSourceNode(),
                           analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL ),
                           criticalNodes );
    
    // edges are needed for blame distribution in OTF2ParallelTraceWriter::processNextEvent()
    //graph.cleanup( true );
  }
  
  if ( criticalNodes.size() > 0 )
  {
    // compute the time-on-critical-path counter
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Set time-on-critical-path for nodes" );

    // set all critical path counter to '1' for all critical nodes
    // AND find the timely first and the last critical node
    for ( EventStream::SortedGraphNodeList::const_iterator iter = criticalNodes.begin();
          iter != criticalNodes.end(); ++iter )
    {
      GraphNode* currentNode = ( *iter );
      currentNode->setCounter( CRITICAL_PATH, 1 );

      // only the enter nodes are relevant to determine the critical path changes
      if ( currentNode->isLeave() )
        continue;

      // to compute the global length of the critical path:
      // get the time and stream ID of the "timely" first and last process-local critical-node
      if ( options.mergeActivities ) // this is the default
      {
        uint64_t currentTime = currentNode->getTime();

        if( currentTime == 0 )
          continue;

        if ( currentTime < criticalPathStart.second )
        {
          criticalPathStart.second = currentTime;
          criticalPathStart.first = currentNode->getStreamId();
        }

        if ( criticalPathEnd.second < currentTime )
        {
          criticalPathEnd.second = currentTime;
          criticalPathEnd.first = currentNode->getStreamId();
        }
      }
    }
  }
  
  // find process where critical path starts for first interval only
  // precondition: know where the critical path starts
  if( firstInterval )
  {
    findCriticalPathStart();
    // this function ends in an MPI_Allgather
  }
  
  if( lastInterval )
  {
    // compute the total length of the critical path
    globalLengthCP = criticalPathEnd.second - criticalPathStart.second;
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "Critical path length = %f sec",
               analysis.getRealTime( globalLengthCP ) );
  }
}

/**
 * Determine the global length of the critical path by reducing the first and 
 * last local critical node times with all processes.
 *
void
Runner::findGlobalLengthCP()
{
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Determine total length of critical path" );
  
  // set initial values to extrema if process has no critical stream
  uint64_t lastTime  = 0;

  // check for the availability of critical streams, before getting the period
  if( criticalPathEnd.first != UINT64_MAX &&
      analysis.getStream( criticalPathEnd.first ) )
  {
    lastTime  = analysis.getStream( criticalPathEnd.first )->getPeriod().second;
  }
//  else
//  {
//    UTILS_MSG( true, "[%d] No critical last time (stream %llu)", mpiRank,
//                     criticalPathEnd.first );
//  }

  if( 0 == lastTime )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC, 
               "[%d] Process is not on the critical path?", mpiRank );
  }
  
  //UTILS_MSG( true, "[%d] Last time stamp: %llu", mpiRank, lastTime );
    
  // get the global last timestamp
  uint64_t globalLastTime = lastTime;
  if ( mpiSize > 1 )
  {
    MPI_CHECK( MPI_Allreduce( &lastTime, &globalLastTime,
                              1, MPI_UINT64_T,
                              MPI_MAX, MPI_COMM_WORLD ) );
  }

  // compute the total length of the critical path
  globalLengthCP = lastTime - criticalPathStart.second;
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Critical path length = %f sec",
             analysis.getRealTime( globalLengthCP ) );
  
  // local stream has the last event of the global trace
  if( lastTime == globalLastTime )
  {
    analysis.getStream( criticalPathEnd.first )->hasLastGlobalEvent() = true;
  }
}*/

/**
 * Determine the stream where the critical path starts and its first event time.
 * The critical path start time may not be the first event in a trace.
 * This function is executed only once after the critical path analysis of the
 * first analysis interval. 
 */
void
Runner::findCriticalPathStart()
{
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Determine critical path start time" );
  
  // set initial values first critical node time and first event time
  uint64_t firstTime[2] = { UINT64_MAX, UINT64_MAX };

  // check for the availability of critical streams, before getting the period
  if( criticalPathStart.first != UINT64_MAX && 
      analysis.getStream( criticalPathStart.first ) )
  {
    firstTime[0] = criticalPathStart.second;
    firstTime[1] = analysis.getStream( criticalPathStart.first )->getPeriod().first;
  }
//  else
//  {
//    UTILS_MSG( true, "[%d] No critical first (stream %llu) or last time (stream %llu)", mpiRank,
//               criticalPathStart.first, criticalPathEnd.first );
//  }
    
  // get the global first timestamp
  uint64_t globalFirstCriticalTime = firstTime[0];
  uint64_t globalFirstEventTime = firstTime[1];
  uint64_t nodeFirstTimes[mpiSize*2];
  if ( mpiSize > 1 )
  {    
    MPI_CHECK( MPI_Allgather( &firstTime, 2, MPI_UINT64_T,
                              nodeFirstTimes, 2, MPI_UINT64_T, 
                              MPI_COMM_WORLD ) );
    
    for ( int i = 0; i < mpiSize*2; i+=2 )
    {
      if ( nodeFirstTimes[i] < globalFirstCriticalTime )
      {
        globalFirstCriticalTime = nodeFirstTimes[i];
        globalFirstEventTime = nodeFirstTimes[i+1];
      }
    }
  }
  
  //if critical path starts on a local stream,
  if( firstTime[0] == globalFirstCriticalTime )
  {
    analysis.getStream( criticalPathStart.first )->isFirstCritical() = true;
  }
  else
  {
    criticalPathStart.first = UINT64_MAX;
  }
  
  // set the global first critical path time
  criticalPathStart.second = globalFirstEventTime;
}

/**
 * Determine where (stream) and when the critical path ends.
 * The critical path end time is the globally last event in the trace. 
 * This function is executed only once after the critical path analysis of the 
 * last analysis interval.
 */
void
Runner::findCriticalPathEnd()
{
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Determine critical path end" );
  
  // set initial value
  uint64_t localEndTime = 0;
  
  // get the last event time for local streams
  EventStream* lastLeaveStream = NULL;
  analysis.getLastLeaveEvent( &lastLeaveStream, &localEndTime );
    
  // get the global timestamp
  uint64_t globalLastLeaveTime = localEndTime;
  uint64_t globalTimes[ mpiSize ];
  if ( mpiSize > 1 )
  {    
    MPI_CHECK( MPI_Allgather( &localEndTime, 1, MPI_UINT64_T,
                              globalTimes, 1, MPI_UINT64_T, 
                              MPI_COMM_WORLD ) );
    
    for ( int i = 0; i < mpiSize; ++i )
    {
      // get globally last leave event time
      if ( globalTimes[i] > globalLastLeaveTime )
      {
        globalLastLeaveTime = globalTimes[i];
      }
    }
  }
  
  // if critical path ends on a local stream, mark it
  if( localEndTime == globalLastLeaveTime )
  {
    lastLeaveStream->hasLastGlobalEvent() = true;
    criticalPathEnd.first = lastLeaveStream->getId();
  }
  else
  {
    // invalidate stream ID as it is not known
    criticalPathEnd.first = UINT64_MAX;
  }
  
  // set the global last leave event time
  criticalPathEnd.second = globalLastLeaveTime;
  
  // compute the total length of the critical path
  /*globalLengthCP = globalLastLeaveTime - criticalPathStart.second;
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "Critical path length = %f sec",
             analysis.getRealTime( globalLengthCP ) );*/
}

/**
 * Compute the critical path between two nodes of the same process
 * (which might have multiple event streams).
 *
 * @param start    local non-MPI start node
 * @param end      local non-MPI end node
 * @param cpNodes  list to store nodes of computed CP in
 * @param subGraph subgraph between the two nodes
 */
void
Runner::getCriticalPathIntern( GraphNode*                        start,
                               GraphNode*                        end,
                               EventStream::SortedGraphNodeList& cpNodes )
{
  UTILS_ASSERT( start && end, "[%d] Cannot determine internal critical path as"
                              " start or end node is not set!", mpiRank );
  
  UTILS_MSG( options.printCriticalPath, "\n[%d] Longest path (%s,%s):",
             mpiRank,
             start->getUniqueName().c_str(),
             end->getUniqueName().c_str() );

  Graph& subGraph = analysis.getGraph();
  GraphNode::GraphNodeList criticalPath;
  subGraph.getCriticalPath( start, end, criticalPath );

  for ( GraphNode::GraphNodeList::const_iterator cpNode = criticalPath.begin();
        cpNode != criticalPath.end(); ++cpNode )
  {
    GraphNode* node = *cpNode;
    cpNodes.push_back( node );

    UTILS_MSG( options.printCriticalPath, "[%d] %s (%f)",
               mpiRank,
               node->getUniqueName().c_str(),
               analysis.getRealTime( node->getTime() ) );
  }
}

/**
 * Compute critical path for nodes between two critical MPI nodes on the
 * same process. Do this for the given critical sections and insert nodes on 
 * the critical path. 
 * 
 * This function works only for MPI sections (start and end node are MPI nodes)!
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
  
  // What's the difference to analysis.getGraph()?
  // --> compare the number of nodes
  //Graph*     subGraph   = analysis.getGraph( PARADIGM_ALL ); 
  uint32_t lastSecCtr = 0;

  // compute all MPI-local critical sections in parallel
  #pragma omp parallel for
  for ( uint32_t i = 0; i < sections.size(); ++i )
  {
    MPIAnalysis::CriticalPathSection* section = &( sections[i] );
    
    GraphNode* startNode = section->startNode;
    GraphNode* endNode   = section->endNode;

    if ( !startNode || !endNode )
    {
      throw RTException( "[%d] Did not find local nodes!",
                         mpiRank );
    }
    else
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL,
                 "[%d] computing local critical path between MPI nodes [%s, %s]",
                 mpiRank,
                 startNode->getUniqueName().c_str(),
                 endNode->getUniqueName().c_str() );
    }
    
    if ( startNode->isEnter() )
    {
      startNode = startNode->getPartner();
    }

    if ( endNode->isLeave() )
    {
      endNode = endNode->getPartner();
    }

    GraphNode* startLocalNode = startNode->getLinkRight();
    GraphNode* endLocalNode   = endNode->getLinkLeft();

    if ( ( !startLocalNode || !endLocalNode ) ||
         ( startLocalNode->getTime() >= endLocalNode->getTime() ) )
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL && startLocalNode && endLocalNode,
                 "[%d] No local path between MPI nodes %s (link right %p, %s) "
                 "and %s (link left %p, %s)", mpiRank,
                 startNode->getUniqueName().c_str(),
                 startLocalNode, startLocalNode->getUniqueName().c_str(),
                 endNode->getUniqueName().c_str(),
                 endLocalNode, endLocalNode->getUniqueName().c_str() );
      
      // add the critical section nodes (start leave, end enter and leave)
      #pragma omp critical
      {
        criticalNodes.push_back( startNode );
        criticalNodes.push_back( endNode );
        
        // for enter end nodes (not atomic!) also add the leave node
        if( endNode->isEnter() )
        {
          criticalNodes.push_back( endNode->getPartner() );
        }
      }

      continue;
    }

    UTILS_MSG( options.verbose > VERBOSE_ALL,
               "[%d] Computing local critical path for section (%s,%s): (%s,%s)",
               mpiRank,
               startNode->getUniqueName().c_str(), 
               endNode->getUniqueName().c_str(),
               startLocalNode->getUniqueName().c_str(), 
               endLocalNode->getUniqueName().c_str() );

    EventStream::SortedGraphNodeList sectionLocalNodes;
    sectionLocalNodes.push_back( startNode );

    getCriticalPathIntern( startLocalNode, endLocalNode,
                           sectionLocalNodes );

    // add the endNode, which is an enter node
    sectionLocalNodes.push_back( endNode );
    
    // for enter end nodes (not atomic!) also add the leave node
    if ( /*endNode->isMPIFinalize() &&*/ endNode->isEnter() )
    {
      sectionLocalNodes.push_back( endNode->getPartner() );
    }

    #pragma omp critical
    {
      criticalNodes.insert( criticalNodes.end(),
                            sectionLocalNodes.begin(), sectionLocalNodes.end() );
    }

    if ( options.verbose >= VERBOSE_BASIC && !options.analysisInterval &&
        mpiRank == 0 && omp_get_thread_num() == 0 &&
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
Runner::findLastMpiNode( GraphNode** localLastMpiLeave )
{
  uint64_t lastMpiNodeTime = 0;
  int      lastMpiRank     = mpiRank;
  
  // return the last MPI leave node (last MPI node has to be a leave)
  *localLastMpiLeave = analysis.getLastGraphNode( PARADIGM_MPI );
  
  // get the last local MPI enter
  GraphNode* localLastMpiEnter = (*localLastMpiLeave)->getGraphPair().first;
  if ( localLastMpiEnter )
  {
    lastMpiNodeTime = localLastMpiEnter->getTime();
  }
  
  // send the enter time of the last MPI activity
  uint64_t nodeTimes[mpiSize];
  MPI_CHECK( MPI_Allgather( &lastMpiNodeTime, 1, MPI_UINT64_T,
                            nodeTimes, 1, MPI_UINT64_T, MPI_COMM_WORLD ) );

  // compare the enter times of the last MPI activities on each rank
  for ( int i = 0; i < mpiSize; ++i )
  {
    if ( nodeTimes[i] >= lastMpiNodeTime )
    {
      lastMpiNodeTime = nodeTimes[i];
      lastMpiRank     = i;
    }
  }
  
  /* \todo: MPI_Allreduce with MPI_OP = MPI_MAXLOC, we need MPI_UINT64_t_INT
  struct { 
        long val; 
        int  rank; 
  } thisMpiNode, lastMpiNode; 
  thisMpiNode.val = lastMpiNodeTime;
  thisMpiNode.rank = mpiRank;
  MPI_CHECK( MPI_Allreduce( &thisMpiNode, &lastMpiNode, 1, MPI_LONG_INT,
                            MPI_MAXLOC, MPI_COMM_WORLD ) );*/
  
  if ( lastMpiRank == mpiRank )
  {
    // the following should not be necessary
    //(*localLastMpiLeave)->setCounter( CRITICAL_PATH, 1 );

    UTILS_DBG_MSG( DEBUG_CPA_MPI,
                   "[%u] critical path reverse replay starts at node %s (%f)",
                   mpiRank, myLastMpiNode->getUniqueName().c_str(),
                   analysis.getRealTime( myLastMpiNode->getTime() ) );
   
  }
  
  return lastMpiRank;
}

/**
 * Detect the critical path of the MPI sub graph and generate a list of critical
 * sections that are analyzed locally (but OpenMP parallel).
 * This routine implements a master slave concept. The master looks for runs
 * until a blocking local MPI edge. Then it sends messages to all communication
 * partners of the respective MPI activity. The slaves wait for such messages 
 * and check whether they will be the new master (have an edge to the last master node).
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
  
  const int MPI_CPA_TAG = 19;

  const size_t BUFFER_SIZE = 3;
  uint64_t sendBfr[ BUFFER_SIZE ];
  uint64_t recvBfr[ BUFFER_SIZE ];

  // decide on global last MPI node to start with
  GraphNode* currentNode    = NULL;
  GraphNode* lastNode       = NULL;
  GraphNode* sectionEndNode = NULL;
  
  bool isMaster  = false;
  
  // this assumes that the last MPI node is a collective and the collective rule
  // created blocking edges for all MPI activities but the last entering one
  currentNode = analysis.getLastGraphNode( PARADIGM_MPI );
  if( !currentNode->isLeave() )
  {
    UTILS_WARNING( "[%u] Last MPI node should be a leave!", mpiRank );
  }
  else
  {
    Edge* lastMPIEdge = 
      analysis.getEdge( currentNode->getGraphPair().first ,currentNode );
    
    // this is the master (critical path end) if the last MPI edge is not blocking
    if( lastMPIEdge )
    {
      if( !lastMPIEdge->isBlocking() )
      {
        isMaster = true;
        sectionEndNode = currentNode;
      }
    }
    else
    {
      UTILS_WARNING( "[%u] Last MPI edge not found!", mpiRank );
    }
  }
  
/* // backup method if MPI collective rule failed
  int  masterRank = findLastMpiNode( &currentNode );
  if ( mpiRank == masterRank )
  {
    isMaster       = true;
    sectionEndNode = currentNode;
  }*/
  
  // mpiGraph is an allocated graph object with a vector of all nodes of the 
  // given paradigm (\TODO this might be extremely memory intensive)
  Graph* mpiGraph = analysis.getGraph( PARADIGM_MPI );

  while ( true )
  {
    ///////////////////////////// master ////////////////////////////////
    if ( isMaster )
    {
      if( !currentNode )
      {
        UTILS_WARNING( "[%u] CPA master: No current node! Abort CPA.", mpiRank );
        
        // savely abort CPA
        // the first MPI node should be an intermediate start node or MPI_Init 
        // enter and therefore end the critical path analysis
        currentNode = analysis.getFirstTimedGraphNode( PARADIGM_MPI );
        continue;
      }
      
      // verbose output
      if ( lastNode )
      {
        UTILS_MSG( options.verbose >= VERBOSE_ANNOY,
                   "[%u] isMaster, currentNode = %s (%f), lastNode = %s (%f)",
                   mpiRank, currentNode->getUniqueName().c_str(),
                   analysis.getRealTime( currentNode->getTime() ),
                   lastNode->getUniqueName().c_str(),
                   analysis.getRealTime( lastNode->getTime() ) );
      }
      else
      {
        UTILS_MSG( options.verbose >= VERBOSE_ANNOY,
                   "[%u] isMaster, currentNode = %s (%f)",
                   mpiRank, currentNode->getUniqueName().c_str(),
                   analysis.getRealTime( currentNode->getTime() ) );
      }

      UTILS_MSG( lastNode && ( lastNode->getId() <= currentNode->getId() ),
                 "[%u] ! [Warning] current node ID %"PRIu64" (%s) is not strictly "
                 "decreasing; last node ID %"PRIu64" (%s)", 
                 mpiRank, currentNode->getId(), currentNode->getUniqueName().c_str(),
                 lastNode->getId(), lastNode->getUniqueName().c_str() );
      
      //\todo: the intermediate begin has to be the start of a section
      if ( currentNode->isLeave() ) // isLeave
      {
        Edge* activityEdge = 
          analysis.getEdge( currentNode->getGraphPair().first, currentNode );
        
        if( !activityEdge )
        {
          UTILS_WARNING( "[%u] CPA master: No activity edge found for %s. Abort CPA.", 
                         mpiRank, currentNode->getUniqueName().c_str() );
          
          // savely abort CPA
          // the first MPI node should be an intermediate start node or MPI_Init 
          // enter and therefore end the critical path analysis
          currentNode = analysis.getFirstTimedGraphNode( PARADIGM_MPI );
          continue;
        }

        // CP changes stream on blocking edges
        if ( activityEdge->isBlocking() )
        {
          // therefore, create section for local processing later
          if ( sectionEndNode && ( currentNode != sectionEndNode ) )
          {
            MPIAnalysis::CriticalPathSection section;
            section.startNode = currentNode;
            section.endNode   = sectionEndNode;

            UTILS_DBG_MSG( DEBUG_CPA_MPI, "[%d] Push critical section [%s,%s]", 
                           mpiRank, currentNode->getUniqueName().c_str(), 
                           sectionEndNode->getUniqueName().c_str());
            sectionsList.push_back( section );

            // the leave node is still on the critical path
            currentNode->setCounter( CRITICAL_PATH, 1 );
          }

          ///////////////// Send a message to the new master ///////////////////
          bool nodeHasRemoteInfo = false;
          
          // check if the given node has a remote edge (currently just check 
          // whether the edge is available and ignore return value)
          MPIAnalysis::RemoteNode pnPair = // (stream ID, node ID)
            analysis.getMPIAnalysis().getRemoteNodeInfo( currentNode,
                                                          &nodeHasRemoteInfo );
          
          // check enter event
          if( !nodeHasRemoteInfo )
          {
            pnPair = // (stream ID, node ID)
            analysis.getMPIAnalysis().getRemoteNodeInfo( 
              currentNode->getGraphPair().first, &nodeHasRemoteInfo );
            
            // if still no remote node found, continue as master
            if( !nodeHasRemoteInfo )
            {
              currentNode->setCounter( CRITICAL_PATH, 1 );
              lastNode    = currentNode;
              currentNode = activityEdge->getStartNode();
              continue;
            }
          }
          
          UTILS_DBG_MSG( DEBUG_CPA_MPI,
                         "[%u]  found wait state for %s (%f), changing stream at %s",
                         mpiRank, currentNode->getUniqueName().c_str(),
                         analysis.getRealTime( currentNode->getTime() ),
                         currentNode->getUniqueName().c_str() );

          uint32_t mpiPartnerRank = 
            analysis.getMPIAnalysis().getMPIRank( pnPair.streamID );
                 
          // prepare send buffer
          sendBfr[0] = pnPair.nodeID;
          sendBfr[1] = NO_MSG; // no specific message
          
          // transfer blame for MPI_Wait[all]
          if( currentNode->isMPIWait() || currentNode->isMPIWaitall() )
          {
            sendBfr[2] = currentNode->getCounter( WAITING_TIME, NULL );
          }
          else
          {
            sendBfr[2] = 0;
          }

          UTILS_DBG_MSG( DEBUG_CPA_MPI,
                         "[%u]  testing remote MPI worker %u for remote edge to"
                         " my node %"PRIu64" on stream %"PRIu64,
                         mpiRank, mpiPartnerRank, sendBfr[0], sendBfr[1] );

          // send a message to the 
          MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UINT64_T,
                               mpiPartnerRank, MPI_CPA_TAG, MPI_COMM_WORLD ) );

          // continue main loop as slave
          isMaster = false;
          sectionEndNode = NULL;
          
          // remove remote node entry
          analysis.getMPIAnalysis().removeRemoteNode( currentNode );
        }
        else // non-blocking edge
        {
          currentNode->setCounter( CRITICAL_PATH, 1 );
          lastNode    = currentNode;
          currentNode = activityEdge->getStartNode();
          // continue main loop as master
        }
      } /* END: currentNode->isLeave() */
      else // isEnter (master) or isAtomic
      {
        // found the MPI_Init enter or an atomic node (e.g. the intermediate begin)
        if ( currentNode->isMPIInit() || currentNode->isAtomic() )
        {
          // create critical section for intermediate begin
          if ( sectionEndNode && ( currentNode != sectionEndNode ) )
          {
            MPIAnalysis::CriticalPathSection section;
            section.startNode = currentNode;
            section.endNode   = sectionEndNode;

            UTILS_DBG_MSG( DEBUG_CPA_MPI,  
                           "[%d] Push critical section [%s,%s] (MPI_Init/atomic)", 
                           mpiRank, currentNode->getUniqueName().c_str(), 
                           sectionEndNode->getUniqueName().c_str());
            sectionsList.push_back( section );
          }
          else if ( currentNode->isMPIInit() )
          {
            // add MPI_Init enter to critical nodes, as it is on the CP and needs 
            // to be considered for global times
            criticalNodes.push_back( currentNode );
          }
          
          currentNode->setCounter( CRITICAL_PATH, 1 );

          // notify all slaves that we are done
          UTILS_MSG( options.verbose >= VERBOSE_BASIC && !options.analysisInterval, 
                     "[%u] Critical path reached global collective %s. "
                     "Asking all slaves to terminate", mpiRank, 
                     currentNode->getUniqueName().c_str() );

          // send "path found" message to all ranks
          sendBfr[0] = 0;
          sendBfr[1] = PATH_FOUND_MSG;

          // get all MPI ranks
          std::set< uint64_t > mpiPartners =
            analysis.getMPIAnalysis().getMPICommGroup( 0 ).procs;
          
          for ( std::set< uint64_t >::const_iterator iter = mpiPartners.begin();
                iter != mpiPartners.end(); ++iter )
          {
            int commMpiRank = analysis.getMPIAnalysis().getMPIRank( *iter );
            
            // ignore own rank
            if ( commMpiRank == mpiRank )
            {
              continue;
            }

            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UINT64_T,
                                 commMpiRank, MPI_CPA_TAG, MPI_COMM_WORLD ) );
          }

          // leave main loop
          break;
        }

        // master: find previous MPI node on the same stream
        bool foundPredecessor = false;
        
        const Graph::EdgeList& inEdges = mpiGraph->getInEdges( currentNode );
        for ( Graph::EdgeList::const_iterator iter = inEdges.begin();
              iter != inEdges.end(); ++iter )
        {
          Edge* intraEdge = *iter;
          if ( intraEdge->isIntraStreamEdge() )
          {
            currentNode->setCounter( CRITICAL_PATH, 1 );
            
            lastNode         = currentNode;
            currentNode      = intraEdge->getStartNode();
            foundPredecessor = true;
            
            // continue main loop as master 
            break;
          }
        }

        if ( !foundPredecessor )
        {
          throw RTException( "[%u] No ingoing intra-stream edge for node %s",
                             currentNode->getUniqueName().c_str() );
        }
      } // END: isEnter (master)
    } ///////////////////////////// END: master ////////////////////////////////
    else
    {
      //////////////////////////////// slave ////////////////////////////////
      UTILS_DBG_MSG( DEBUG_CPA_MPI, "[%u] Slave receives... ", mpiRank);

      // use a non-blocking MPI receive to start local CPA meanwhile
      MPI_Request request_recv = MPI_REQUEST_NULL;
      int finished = 0;
      MPI_CHECK( MPI_Irecv( recvBfr, BUFFER_SIZE, MPI_UINT64_T, MPI_ANY_SOURCE,
                            MPI_CPA_TAG, MPI_COMM_WORLD, &request_recv ) );
      
      MPI_Status status;
      MPI_CHECK( MPI_Test( &request_recv, &finished, &status ) );

      if( !finished )
      {
        if( sectionsList.size() > 0 )
        {
          //\todo: compute the last local critical section, which is the last element in the vector
          //getCriticalLocalNodes( sectionsList, criticalNodes );
        }
          
        MPI_CHECK( MPI_Wait( &request_recv, &status ) );
      }
      
      // if the master send "found message" we can leave the while loop
      if ( recvBfr[1] == PATH_FOUND_MSG )
      {
        UTILS_MSG( options.verbose >= VERBOSE_ALL,
                   "[%u] * terminate requested by master", mpiRank );
        break;
      }
      
      // ID of the next critical path node (send from remote rank)
      uint64_t nextNodeID = recvBfr[0];
   
      // find local node for remote node id and decide if we can continue here
      UTILS_DBG_MSG( DEBUG_CPA_MPI,
                     "[%u]  tested by remote MPI worker %u for node %"PRIu64,
                     mpiRank, status.MPI_SOURCE,
                     nextNodeID ); // continuation node ID

      // binary search for continuation node
      GraphNode* slaveLeaveNode = 
        GraphNode::findNode( nextNodeID, mpiGraph->getNodes() );

      //////// if the node could not be found, do a sequential search //////////
      if( !slaveLeaveNode || slaveLeaveNode->getId() != nextNodeID )
      {
        // reset slaveNode
        slaveLeaveNode = NULL;

        // sequential search 
        for( Graph::NodeList::const_iterator iter = mpiGraph->getNodes().begin();
             iter != mpiGraph->getNodes().end(); ++iter )
        {
          if( (*iter)->getId() == nextNodeID )
          {
            slaveLeaveNode = ( *iter );
            break;
          }
        }

        if( !slaveLeaveNode )
        {
          if( nextNodeID < mpiGraph->getNodes().front()->getId() )
          {
            UTILS_MSG( true, "[%u] Node ID %"PRIu64" is out of range "
                       "[%"PRIu64",%"PRIu64"]! Send from %d",
                       mpiRank, nextNodeID, mpiGraph->getNodes().front()->getId(), 
                       mpiGraph->getNodes().back()->getId(), status.MPI_SOURCE );
          }
          else
          {
            UTILS_MSG( true, "[%u] Sequential search for node ID %"PRIu64" failed!",
                       mpiRank, nextNodeID );
          }
          //slaveNode = mpiGraph->getNodes().front();
        }
      }
      //////////////////////////////////////////////////////////////////////////
      
      // set slave leave and enter node
      slaveLeaveNode = slaveLeaveNode->getGraphPair().second;
      GraphNode* slaveEnterNode = slaveLeaveNode->getGraphPair().first;
      
      // \todo:
      /*if( recvBfr[2] > 0 )
      {
        distributeBlame( &analysis,
                       slaveEnterNode,
                       0,
                       mpi::streamWalkCallback );
      }*/

      //this rank is the new master
      isMaster = true;
      slaveLeaveNode->setCounter( CRITICAL_PATH, 1 );

      lastNode       = slaveLeaveNode;
      currentNode    = slaveEnterNode;
      sectionEndNode = lastNode;

      UTILS_DBG_MSG( DEBUG_CPA_MPI,
                     "[%u] becomes new master at node %s, lastNode = %s\n",
                     mpiRank,
                     currentNode->getUniqueName().c_str(),
                     lastNode->getUniqueName().c_str() );

      // continue main loop as master
    }
  }
  
  // allocated before this loop and not bound to any other object
  delete mpiGraph;

  // make sure that every process is leaving
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
        
      case PARADIGM_OCL:
        UTILS_MSG( true, "Running analysis: OpenCL" );
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
  size_t num_nodes = allNodes.size();

  // apply paradigm specific rules
  for ( EventStream::SortedGraphNodeList::const_iterator nIter = allNodes.begin();
        nIter != allNodes.end(); ++nIter )
  {
    GraphNode* node = *nIter;
    ctr++;

    // ignore non-paradigm rules
    if ( !( node->getParadigm() & paradigm ) )
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
  
  // apply rules on pending nodes
  //analysis.processDeferredNodes( paradigm );

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
Runner::printAllActivities()
{
  OTF2ParallelTraceWriter::ActivityGroupMap* activityGroupMap =
    writer->getActivityGroupMap();

  if ( mpiRank == 0 )
  {
    printf( "\n%50s %10s %11s %11s %6s %8s %8s %10s\n",
            "Activity Group",
            "Calls",
            "Time[s]",
            "Time on CP",
            "CP[%]",
            "Blame[%]",
            "Blame+CP",
            "Blame on CP" );
    //printf( "-------------------------------------------------- ---------- ----"
    //        "------- ----------- ------- ----------- -------- ------\n" );
    printf( "--------------------------------------------------\n" );
    
    std::set< OTF2ParallelTraceWriter::ActivityGroup,
              OTF2ParallelTraceWriter::ActivityGroupCompare > sortedActivityGroups;

    if( globalLengthCP == 0 )
    {
      UTILS_MSG( true, "Global critical path length is 0. Skipping output ..." );
      return;
    }
      
    // for all activity groups
    for ( OTF2ParallelTraceWriter::ActivityGroupMap::iterator iter =
            activityGroupMap->begin();
          iter != activityGroupMap->end(); ++iter )
    {
      iter->second.fractionCP = 0.0;
      if ( iter->second.totalDurationOnCP > 0 )
      {
        iter->second.fractionCP =
          (double)iter->second.totalDurationOnCP / (double)globalLengthCP;
      }
      
      sortedActivityGroups.insert( iter->second );
    }
      
    // print summary in CSV file (file has to be declared in this scope for closing it)
    FILE *summaryFile;
    if (options.createRatingCSV){
      
      UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
                 "[0] generate rating as csv file" );

      std::string Filename = Parser::getInstance().getPathToFile() 
                           + std::string( "/" ) + Parser::getInstance().getOutArchiveName()
                           + std::string( "_rating.csv" );
      
      summaryFile = fopen(Filename.c_str(),"w");
      fprintf(summaryFile, "Activity Group;Calls;Time [s];Time on CP [s];"
                           "CP Ratio [%%];Global Blame Ratio [%%];"
                           "Blame+CP;Blame on CP [s]\n" );
    }
    
    uint32_t sumInstances   = 0;
    uint64_t sumDuration    = 0;
    uint64_t sumDurationCP  = 0;
    uint64_t sumBlameOnCP   = 0;  
    double sumFractionCP    = 0.0;
    double sumFractionBlame = 0.0;
    
    uint64_t sumWaitingTime = 0;
    
    size_t ctr           = 0;
    for ( std::set< OTF2ParallelTraceWriter::ActivityGroup,
                    OTF2ParallelTraceWriter::ActivityGroupCompare >::
          const_iterator iter = sortedActivityGroups.begin();
          iter != sortedActivityGroups.end(); ++iter )
    {
      if( ctr < options.topX )
      {
        // generate a sum of the TOP rated functions
        sumInstances     += iter->numInstances;
        sumDuration      += iter->totalDuration;
        sumDurationCP    += iter->totalDurationOnCP;
        sumBlameOnCP     += iter->blameOnCP;
        sumFractionCP    += iter->fractionCP;
        sumFractionBlame += iter->fractionBlame;
        
        sumWaitingTime   += iter->waitingTime;
      
        printf( "%50.50s %10u %11f %11f %6.2f %8.4f %6.6f %11.6f\n",
                analysis.getFunctionName( iter->functionId ),
                iter->numInstances,
                analysis.getRealTime( iter->totalDuration ),
                analysis.getRealTime( iter->totalDurationOnCP ),
                100.0 * iter->fractionCP,
                100.0 * iter->fractionBlame,
                iter->fractionCP + iter->fractionBlame,
                analysis.getRealTime( iter->blameOnCP ) );
        
        ++ctr;
      }
      
      if( options.createRatingCSV )
      {
        fprintf( summaryFile, "%s;%u;%lf;%lf;%lf;%lf;%lf;%lf\n",
                 analysis.getFunctionName( iter->functionId ),
                 iter->numInstances,
                 analysis.getRealTime( iter->totalDuration ),
                 analysis.getRealTime( iter->totalDurationOnCP ),
                 100.0 * iter->fractionCP,
                 100.0 * iter->fractionBlame,
                 iter->fractionCP + iter->fractionBlame,
                 analysis.getRealTime( iter->blameOnCP ) );
      }
    }
    
    printf( "--------------------------------------------------\n"
            "%48.48s%2lu %10u %11lf %11lf %6.2lf %8.4lf %20.6lf\n",
              "Sum of Top ",
              ctr,
              sumInstances,
              analysis.getRealTime( sumDuration ),
              analysis.getRealTime( sumDurationCP ),
              100.0 * sumFractionCP,
              100.0 * sumFractionBlame, 
              analysis.getRealTime( sumBlameOnCP ) );
    
    // print statistics
    Statistics& stats = analysis.getStatistics();
    printf( "\nPattern summary:\n"
            " CUDA\n"
            "  Early blocking wait for kernel: %"PRIu64" (%lf sec)\n"
            "  Total attributed waiting time: %lf sec\n"
            "  Early test for completion: %"PRIu64" (%lf s)\n"
            "  Idle device: %lf sec\n"
            "  Compute idle device: %lf sec\n"
            "  Blocking communication: %"PRIu64" (%lf sec)\n"
            "  Consecutive communication: %"PRIu64" (%lf sec)\n\n",
            stats.getStatsOffloading()[OFLD_STAT_EARLY_BLOCKING_WAIT],
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_EARLY_BLOCKING_WTIME] ),
            analysis.getRealTime( sumWaitingTime ),
            stats.getStatsOffloading()[OFLD_STAT_EARLY_TEST],
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_EARLY_TEST_TIME] ),
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_IDLE_TIME] ),
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_COMPUTE_IDLE_TIME]),
            stats.getStatsOffloading()[OFLD_STAT_BLOCKING_COM],
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_BLOCKING_COM_TIME] ),
            stats.getStatsOffloading()[OFLD_STAT_MULTIPLE_COM],
            analysis.getRealTime( stats.getStatsOffloading()[OFLD_STAT_MULTIPLE_COM_TIME] )
           );
    
    if (options.createRatingCSV)
    {
      fclose(summaryFile);
    }
  }
}
