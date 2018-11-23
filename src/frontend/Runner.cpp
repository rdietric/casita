/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
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
#include <iomanip>

#include "otf/OTF2DefinitionHandler.hpp"

#include "Runner.hpp"

#include <fstream>
#include <iostream>

#if defined(SCOREP_USER_ENABLE)
#include "scorep/SCOREP_User.h"
#endif

using namespace casita;
using namespace casita::io;

Runner::Runner( int mpiRank, int mpiSize ) :
  mpiRank( mpiRank ),
  mpiSize( mpiSize ),
  options( Parser::getInstance().getProgramOptions() ),
  analysis( mpiRank, mpiSize ),
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
  
  totalEventsRead = 0;
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

void
Runner::prepareAnalysis()
{
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION_DEFINE( prepare_handle )
  SCOREP_USER_REGION_BEGIN( prepare_handle, "prepareAnalysis",
                            SCOREP_USER_REGION_TYPE_PHASE )
#endif
    
  //OTF2DefinitionHandler* defHandler = new OTF2DefinitionHandler();
  callbacks.setDefinitionHandler( &definitions );

  UTILS_MSG( mpiRank == 0, "Reading %s", options.filename.c_str() );

  OTF2TraceReader* traceReader = 
    new OTF2TraceReader( &callbacks, &definitions, mpiRank, mpiSize );

  if ( !traceReader )
  {
    throw RTException( "Could not create trace reader" );
  }

  // set the OTF2 callback handlers
  traceReader->handleDefProcess        = CallbackHandler::handleDefProcess;
  traceReader->handleLocationProperty  = CallbackHandler::handleLocationProperty;
  traceReader->handleDefFunction       = CallbackHandler::handleDefRegion;
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
  traceReader->handleThreadFork       = CallbackHandler::handleThreadFork;

  traceReader->open( options.filename, 10 );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading definitions ... " );
  
  // read the OTF2 definitions and initialize some maps and variables of the trace reader
  if( false == traceReader->readDefinitions() )
  {
    throw RTException( "Error while reading definitions!" );
  }

  // OTF2 definitions have been checked for existence of CUDA. OpenCL, and OpenMP
  // MPI currently is not checked and assumed to be available
  
  if( ( analysis.haveParadigm( PARADIGM_CUDA ) || 
        analysis.haveParadigm( PARADIGM_OCL ) ) && 
        Parser::ignoreOffload() == false )
  {
    analysis.addAnalysis( new offload::AnalysisParadigmOffload( &analysis ) );
  }
  else
  {
    // avoid the generation of CUDA/OpenCL graph nodes
    Parser::getInstance().getProgramOptions().ignoreOffload = true;
  }
  
  if( analysis.haveParadigm( PARADIGM_OMP ) )
  {
    analysis.addAnalysis( new omp::AnalysisParadigmOMP( &analysis ) );
  }
  
  // create MPI communicators according to the OTF2 trace definitions
  analysis.getMPIAnalysis().createMPICommunicatorsFromMap();

  // set the timer resolution in the analysis engine
  uint64_t timerResolution = definitions.getTimerResolution();
  analysis.setTimerResolution( timerResolution );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Timer resolution = %llu",
             (long long unsigned)( timerResolution ) );

  if ( timerResolution < 1000000000 ) // 1GHz
  {
    UTILS_MSG( mpiRank == 0, 
               "Warning: your timer resolution is very low (< 1 GHz)!" );
  }

  // setup reading events
  traceReader->setupEventReader( options.ignoreAsyncMpi );
  
  // initialize the OTF2 trace writer
  writer = new OTF2ParallelTraceWriter( &analysis, &definitions );
  
  #if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION_END( prepare_handle )
  #endif
  
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
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION( "processTrace", SCOREP_USER_REGION_TYPE_FUNCTION )
#endif
    
  if ( !traceReader )
  {
    throw RTException( "Trace reader is not available!" );
  }
  
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[0] Reading events ..." );
  
  ////////////////////////////////////////////////////////
  // reading events, until the end of the trace is reached
  
  // get OpenMP analysis, if the paradigm is used in the trace file
  omp::AnalysisParadigmOMP* ompAnalysis = NULL;
  if( analysis.haveParadigm( PARADIGM_OMP ) )
  {
    ompAnalysis = 
      ( omp::AnalysisParadigmOMP* )analysis.getAnalysis( PARADIGM_OMP );
  }
  
  // get offload analysis, if the paradigm is used in the trace file
  offload::AnalysisParadigmOffload* ofldAnalysis = NULL;
  if( ( analysis.haveParadigm( PARADIGM_OCL ) || 
        analysis.haveParadigm( PARADIGM_CUDA ) ) && 
        Parser::ignoreOffload() == false )
  {
    ofldAnalysis = 
      ( offload::AnalysisParadigmOffload* )analysis.getAnalysis( PARADIGM_OFFLOAD );
  }
  
  bool events_available = false;
  bool otf2_def_written = false;
  
  // minimum number of graph nodes required to start analysis
  uint64_t interval_node_id   = 0;
  uint32_t analysis_intervals = 0;
  uint64_t events_to_read     = 0; // number of events for the trace writer to read
  
  clock_t time_start         = clock();
  clock_t time_events_read   = 0;
  clock_t time_events_write  = 0;
  clock_t time_events_flush  = 0;
  clock_t time_analysis      = 0;
  clock_t time_analysis_cp   = 0;
  do
  {
#if defined(SCOREP_USER_ENABLE)
    SCOREP_USER_REGION_DEFINE( read_handle )
    SCOREP_USER_REGION_BEGIN( read_handle, "processTrace::read",
                              SCOREP_USER_REGION_TYPE_PHASE )
#endif
    
    // read events until global collective (with global event reader)
    clock_t time_tmp = clock();
    
    uint64_t events_read = 0;
    events_available = traceReader->readEvents( &events_read );

    totalEventsRead += events_read;
    events_to_read += events_read;
    
    time_events_read += clock() - time_tmp;
    
    //\todo: separate function?
    // for interval analysis
    // invokes global blocking collective
    if( events_available )
    {
      time_tmp = clock();
      
      bool start_analysis = false;
      
      uint64_t last_node_id = 0;
      uint64_t current_pending_nodes = 0;
        
      // if the global collective is within an OpenMP region or an offload 
      // kernel is pending we cannot start an intermediate analysis
      if( ( ompAnalysis && ompAnalysis->getNestingLevel() > 0 ) ||
          ( ofldAnalysis && ofldAnalysis->getPendingKernelCount() > 0 ) )
      {
        //UTILS_OUT( "Found pending local region" );
        // invalidate pending nodes
        current_pending_nodes = UINT64_MAX;
      }
      else
      {
        // get the last node's ID
        last_node_id = analysis.getGraph().getNodes().back()->getId();
        
        current_pending_nodes = last_node_id - interval_node_id;        
      }
      
      // \todo: this is expensive, because we add for every global collective an
      // additional MPI_Allreduce to gather the maximum pending nodes on all processes
      uint64_t maxNodes = 0;
      MPI_CHECK( MPI_Allreduce( &current_pending_nodes, &maxNodes, 1, 
                                MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD ) );

      if ( options.analysisInterval < maxNodes && UINT64_MAX != maxNodes )
      {
        start_analysis = true;
        
        // set a new interval begin node ID
        interval_node_id = last_node_id;
      }
      
      time_events_flush += clock() - time_tmp;
      
      // if we didn't find a process with enough work, continue reading
      if( !start_analysis )
      {
#if defined(SCOREP_USER_ENABLE)
        SCOREP_USER_REGION_END( read_handle )
#endif
        continue;
      }
      
      // increase counter of analysis intervals
      ++analysis_intervals;
    }
    
#if defined(SCOREP_USER_ENABLE)
    SCOREP_USER_REGION_END( read_handle )
#endif

    // perform analysis for these events
    time_tmp = clock();
    //runAnalysis( allNodes );
    analysis.runAnalysis();
    time_analysis += clock() - time_tmp;
      
    // \todo: to run the CPA we need all nodes on all processes to be analyzed?
    //MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );

    UTILS_MSG( mpiRank == 0 && !options.analysisInterval, 
               "[0] Computing the critical path" );
    
    // check for pending non-blocking MPI!
    if( Parser::getVerboseLevel() >= VERBOSE_BASIC )
    {
      analysis.checkPendingMPIRequests();
    }
    
    time_tmp = clock();

    // initiate the detection of the critical path
    computeCriticalPath( analysis_intervals <= 1, !events_available );

    //\todo: needed?
    /*if ( analysis_intervals > 1 && events_available )
    {
      MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
    }*/
    
    time_analysis_cp += clock() - time_tmp;
    
    // write the first already analyzed part (with local event readers and writers)
    
    time_tmp = clock();
    
    // write OTF2 definitions for this MPI rank (only once), not thread safe!
    if( !otf2_def_written )
    {
      UTILS_MSG( mpiRank == 0, "Writing result to %s", 
                 Parser::getInstance().getPathToFile().c_str() );

      otf2_def_written = true;
    }

    // writes the OTF2 event streams and computes blame for CPU functions
    if( writer->writeLocations( events_to_read ) != events_to_read )
    {
      UTILS_OUT( "[%d] Reader and writer are not synchronous! Aborting ...", 
                 mpiRank );
      events_available = false;
    }
    
    // reset events to read as they have been read by the trace writer
    events_to_read = 0;
    
    time_events_write += clock() - time_tmp;
    
    // deletes all previous nodes and create an intermediate start point
    if( events_available )
    {
      // create intermediate graph (reset and clear graph objects)
      time_tmp = clock();
      //writer->clearOpenEdges(); // debugging
      analysis.createIntermediateBegin();
      time_events_flush += clock() - time_tmp;
    }
    
    //MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
  } while( events_available );
  
  // write the last device idle leave events
  writer->handleFinalDeviceIdleLeave();
  
  if( mpiRank == 0 && options.verbose >= VERBOSE_TIME )
  {
    // open a file in write mode.
    ofstream summaryFile;
    std::string sFileName = Parser::getInstance().getSummaryFileName();
    
    summaryFile.open( sFileName.c_str() );
   
    summaryFile.precision(6);
    summaryFile << std::fixed;
    
    summaryFile << std::setw(44) << std::left << "- Total analysis time: " 
                << std::right << std::setw(12)
                << ( (float) clock() - time_start ) / CLOCKS_PER_SEC 
                << std::setw(4) << " sec" << std::endl;
   
    // Time consumption output for individual analysis steps
    summaryFile << std::setw(44) << std::left 
                << "  Trace reading (and graph construction): " << std::right
                << std::setw(12)
                << ( (float) time_events_read ) / CLOCKS_PER_SEC
                << std::setw(4) << " sec" << std::endl;

    summaryFile << std::setw(44) << std::left 
                << "  Applying analysis rules: " << std::right
                << std::setw(12)
                << ( (float) time_analysis ) / CLOCKS_PER_SEC
                << std::setw(4) << " sec" << std::endl;
    
    summaryFile << std::setw(44) << std::left 
                << "  Critical-path analysis: " << std::right
                << std::setw(12)
                << ( (float) time_analysis_cp ) / CLOCKS_PER_SEC
                << std::setw(4) << " sec" << std::endl;
    
    summaryFile << std::setw(44) << std::left 
                << "  Trace writing (and blame assignment): " << std::right
                << std::setw(12)
                << ( (float) time_events_write ) / CLOCKS_PER_SEC
                << std::setw(4) << " sec" << std::endl;
    
    summaryFile.close();
    
    UTILS_MSG( options.analysisInterval, "- Number of analysis intervals: %" PRIu32
               " (Cleanup nodes took %f seconds)", ++analysis_intervals, 
               ( (float) time_events_flush ) / CLOCKS_PER_SEC );
  }
  
  analysis.getStatistics().setActivityCount( STAT_TOTAL_TRACE_EVENTS, totalEventsRead );
  UTILS_MSG( options.verbose >= VERBOSE_SOME, 
             "  [%u] Total number of processed events (per process): %" PRIu64, 
             mpiRank, totalEventsRead );
  
  // add number of host streams to statistics to get a total number for summary output
  analysis.getStatistics().setActivityCount( STAT_HOST_STREAMS, 
                                             analysis.getHostStreams().size() );
  analysis.getStatistics().setActivityCount( STAT_DEVICE_NUM, 
                                             analysis.getStreamGroup().getNumDevices() );
}

void
Runner::mergeActivityGroups()
{
  OTF2ParallelTraceWriter::ActivityGroupMap* activityGroupMap =
    writer->getActivityGroupMap();
  
  assert( activityGroupMap );

  // waiting time on each process
  uint64_t processWaitingTime = 0;
  
  // total blame over all processes
  double   globalBlame        = 0;
  
  uint64_t lengthCritPath = globalLengthCP;
  
  /*UTILS_OUT( "CP length (%llu - %llu) = %llu",
             analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL )->getTime(),
             analysis.getSourceNode()->getTime(),
             lengthCritPath );*/

  // compute total process-local blame and process-local CP fraction of activity groups
  for ( OTF2ParallelTraceWriter::ActivityGroupMap::iterator groupIter =
          activityGroupMap->begin(); groupIter != activityGroupMap->end(); 
        ++groupIter )
  {
    //IParallelTraceWriter::ActivityGroupMap::iterator iter = groupIter;
    //UTILS_OUT( "Group ID %u", groupIter->second.functionId);
    
    // set the local CP fraction for this activity type
    groupIter->second.fractionCP =
      (double)( groupIter->second.totalDurationOnCP ) / (double)lengthCritPath;
    
    if ( 0 == mpiRank )
    {
      // add the activity groups blame to the total global blame
      globalBlame += groupIter->second.totalBlame;

      // sum up waiting time of individual regions for rank 0
      processWaitingTime += groupIter->second.waitingTime;
    }
  }

  // ************* Phase 2: MPI all-reduce *************** //
  const int MPI_ENTRIES_TAG = 22;
  
  // send/receive groups to master/from other MPI streams
  if ( 0 == mpiRank )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC,
               " Combining regions from %d analysis processes", mpiSize );
  
    // receive number of regions from other processes
    uint32_t numEntriesSend = 0; // we do not evaluate this for root rank 0
    uint32_t numRegionsRecv[ mpiSize ];
    MPI_CHECK( MPI_Gather( &numEntriesSend, 1, MPI_UINT32_T, 
                           numRegionsRecv,  1, MPI_UINT32_T, 
                           0, MPI_COMM_WORLD ) );
    
    // initially assign rank 0 with its waiting time
    this->maxWaitingTime = processWaitingTime;
    this->maxWtimeRank = 0;
    
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
        
        // total waiting time per process
        processWaitingTime = 0;

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
            groupIter->second.waitingTime       += group->waitingTime;
            
            for( int i = 0; i < REASON_NUMBER; i++ )
            {
              groupIter->second.blame4[ i ] += group->blame4[ i ];
            }
            
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
            ( *activityGroupMap )[fId].waitingTime       = group->waitingTime;
            ( *activityGroupMap )[fId].numUnifyStreams   = group->numUnifyStreams;
            
            for( int i = 0; i < REASON_NUMBER; i++ )
            {
              ( *activityGroupMap )[ fId ].blame4[ i ]  = 
                group->blame4[ i ];
            }
          }

          // sum up over all regions
          {
            // add the region's blame to overall blame
            globalBlame += group->totalBlame;

            // get the waiting time per process (\todo: could be done by each process)
            processWaitingTime += group->waitingTime;
          }
        }
        
        // find rank with maximum waiting time
        if( processWaitingTime > maxWaitingTime )
        {
          maxWaitingTime = processWaitingTime;
          maxWtimeRank = rank;
        }

        delete[]buf;
      }
    }

    // set the global CP fraction for all program regions
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
        groupIter->second.fractionBlame = groupIter->second.totalBlame 
                                        / globalBlame;
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

  //// summarize inefficiency patterns and wait statistics ////
  uint64_t statsRecvBuf[ mpiSize * STAT_NUMBER ];
  
  //\todo: MPI_Reduce was more efficient, but we might detect imbalances
  MPI_CHECK( MPI_Gather( stats.getStats(), STAT_NUMBER, MPI_UINT64_T, 
                         statsRecvBuf,  STAT_NUMBER, MPI_UINT64_T, 
                         0, MPI_COMM_WORLD ) );

  if( 0 == mpiRank )
  {
    // add for each MPI stream
    // \todo: here we might detect imbalances
    #pragma omp parallel for
    for ( int rank = STAT_NUMBER; // ignore own values
          rank < mpiSize * STAT_NUMBER; 
          rank += STAT_NUMBER )
    {
      stats.addAllStats( &statsRecvBuf[ rank ] );
    }
  }
  
  //// aggregate number of activity occurrences over all processes ////
  uint64_t *recvBuf = NULL;
  
  if( static_cast<unsigned>(STAT_ACTIVITY_TYPE_NUMBER) <= 
                                            static_cast<unsigned>(STAT_NUMBER) )
  {
    recvBuf = statsRecvBuf;
  }
  else
  {
    recvBuf = (uint64_t*)malloc( mpiSize * STAT_NUMBER * sizeof( uint64_t ) );
  }
  
  /* MPI_Gather would enable some kind of imbalance analysis on the root process 
  //MPI_CHECK( MPI_Gather( stats.getActivityCounts(), STAT_ACTIVITY_TYPE_NUMBER, 
  //                       MPI_UINT64_T, recvBuf,  STAT_ACTIVITY_TYPE_NUMBER, 
  //                       MPI_UINT64_T, 0, MPI_COMM_WORLD ) );
  if( 0 == mpiRank )
  {
    // add for each MPI stream
    #pragma omp parallel for
    for ( int rank = STAT_ACTIVITY_TYPE_NUMBER; // ignore own values
          rank < mpiSize * STAT_ACTIVITY_TYPE_NUMBER; 
          rank += STAT_ACTIVITY_TYPE_NUMBER )
    {
      stats.addActivityCounts( &recvBuf[ rank ] );
    }
  }*/
  MPI_CHECK( MPI_Reduce( stats.getActivityCounts(), recvBuf, 
                         STAT_ACTIVITY_TYPE_NUMBER, MPI_UINT64_T, MPI_SUM, 0, 
                         MPI_COMM_WORLD ) );
  if( 0 == mpiRank )
  {
    // set the accumulated values
    stats.setActivityCounts( recvBuf );
  }
  
  // free memory if allocated beforehand
  if( static_cast<unsigned>(STAT_ACTIVITY_TYPE_NUMBER) > 
                                          static_cast<unsigned>(STAT_NUMBER) )
  {
    free(recvBuf);
  }
  //////////////////////////////////////////////////////////////
  
  /* print the total number of processed events over all processes
  if( options.verbose >= VERBOSE_TIME )
  {
    uint64_t total_events = 0;
    MPI_CHECK( MPI_Reduce( &total_events_read, &total_events, 1, 
                           MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD ) );
    
    total_events_read = total_events;
  }*/
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
#if defined(SCOREP_USER_ENABLE)
  SCOREP_USER_REGION( "computeCriticalPath", SCOREP_USER_REGION_TYPE_FUNCTION )
#endif
    
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
    UTILS_MSG_NOBR( mpiRank == 0 && options.verbose >= VERBOSE_BASIC,
                    "Start critical-path detection for MPI ..." );
    detectCriticalPathMPIP2P( sectionsList, criticalNodes );
    
    if( sectionsList.size() > 0 )
    {
      // detect the critical path within all sections individually
      UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_BASIC,
                 " for %llu sections.", sectionsList.size() );
      
      //getCriticalLocalNodes( sectionsList, criticalNodes );
      processSectionsParallel( sectionsList, criticalNodes );
      
      sectionsList.clear();
    }
    else
    {
      UTILS_MSG( mpiRank == 0 && options.verbose >= VERBOSE_BASIC, "");
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
    UTILS_MSG( options.verbose > VERBOSE_BASIC && mpiRank == 0,
               "  Set time-on-critical-path for nodes" );

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
//    UTILS_OUT( "[%d] No critical last time (stream %llu)", mpiRank,
//                     criticalPathEnd.first );
//  }

  if( 0 == lastTime )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC, 
               "[%d] Process is not on the critical path?", mpiRank );
  }
  
  //UTILS_OUT( "[%d] Last time stamp: %llu", mpiRank, lastTime );
    
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
//    UTILS_OUT( "[%d] No critical first (stream %llu) or last time (stream %llu)", mpiRank,
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
  if( firstTime[0] == globalFirstCriticalTime && 
      analysis.getStream( criticalPathStart.first ) )
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

void
Runner::getCriticalLocalNodes( MPIAnalysis::CriticalPathSection* section,
                               EventStream::SortedGraphNodeList& criticalNodes )
{
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
               "  [%d] computing local critical path between MPI nodes [%s, %s]",
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

  //\todo: replace left and right link with edges
  GraphNode* startLocalNode = startNode->getLinkRight();
  GraphNode* endLocalNode   = endNode->getLinkLeft();

  if ( ( !startLocalNode || !endLocalNode ) ||
       ( startLocalNode->getTime() >= endLocalNode->getTime() ) )
  {
    UTILS_MSG( options.verbose > VERBOSE_ALL && startLocalNode && endLocalNode,
               "  [%d] No local path between MPI nodes %s (link right %p, %s) "
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

    return;
  }

  UTILS_MSG( options.verbose > VERBOSE_ALL,
             "  [%d] Computing local critical path for section (%s,%s): (%s,%s)",
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
Runner::processSectionsParallel( MPIAnalysis::CriticalSectionsList& sections,
                                 EventStream::SortedGraphNodeList& criticalNodes )
{
  // compute all MPI-local critical sections in parallel
  #pragma omp parallel for
  for ( uint32_t i = 0; i < sections.size(); ++i )
  {
    MPIAnalysis::CriticalPathSection* section = &( sections[i] );
    
    getCriticalLocalNodes( section, criticalNodes );
  }
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
                   "  [%u] critical path reverse replay starts at node %s (%f)",
                   mpiRank, analysis.getNodeInfo(localLastMpiEnter).c_str() );
   
  }
  
  return lastMpiRank;
}

/**
 * Detect the critical path of the MPI sub graph and generate a list of critical
 * sections that are analyzed locally (but OpenMP parallel).
 * This routine implements a master slave concept. The master MPI nodes on the
 * process as critical until a blocking local MPI edge is found. Then it sends 
 * a message (new master) to the non-blocking communication partner using the MPI remote node
 * which has been generated during the analysis. The slaves wait for a message
 * and process their "critical MPI sections".
 *
 * @param sectionsList  critical sections between MPI regions on the critical 
 *                      path will be stored in this list
 * @param criticalNodes critical nodes can be computed instead of actively 
 *                      waiting for the master
 */
void
Runner::detectCriticalPathMPIP2P( MPIAnalysis::CriticalSectionsList& sectionList,
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
  
  bool isMaster = false;
  
  // this assumes that the last MPI node is a collective and the collective rule
  // created blocking edges for all MPI activities but the last entering one
  currentNode = analysis.getLastGraphNode( PARADIGM_MPI );
  if( !currentNode->isLeave() )
  {
    UTILS_WARNING( "[%u] Last MPI node should be a leave! %s", mpiRank,
                   analysis.getNodeInfo( currentNode ).c_str() );
  }
  else
  {
    Edge* lastMPIEdge = 
      analysis.getEdge( currentNode->getGraphPair().first, currentNode );
    
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
                   "[%u] isMaster, currentNode = %s, lastNode = %s",
                   mpiRank, analysis.getNodeInfo( currentNode ).c_str(),
                   analysis.getNodeInfo( lastNode ).c_str() );
      }
      else
      {
        UTILS_MSG( options.verbose >= VERBOSE_ANNOY,
                   "[%u] isMaster, currentNode = %s",
                   mpiRank, analysis.getNodeInfo( currentNode ).c_str() );
      }

      UTILS_MSG( lastNode && ( lastNode->getId() <= currentNode->getId() ),
                 "[%u] ! [Warning] current node ID %" PRIu64 " (%s) is not strictly "
                 "decreasing; last node ID %" PRIu64 " (%s)", 
                 mpiRank, currentNode->getId(), 
                 analysis.getNodeInfo( currentNode ).c_str(),
                 lastNode->getId(), analysis.getNodeInfo( lastNode ).c_str() );
      
      //\todo: the intermediate begin has to be the start of a section
      if ( currentNode->isLeave() ) // isLeave
      {
        Edge* activityEdge = 
          analysis.getEdge( currentNode->getGraphPair().first, currentNode );
        
        if( !activityEdge )
        {
          UTILS_WARNING( "[%u] CPA master: No activity edge found for %s. Abort CPA.", 
                         mpiRank, analysis.getNodeInfo( currentNode ).c_str() );
          
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
                           mpiRank, analysis.getNodeInfo( currentNode ).c_str(), 
                           analysis.getNodeInfo( sectionEndNode ).c_str() );
            sectionList.push_back( section );

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
                         "[%d]  found wait state for %s, changing stream",
                         mpiRank, analysis.getNodeInfo( currentNode ).c_str() );

          // get global MPI world rank from stream ID
          uint32_t mpiPartnerRank = 
            analysis.getMPIAnalysis().getMPIRank( pnPair.streamID );
                 
          // prepare send buffer
          sendBfr[0] = pnPair.nodeID;
          sendBfr[1] = NO_MSG; // no specific message
          
          // transfer blame for MPI_Wait[all]
          if( currentNode->isMPIWait() || currentNode->isMPIWaitall() )
          {
            sendBfr[2] = currentNode->getWaitingTime();
          }
          else
          {
            sendBfr[2] = 0;
          }

          UTILS_DBG_MSG( DEBUG_CPA_MPI,
                         "[%d]  testing remote MPI worker %u for remote edge to"
                         " my node %" PRIu64 " on stream %" PRIu64,
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
                           mpiRank, analysis.getNodeInfo( currentNode ).c_str(), 
                           analysis.getNodeInfo( sectionEndNode ).c_str() );
            sectionList.push_back( section );
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
                     "[%d] Critical path reached global collective %s. "
                     "Asking all slaves to terminate", mpiRank, 
                     analysis.getNodeInfo( currentNode ).c_str() );

          // send "path found" message to all ranks
          sendBfr[0] = 0;
          sendBfr[1] = PATH_FOUND_MSG;

          // get all MPI ranks
          std::vector< uint32_t > mpiPartners =
            analysis.getMPIAnalysis().getMPICommGroup( 0 ).procs;
          
          for ( std::vector< uint32_t >::const_iterator iter = mpiPartners.begin();
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
          throw RTException( "[%d] No ingoing intra-stream edge for node %s",
                             mpiRank, analysis.getNodeInfo( currentNode ).c_str() );
        }
      } // END: isEnter (master)
    } ///////////////////////////// END: master ////////////////////////////////
    else
    {
      //////////////////////////////// slave ////////////////////////////////
      UTILS_DBG_MSG( DEBUG_CPA_MPI, "[%d] Slave receives... ", mpiRank);

      // use a non-blocking MPI receive to start local CPA meanwhile
      MPI_Request request_recv = MPI_REQUEST_NULL;
      int finished = 0;
      MPI_CHECK( MPI_Irecv( recvBfr, BUFFER_SIZE, MPI_UINT64_T, MPI_ANY_SOURCE,
                            MPI_CPA_TAG, MPI_COMM_WORLD, &request_recv ) );
      
      MPI_Status status;
      MPI_CHECK( MPI_Test( &request_recv, &finished, &status ) );

      // if we have to wait for the master, start the local CPA for sections
      while( !finished )
      {
        if( sectionList.size() > 0 )
        {
          // compute last local critical section (last element in the vector)
          getCriticalLocalNodes( &(sectionList.back()), criticalNodes );
          sectionList.pop_back();
          
          // check if the receive is now finished
          MPI_CHECK( MPI_Test( &request_recv, &finished, &status ) );
        }
        else
        {
          // if no more sections are available: 
          // wait for the MPI_Irecv and leave the loop
          MPI_CHECK( MPI_Wait( &request_recv, &status ) );
          finished = 1;
          break;
        }
      }
      
      // if the master send "found message" we can leave the while loop
      if ( recvBfr[1] == PATH_FOUND_MSG )
      {
        UTILS_MSG( options.verbose >= VERBOSE_ALL,
                   "[%d] * terminate requested by master", mpiRank );
        break;
      }
      
      // ID of the next critical path node (send from remote rank)
      uint64_t nextNodeID = recvBfr[0];
   
      // find local node for remote node id and decide if we can continue here
      UTILS_DBG_MSG( DEBUG_CPA_MPI,
                     "[%u]  tested by remote MPI worker %u for node %" PRIu64,
                     mpiRank, status.MPI_SOURCE,
                     nextNodeID ); // continuation node ID

      const Graph::NodeList& nodes = mpiGraph->getNodes();
      
      // binary search for continuation node
      GraphNode* slaveLeaveNode = 
        GraphNode::findNode( nextNodeID, nodes );

      //////// if the node could not be found, do a sequential search //////////
      if( !slaveLeaveNode || slaveLeaveNode->getId() != nextNodeID )
      {
        // reset slaveNode
        slaveLeaveNode = NULL;

        // sequential search 
        for( Graph::NodeList::const_iterator iter = nodes.begin();
             iter != nodes.end(); ++iter )
        {
          if( (*iter)->getId() == nextNodeID )
          {
            slaveLeaveNode = ( *iter );
            break;
          }
        }

        if( !slaveLeaveNode )
        {
          if( nextNodeID < nodes.front()->getId() )
          {
            UTILS_WARNING( "[%d] Node ID %" PRIu64 " is out of range "
                           "[%" PRIu64 ",%" PRIu64 "]! Send from %d",
                           mpiRank, nextNodeID, nodes.front()->getId(), 
                           nodes.back()->getId(), status.MPI_SOURCE );
          }
          else
          {
            UTILS_WARNING( "[%d] Sequential search for node ID %" PRIu64 " failed!",
                           mpiRank, nextNodeID );
          }
          
          //UTILS_WARNING( "[%d] Continue at first local MPI node %s",
          //               mpiRank, analysis.getNodeInfo( slaveLeaveNode ).c_str() );
          //slaveLeaveNode = mpiGraph->getNodes().front();
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
                     analysis.getNodeInfo( currentNode ).c_str(),
                     analysis.getNodeInfo( lastNode ).c_str() );

      // continue main loop as master
    }
  }
  
  // allocated before this loop and not bound to any other object
  delete mpiGraph;

  // make sure that every process is leaving
  MPI_CHECK( MPI_Barrier( MPI_COMM_WORLD ) );
}

/**
 * Write the summary statistics to file and stdout.
 */
void
Runner::writeStatistics()
{
  if ( mpiRank == 0 )
  {
    char mode[1] = {'w'};
    
    // use append mode, if timing information have already been written
    if( options.verbose >= VERBOSE_TIME )
    {
      mode[0] = 'a';
    }
    
    std::string sFileName = Parser::getInstance().getSummaryFileName();
    
    FILE *sFile = fopen( sFileName.c_str(), mode );
    
    if( NULL == sFile )
    {
      sFile = stdout;
    }

    ///////////// print information on activity occurrences ///////////
    Statistics& stats = analysis.getStatistics();
    
    fprintf( sFile, "- %s: %19" PRIu64 "\n",
             casita::typeStrTableActivity[ STAT_TOTAL_TRACE_EVENTS ].str, 
             //total_events_read, 
             stats.getActivityCounts()[ STAT_TOTAL_TRACE_EVENTS ] );
    
    for(int i = 0; i < STAT_ACTIVITY_TYPE_NUMBER-3; i++)
    {
      uint64_t actCount = stats.getActivityCounts()[ i ];
      if( actCount )
      {
        for(int j = 0; j < STAT_ACTIVITY_TYPE_NUMBER-3; j++)
        {
          if( casita::typeStrTableActivity[ i ].type == 
              casita::typeStrTableActivity[ j ].type )
          {
            fprintf( sFile, "%29s: %19" PRIu64 "\n", 
                     casita::typeStrTableActivity[ i ].str, actCount );
            break;
          }
        }
      }
    }
    
    if( sFile )
    {
      fclose( sFile );
    }
  }
}
 
void
Runner::writeActivityRating()
{
  if ( mpiRank == 0 )
  {
    char mode[1] = {'w'};
    
    // use append mode, if timing information have already been written
    if( options.verbose >= VERBOSE_TIME )
    {
      mode[0] = 'a';
    }
    
    std::string sFileName = Parser::getInstance().getSummaryFileName();
    
    FILE *sFile = fopen( sFileName.c_str(), mode );
    
    if( NULL == sFile )
    {
      sFile = stdout;
    }
    
    /////////////// Region statistics //////////////7
    OTF2ParallelTraceWriter::ActivityGroupMap* activityGroupMap =
                                                  writer->getActivityGroupMap();
    
    // length of the first column in stdout (region name)
    const int RNAMELEN = 42;
    
    if( globalLengthCP == 0 )
    {
      UTILS_OUT( "Global critical path length is 0. Skipping output ..." );
      return;
    }

    fprintf( sFile, "\n%*s %10s %11s %11s %6s %11s %8s %10s\n",
            RNAMELEN, "Region",
            "Calls",
            "Time[s]",
            "Time on CP",
            "CP[%]",
            "Blame[s]",
            "Blame[%]",
            "Blame on CP" );

    fprintf( sFile, "%.*s", 
             RNAMELEN, "-----------------------------------------------------" );
    fprintf( sFile, "\n" );

    std::set< OTF2ParallelTraceWriter::ActivityGroup,
              OTF2ParallelTraceWriter::ActivityGroupCompare > sortedActivityGroups;
      
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
      
    // print rating in CSV file (file has to be declared in this scope for closing it)
    FILE *ratingFile;
    if (options.createRatingCSV){
      
      UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
                 "[0] generate rating as csv file" );

      string rFileName = Parser::getInstance().getPathToFile() 
                       + string( "/" ) + Parser::getInstance().getOutArchiveName()
                       + string( "_rating.csv" );
      
      ratingFile = fopen( rFileName.c_str(), "w" );
      fprintf(ratingFile, "Region;Calls;Time [s];Time on CP [s];"
                           "CP Ratio [%%];Blame [s];Blame Ratio [%%];"
                           "Blame on CP [s]" );
      
      for(int i = 0; i < REASON_NUMBER; i++ )
      {
        fprintf(ratingFile, "; blame for %s [%%]", casita::typeStrTableBlameReason[ i ].str );
      }
      
      fprintf(ratingFile, "\n" );      
    }
    
    uint32_t sumInstances   = 0;
    uint64_t sumDuration    = 0;
    uint64_t sumDurationCP  = 0;
    double sumBlameOnCP     = 0.0;  
    double sumFractionCP    = 0.0;
    double sumBlame         = 0.0;
    double sumFractionBlame = 0.0;
    uint64_t sumWaitingTime = 0;
    
    size_t ctr = 0;
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
        sumBlame         += iter->totalBlame;
        sumFractionBlame += iter->fractionBlame;
        
        const char* regName = definitions.getRegionName( iter->functionId );
        size_t regNameLen = strlen( regName );
        size_t regShift = 0;
        
        if( regNameLen > ( size_t ) RNAMELEN )
        {
          do
          {
            fprintf( sFile, "%.*s\n", RNAMELEN-1, regName + regShift );
            regShift += RNAMELEN - 1;
          } while( ( regNameLen - regShift ) > ( size_t ) RNAMELEN );
        }

        fprintf( sFile, "%*.*s %10u %11f %11f %6.2f %11f %8.4f %11.6f",
                RNAMELEN, RNAMELEN, regName + regShift,
                iter->numInstances,
                analysis.getRealTime( iter->totalDuration ),
                analysis.getRealTime( iter->totalDurationOnCP ),
                100.0 * iter->fractionCP,
                iter->totalBlame,
                100.0 * iter->fractionBlame,
                iter->blameOnCP );
        
        // blame reasons
        if( options.extendedBlame && iter->totalBlame > 0 ) // avoid division by zero
        {        
          for(int i = 0; i < REASON_NUMBER; i++ )
          {
            double blameShare = 100*iter->blame4[ i ] / iter->totalBlame;
            if( iter->blame4[ i ] > 0 && blameShare >= 1.0 )
            {
              for(int j = 0; j < REASON_NUMBER; j++)
              {
                if( casita::typeStrTableBlameReason[ i ].type == 
                    casita::typeStrTableBlameReason[ j ].type )
                {
                  fprintf( sFile, "\n -> %3.2f%% (%.6f s) blame for %s", 
                    blameShare, iter->blame4[i], 
                    casita::typeStrTableBlameReason[ i ].str );
                  break;
                }
              }
            }
          }
        }
        fprintf( sFile, "\n" );
        
        ++ctr;
      }

      if( options.createRatingCSV )
      {
        fprintf( ratingFile, "%s;%u;%lf;%lf;%lf;%lf;%lf;%lf",
                 definitions.getRegionName( iter->functionId ),
                 iter->numInstances,
                 analysis.getRealTime( iter->totalDuration ),
                 analysis.getRealTime( iter->totalDurationOnCP ),
                 100.0 * iter->fractionCP,
                 iter->totalBlame,
                 100.0 * iter->fractionBlame,
                 iter->blameOnCP );
        
        if( iter->totalBlame > 0 ) // avoid division by zero
        {
          for(int i = 0; i < REASON_NUMBER; i++ )
          {
            fprintf( ratingFile, ";%lf", 100*iter->blame4[ i ] / iter->totalBlame );
          }
        }
        
        fprintf( ratingFile, "\n" );
      }
      
      sumWaitingTime += iter->waitingTime;
    }
    
    if (options.createRatingCSV)
    {
      fclose(ratingFile);
    }
    
    fprintf( sFile, "%.*s", 
            RNAMELEN, "-----------------------------------------------------" );
    fprintf( sFile, "\n%*.*s%2lu %10u %11lf %11lf %6.2lf %11lf %8.4lf %11.6lf\n",
              RNAMELEN - 2, RNAMELEN - 2, "Sum of Top ",
              ctr,
              sumInstances,
              analysis.getRealTime( sumDuration ),
              analysis.getRealTime( sumDurationCP ),
              100.0 * sumFractionCP,
              sumBlame,
              100.0 * sumFractionBlame, 
              sumBlameOnCP );
    
    // some more statistics
    Statistics& stats = analysis.getStatistics();
    fprintf( sFile, "\nStream summary: %d MPI ranks, %" PRIu64 " host streams, %" PRIu64 " devices",
             mpiSize,
             stats.getActivityCounts()[ STAT_HOST_STREAMS ],
             stats.getActivityCounts()[ STAT_DEVICE_NUM ]
           );
    
    // print inefficiency and wait statistics
    fprintf( sFile, "\n"
             "%-31.31s: %11lf s\n"
             "%-31.31s: %11lf s (%lf s per rank, %lf s (%2.2lf%%) per host stream)\n",
             "Total program runtime", 
             (double) definitions.getTraceLength() / (double) definitions.getTimerResolution(),
             "Total waiting time (host)",
             analysis.getRealTime( sumWaitingTime ),
             analysis.getRealTime( sumWaitingTime ) / mpiSize,
             analysis.getRealTime( sumWaitingTime ) / 
               stats.getActivityCounts()[ STAT_HOST_STREAMS ],
             analysis.getRealTime( sumWaitingTime ) / 
               stats.getActivityCounts()[ STAT_HOST_STREAMS ] /
                 analysis.getRealTime( definitions.getTraceLength() )
               * 100
            ); 
    fprintf( sFile, "%47c %lf s on rank %d (max. waiting time)\n", ' ',
             analysis.getRealTime( this->maxWaitingTime ), this->maxWtimeRank );
              
    fprintf( sFile, "\nPattern summary:\n" );
            
    //// MPI ////
    uint64_t patternCount = stats.getStats()[MPI_STAT_LATE_SENDER] + 
                            stats.getStats()[ MPI_STAT_LATE_RECEIVER ] +
                            stats.getStats()[MPI_STAT_SENDRECV] +
                            stats.getStats()[ MPI_STAT_WAITALL_LATEPARTNER ] +
                            stats.getStats()[ MPI_STAT_COLLECTIVE ];
    if( patternCount )
    {
      double ptime = analysis.getRealTime( 
                stats.getStats()[MPI_STAT_LATE_SENDER_WTIME] + 
                stats.getStats()[MPI_STAT_LATE_RECEIVER_WTIME] +
                stats.getStats()[MPI_STAT_SENDRECV_WTIME] +
                stats.getStats()[MPI_STAT_WAITALL_LATEPARTNER_WTIME] +
                stats.getStats()[MPI_STAT_COLLECTIVE_WTIME]);
      
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              "MPI wait patterns",
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
                            
    patternCount = stats.getStats()[MPI_STAT_LATE_SENDER];
    if( patternCount )
    {
      double ptime = 
          analysis.getRealTime( stats.getStats()[MPI_STAT_LATE_SENDER_WTIME] );
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              " Late sender", 
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
    
    patternCount = stats.getStats()[ MPI_STAT_LATE_RECEIVER ];
    if( patternCount )
    {
      double ptime = 
          analysis.getRealTime( stats.getStats()[MPI_STAT_LATE_RECEIVER_WTIME] );
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              " Late receiver",
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
    
    patternCount = stats.getStats()[MPI_STAT_SENDRECV];
    if( patternCount )
    {
      double ptime = 
          analysis.getRealTime( stats.getStats()[MPI_STAT_SENDRECV_WTIME] );
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              " Wait in MPI_Sendrecv",
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
    
    patternCount = stats.getStats()[ MPI_STAT_WAITALL_LATEPARTNER ];
    if( patternCount )
    {
      double ptime = 
          analysis.getRealTime( stats.getStats()[MPI_STAT_WAITALL_LATEPARTNER_WTIME] );
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              " MPI_Waitall late partner",
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
    
    patternCount = stats.getStats()[ MPI_STAT_COLLECTIVE ];
    if( patternCount )
    {
      double ptime = 
          analysis.getRealTime( stats.getStats()[MPI_STAT_COLLECTIVE_WTIME] );
      fprintf( sFile, " %-30.30s: %11lf s (%lf s per rank; %" PRIu64 " overall occurrences)\n",
              " Wait in MPI collective",
              ptime, ptime/(double)analysis.getMPISize(), patternCount );
    }
    
    //// OpenMP ////
    if( analysis.haveParadigm( PARADIGM_OMP ) )
    {
      patternCount = stats.getStats()[ OMP_STAT_BARRIER ];
      if( patternCount )
      {
        fprintf( sFile, " OpenMP\n"
                " %-30.30s: %11lf s (%lf s per host stream, %" PRIu64 " overall occurrences)\n",
                " Wait in OpenMP barrier",
                analysis.getRealTime( stats.getStats()[OMP_STAT_BARRIER_WTIME] ),
                analysis.getRealTime( stats.getStats()[OMP_STAT_BARRIER_WTIME] ) /
                  stats.getActivityCounts()[ STAT_HOST_STREAMS ],
                patternCount );
      }
    }
    
    //// Offloading ////
    if( !Parser::ignoreOffload() && ( analysis.haveParadigm( PARADIGM_CUDA ) || 
                                      analysis.haveParadigm( PARADIGM_OCL ) ) )
    {
      fprintf( sFile, " Offloading\n"
              " %-30.30s: %11lf s (%2.2lf%% -> %lf s per device)\n"
              " %-30.30s: %11lf s (%2.2lf%% -> %lf s per device)\n",
              " Idle device",
              analysis.getRealTime( stats.getStats()[OFLD_STAT_IDLE_TIME] ),
              (double) stats.getStats()[OFLD_STAT_IDLE_TIME] / 
              (double) stats.getStats()[OFLD_STAT_OFLD_TIME] * 100,
              analysis.getRealTime( stats.getStats()[OFLD_STAT_IDLE_TIME] ) / 
                stats.getActivityCounts()[ STAT_DEVICE_NUM ],
              " Compute idle device",
              analysis.getRealTime( stats.getStats()[OFLD_STAT_COMPUTE_IDLE_TIME] ), 
              (double) stats.getStats()[OFLD_STAT_COMPUTE_IDLE_TIME] / 
              (double) stats.getStats()[OFLD_STAT_OFLD_TIME] * 100,
              analysis.getRealTime( stats.getStats()[OFLD_STAT_COMPUTE_IDLE_TIME] ) /
                stats.getActivityCounts()[ STAT_DEVICE_NUM ]
         );

      patternCount = stats.getStats()[OFLD_STAT_EARLY_BLOCKING_WAIT];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%" PRIu64 " occurrences, on kernel: %lf s)\n",
          " Early blocking wait",
          analysis.getRealTime( stats.getStats()[OFLD_STAT_EARLY_BLOCKING_WTIME] ),
          patternCount,
          analysis.getRealTime( stats.getStats()[OFLD_STAT_EARLY_BLOCKING_WTIME_KERNEL] ));
      }

      patternCount = stats.getStats()[OFLD_STAT_EARLY_TEST];
      if( patternCount )
      {
        fprintf( sFile, "  Early test for completion: %" PRIu64 " (%lf s)\n", patternCount,
          analysis.getRealTime( stats.getStats()[OFLD_STAT_EARLY_TEST_TIME] ) );
      }
      
      patternCount = stats.getStats()[STAT_OFLD_TOTAL_TRANSFER_TIME];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%2.2lf%% of offload time)\n",
                 " Total communication time", 
                 analysis.getRealTime( patternCount ),
                 (double) stats.getStats()[STAT_OFLD_TOTAL_TRANSFER_TIME] / 
                   (double) stats.getStats()[OFLD_STAT_OFLD_TIME] * 100 );
      }
      
      patternCount = stats.getStats()[OFLD_STAT_COMPUTE_IDLE_TIME]
                   - stats.getStats()[OFLD_STAT_IDLE_TIME];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%2.2lf%% of total communication time)\n",
                 " -Exclusive communication", 
                 analysis.getRealTime( patternCount ),
                 (double) patternCount / 
                   (double) stats.getStats()[STAT_OFLD_TOTAL_TRANSFER_TIME] * 100 );
      }
              
      patternCount = stats.getStats()[STAT_OFLD_TOTAL_TRANSFER_TIME]
                   - ( stats.getStats()[OFLD_STAT_COMPUTE_IDLE_TIME]
                       - stats.getStats()[OFLD_STAT_IDLE_TIME] );
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%2.2lf%% of total communication time)\n",
                 " -Copy-compute overlap", 
                 analysis.getRealTime( patternCount ),
                 (double) patternCount / 
                   (double) stats.getStats()[STAT_OFLD_TOTAL_TRANSFER_TIME] * 100 );
      }
      else
      {
        fprintf( sFile, "  No overlap between copy and compute tasks!\n\n" );
      }

      patternCount = stats.getStats()[STAT_OFLD_BLOCKING_COM];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%" PRIu64 " occurrences)",
                " -Blocking communication",
          analysis.getRealTime( stats.getStats()[STAT_OFLD_BLOCKING_COM_TIME] ),
          patternCount );
        
        patternCount = stats.getStats()[OFLD_STAT_BLOCKING_COM_EXCL_TIME];
        if( patternCount )
        {
          fprintf( sFile, ", exclusive: %lf s\n",
            analysis.getRealTime( patternCount ) );
        }
        else
        {
          fprintf( sFile, "\n" );
        }
      }

      patternCount = stats.getStats()[OFLD_STAT_MULTIPLE_COM];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s (%" PRIu64 " occurrences)",
                " -Consecutive communication",
          analysis.getRealTime( stats.getStats()[OFLD_STAT_MULTIPLE_COM_TIME] ),
          patternCount );
      }
      
      patternCount = stats.getStats()[OFLD_STAT_MULTIPLE_COM_SD];
      if( patternCount )
      {
        fprintf( sFile, ", same direction: %lf s (%" PRIu64 " occurrences)",
          analysis.getRealTime( stats.getStats()[OFLD_STAT_MULTIPLE_COM_SD_TIME] ),
          patternCount );
      }
      if( stats.getStats()[OFLD_STAT_MULTIPLE_COM] || patternCount )
      {
        fprintf( sFile, "\n" );
      }
      
      patternCount = stats.getStats()[OFLD_STAT_KERNEL_START_DELAY];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf ms/kernel (%" PRIu64 " occurrences), total delay: %lf s\n",
          " Kernel startup delay",
          analysis.getRealTime( stats.getStats()[OFLD_STAT_KERNEL_START_DELAY_TIME] ) / patternCount *1000,
          patternCount,
          analysis.getRealTime( stats.getStats()[OFLD_STAT_KERNEL_START_DELAY_TIME] )
        );
      }
      
      patternCount = stats.getStats()[STAT_OFLD_COMPUTE_OVERLAP_TIME];
      if( patternCount )
      {
        fprintf( sFile, " %-30.30s: %11lf s\n",
          " Compute overlap",
          analysis.getRealTime( stats.getStats()[STAT_OFLD_COMPUTE_OVERLAP_TIME] ) );
      }
      else
      {
        fprintf( sFile, "  No overlap between compute tasks!\n" );
      }
    }
    
    if( sFile )
    {
      fclose( sFile );
    }
  }
}

// print summary file to console
void
Runner::printToStdout()
{
  if ( mpiRank == 0 )
  {  
   char* sFileName = Parser::getInstance().getSummaryFileName().c_str();
    
    ifstream fin( sFileName );
    string temp;
    while( getline( fin, temp ) )
    {
      std::cerr << temp << std::endl;
    }
    fin.close();
  }
}
