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
Runner::readOTF( )
{
  uint32_t      mpiRank     = analysis.getMPIRank( );
  ITraceReader* traceReader = NULL;

  UTILS_MSG( mpiRank == 0, "[%u] Reading OTF %s", mpiRank, options.filename.c_str( ) );

  if ( strstr( options.filename.c_str( ), ".otf2" ) != NULL )
  {
    uint32_t mpiSize = analysis.getMPISize( );
    // OTF is not supported any more
    //UTILS_DBG_MSG( mpiRank == 0, "[%u] Operating in OTF2-Mode", mpiRank );
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
             "[%u] Reading definitions", mpiRank );
  
  // read the OTF2 definitions and initialize some maps and variables of the trace reader
  traceReader->readDefinitions( );

  // TODO:
  analysis.getMPIAnalysis( ).createMPICommunicatorsFromMap( );
  
  // TODO: 
  analysis.setWaitStateFunctionId( analysis.getNewFunctionId( ) );

  uint64_t timerResolution = traceReader->getTimerResolution( );
  analysis.setTimerResolution( timerResolution );
  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[%u] Timer resolution = %llu",
             mpiRank, (long long unsigned)( timerResolution ) );

  if ( timerResolution < 1000000000 ) /* 1GHz */
  {
    UTILS_MSG( mpiRank == 0, "[%u] Warning: your timer resolution is very low (< 1 GHz)!", 
               mpiRank );
  }

  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[%u] Reading events", mpiRank );

  // use a global event reader that triggers the registered callbacks
  traceReader->readEvents( options.ignoreAsyncMpi );
  traceReader->close( );
  delete traceReader;
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
  /* send/receive groups to master/from other MPI streams */
  if ( analysis.getMPIRank( ) == 0 )
  {
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[%u] Combining results from all analysis streams", mpiRank );

    /* receive from all other MPI streams */
    uint32_t mpiSize = analysis.getMPISize( );
    for ( uint32_t rank = 1; rank < mpiSize; ++rank )
    {
      /* receive number of entries */
      uint32_t   numEntries = 0;
      MPI_Status status;

      /* /\todo: receive from any */
      MPI_Recv( &numEntries, 1, MPI_UNSIGNED, rank, 1, MPI_COMM_WORLD, &status );

      /* receive entries */
      if ( numEntries > 0 )
      {
        IParallelTraceWriter::ActivityGroup* buf =
          new IParallelTraceWriter::ActivityGroup[numEntries];
        MPI_Recv( buf,
                  numEntries * sizeof( IParallelTraceWriter::ActivityGroup ),
                  MPI_BYTE,
                  rank,
                  2,
                  MPI_COMM_WORLD,
                  &status );

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
    /* send to MPI master process (rank 0) */
    uint32_t i          = 0;
    uint32_t numEntries = activityGroupMap->size( );

    MPI_Send( &numEntries, 1, MPI_UNSIGNED, 0, 1, MPI_COMM_WORLD );

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

    MPI_Send( buf,
              numEntries * sizeof( IParallelTraceWriter::ActivityGroup ),
              MPI_CHAR,
              0,
              2,
              MPI_COMM_WORLD );

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
  MPIAnalysis::CriticalSectionsMap sectionsMap;
  criticalNodes.clear( );

  if ( mpiSize > 1 )
  {
    /* better reverse replay strategy */
    MPIAnalysis::CriticalSectionsList sectionsList;
    
    // Perform MPI revers-replay using blocking edges; create a list of sections
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Start MPI revers-replay %s ...",
               (options.criticalPathSecureMPI) ? "(with slave feedback)" : "" );
    reverseReplayMPICriticalPath( sectionsList );
    
    // detect the critical path within all sections individually
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Start critical-path detection for sections ..." );
    getCriticalLocalSections( sectionsList.data( ),
                              sectionsList.size( ), criticalNodes, sectionsMap );
  }
  else
  {
    /* compute local critical path on root, only */
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[0] Single process: defaulting to CUDA/OMP only mode" );

    Graph& subGraph = analysis.getGraph( );

    getCriticalPathIntern( analysis.getSourceNode( ),
                           analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL ),
                           criticalNodes, subGraph );
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

    /* compute total program runtime, i.e. total length of the
     *critical path */
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
      MPI_Allreduce( &firstTimestamp,
                     &firstTimestamp,
                     1,
                     MPI_UNSIGNED_LONG_LONG,
                     MPI_MIN,
                     MPI_COMM_WORLD );
      MPI_Allreduce( &lastTimestamp,
                     &lastTimestamp,
                     1,
                     MPI_UNSIGNED_LONG_LONG,
                     MPI_MAX,
                     MPI_COMM_WORLD );
    }

    globalLengthCP = lastTimestamp - firstTimestamp;
    UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
               "[%u] Critical path length = %f",
               mpiRank, analysis.getRealTime( globalLengthCP ) );

    MPI_Barrier( MPI_COMM_WORLD );
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
             analysis.getMPIRank( ),
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
               analysis.getMPIRank( ), i,
               node->getUniqueName( ).c_str( ),
               analysis.getRealTime( node->getTime( ) ) );

    i++;
  }
}

/**
 * Compute critical path for nodes between two critical MPI nodes on the
 * same process.
 *
 * @param sections
 * @param numSections
 * @param criticalNodes - store nodes on critical path here...
 * @param sectionsMap
 */
void
Runner::getCriticalLocalSections( MPIAnalysis::CriticalPathSection* sections,
                                  uint32_t                          numSections,
                                  EventStream::SortedGraphNodeList&
                                  criticalNodes,
                                  MPIAnalysis::CriticalSectionsMap&
                                  sectionsMap )
{
  uint32_t   mpiRank    = analysis.getMPIRank( );
  Graph*     subGraph   = analysis.getGraph( PARADIGM_ALL );

  omp_lock_t localNodesLock;
  omp_init_lock( &localNodesLock );

  uint32_t   lastSecCtr = 0;

#pragma omp parallel for
  for ( uint32_t i = 0; i < numSections; ++i )
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
      analysis.getStream( sections->streamID )->getNodes( );
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

    MPIAnalysis::CriticalPathSection mappedSection;
    mappedSection.streamID    = section->streamID;
    mappedSection.nodeStartID = section->nodeStartID;
    mappedSection.nodeEndID   = section->nodeEndID;

    if ( startNode->isEnter( ) )
    {
      startNode = startNode->getPartner( );
    }

    if ( endNode->isLeave( ) )
    {
      endNode = endNode->getPartner( );
    }

    GraphNode* csStartOuter   = startNode;
    GraphNode* csEndOuter     = endNode;

    sectionsMap[csStartOuter] = mappedSection;
    sectionsMap[csEndOuter]   = mappedSection;

    GraphNode* startLocalNode = startNode->getLinkRight( );
    GraphNode* endLocalNode   = endNode->getLinkLeft( );

    if ( ( !startLocalNode ||
           !endLocalNode ) ||
         ( startLocalNode->getTime( ) >= endLocalNode->getTime( ) ) )
    {
      UTILS_MSG( options.verbose > VERBOSE_ALL,
                 "[%u] No local path possible between MPI nodes %s (link %p) and %s (link %p)",
                 mpiRank,
                 startNode->getUniqueName( ).c_str( ),
                 startLocalNode,
                 endNode->getUniqueName( ).c_str( ),
                 endLocalNode );

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

    omp_set_lock( &localNodesLock );
    criticalNodes.insert( criticalNodes.end( ),
                          sectionLocalNodes.begin( ), sectionLocalNodes.end( ) );
    omp_unset_lock( &localNodesLock );

    if ( ( mpiRank == 0 ) && ( omp_get_thread_num( ) == 0 ) &&
         ( i - lastSecCtr > numSections / 10 ) )
    {
      UTILS_MSG( options.verbose >= VERBOSE_BASIC, "[%u] %lu%% ", mpiRank,
                 (size_t)( 100.0 * (double)i / (double)numSections ) );
      
      fflush( NULL );
      lastSecCtr = i;
    }
  }

  omp_destroy_lock( &localNodesLock );

  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
             "[%u] 100%%", mpiRank );

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
  uint64_t   nodeTimes[analysis.getMPISize( )];
  *node = NULL;

  GraphNode* myLastMpiNode   = analysis.getLastGraphNode( PARADIGM_MPI );
  if ( myLastMpiNode )
  {
    lastMpiNodeTime = myLastMpiNode->getTime( );
  }

  MPI_CHECK( MPI_Allgather( &lastMpiNodeTime, 1, MPI_UNSIGNED_LONG_LONG,
                            nodeTimes,
                            1, MPI_UNSIGNED_LONG_LONG, MPI_COMM_WORLD ) );

  for ( uint32_t i = 0; i < analysis.getMPISize( ); ++i )
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
      /* master */
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
                 "[%u] ! [Warning] current node ID is not strictly decreasing", 
                 mpiRank );

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
//        }
//        if ( activityEdge->isBlocking( ) )
//        {
          
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

            // prepare send buffer
            sendBfr[0] = commMaster->getId( );
            sendBfr[1] = commMaster->getStreamId( );
            sendBfr[2] = NO_MSG;

            UTILS_DBG_MSG( DEBUG_CPA_MPI,
                           "[%u]  testing remote MPI worker %u for remote edge to my node %u on stream %lu",
                           mpiRank,
                           commMpiRank,
                           (uint32_t)sendBfr[0],
                           sendBfr[1] );

            //TODO: MPI_Bcast( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, mpiRank, comm) 
            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, 0, MPI_COMM_WORLD ) );
            
            // if no master has been found yet, receive from next slave
            if ( options.criticalPathSecureMPI && 
                 sendBfr[BUFFER_SIZE - 1] == 0 )
            {
              MPI_CHECK( MPI_Recv( recvBfr, 1, MPI_UNSIGNED_LONG_LONG,
                                   commMpiRank, 42, MPI_COMM_WORLD, 
                                   MPI_STATUS_IGNORE ) );
            
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
          UTILS_MSG( true, "[%u] Critical path reached MPI_Init(). "
                           "Asking all slaves to terminate", mpiRank );

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
                                 commMpiRank, 0, MPI_COMM_WORLD ) );
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
                           MPI_ANY_TAG, MPI_COMM_WORLD, &status ) );
      
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
                             42, MPI_COMM_WORLD ) );
      }
      
      if ( haveEdge )
      {
        GraphNode::GraphNodePair& slaveActivity =
          mpiEdge.localNode->getGraphPair( );
        /* we are the new master */

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

  MPI_Barrier( MPI_COMM_WORLD );
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
  switch ( paradigm )
  {
    case PARADIGM_CUDA:
      UTILS_MSG( mpiRank == 0, "[%u] Running analysis: CUDA", mpiRank );

      break;

    case PARADIGM_MPI:
      UTILS_MSG( mpiRank == 0, "[%u] Running analysis: MPI", mpiRank );

      break;

    case PARADIGM_OMP:
      UTILS_MSG( mpiRank == 0, "[%u] Running analysis: OMP", mpiRank );

      break;

    default:
      UTILS_MSG( mpiRank == 0, "[%u] No analysis for unknown paradigm %d",
                     mpiRank, paradigm );
      return;
  }

  size_t ctr       = 0, last_ctr = 0;
  size_t num_nodes = allNodes.size( );

  for ( EventStream::SortedGraphNodeList::const_iterator nIter = allNodes.begin( );
        nIter != allNodes.end( ); ++nIter )
  {
    GraphNode* node = *nIter;
    ctr++;

    // TODO: do we really want this?
    if ( !( node->getParadigm( ) & paradigm ) )
    {
      continue;
    }

    analysis.applyRules( node, paradigm, options.verbose >= VERBOSE_BASIC );

    // print process every 5 percent (TODO: depending on number of events per paradigm)
    if ( ( mpiRank == 0 ) && options.verbose >= VERBOSE_BASIC &&
         ( ctr - last_ctr > num_nodes / 20 ) )
    {
      UTILS_MSG( true, "[%u] %lu%% ", mpiRank,
                 (size_t)( 100.0 * (double)ctr / (double)num_nodes ) );
      fflush( NULL );
      last_ctr = ctr;
    }
  }

  UTILS_MSG( options.verbose >= VERBOSE_BASIC && mpiRank == 0,
                 "[%u] 100%%", mpiRank );

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
