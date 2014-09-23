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

#include <sys/types.h>
#include <sys/time.h>

#include <omp.h>

#include "Runner.hpp"

#include "otf/IKeyValueList.hpp"
#include "otf/ITraceReader.hpp"

#include "otf/OTF2TraceReader.hpp"

using namespace casita;
using namespace casita::io;

static void
dumpAllocationTail( AnalysisEngine& analysis )
{
  EventStreamGroup::EventStreamList allProcs;
  analysis.getStreams( allProcs );

  for ( EventStreamGroup::EventStreamList::const_iterator iter = allProcs.begin( );
        iter != allProcs.end( ); ++iter )
  {
    EventStream* p = *iter;
    printf( "# %s (%lu)\n", p->getName( ), p->getId( ) );
    EventStream::SortedGraphNodeList& nodes = p->getNodes( );
    int n          = 0;
    for ( EventStream::SortedGraphNodeList::const_reverse_iterator rIter =
            nodes.rbegin( );
          rIter != nodes.rend( ); ++rIter )
    {
      if ( n > 10 )
      {
        break;
      }

      printf( "  %s\n", ( *rIter )->getUniqueName( ).c_str( ) );
      n++;
    }
  }
}

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

  if ( mpiRank == 0 )
  {
    printf( "[%u] Reading OTF %s\n", mpiRank, options.filename.c_str( ) );
  }

  if ( strstr( options.filename.c_str( ), ".otf2" ) != NULL )
  {
    uint32_t mpiSize = analysis.getMPISize( );
    if ( mpiRank == 0 )
    {
      printf( "Operating in OTF2-Mode.\n" );
    }
    traceReader = new OTF2TraceReader( &callbacks, mpiRank, mpiSize );
  }

  if ( !traceReader )
  {
    throw RTException( "Could not create trace reader" );
  }

  traceReader->handleDefProcess        = CallbackHandler::handleDefProcess;
  traceReader->handleDefFunction       = CallbackHandler::handleDefFunction;
  traceReader->handleEnter             = CallbackHandler::handleEnter;
  traceReader->handleLeave             = CallbackHandler::handleLeave;
  traceReader->handleProcessMPIMapping = CallbackHandler::handleProcessMPIMapping;
  traceReader->handleMPIComm           = CallbackHandler::handleMPIComm;
  traceReader->handleMPICommGroup      = CallbackHandler::handleMPICommGroup;

  traceReader->open( options.filename, 10 );
  if ( options.verbose >= VERBOSE_BASIC )
  {
    printf( " [%u] Reading definitions\n", mpiRank );
  }
  traceReader->readDefinitions( );

  analysis.getMPIAnalysis( ).createMPICommunicatorsFromMap( );
  analysis.setWaitStateFunctionId( analysis.getNewFunctionId( ) );

  uint64_t timerResolution = traceReader->getTimerResolution( );
  analysis.setTimerResolution( timerResolution );
  if ( options.verbose >= VERBOSE_BASIC )
  {
    printf( " [%u] Timer resolution = %llu\n",
            mpiRank, (long long unsigned)( timerResolution ) );
  }
  if ( timerResolution < 1000000000 ) /* 1GHz */
  {
    printf( " [%u] Warning: your timer resolution is very low!\n", mpiRank );
  }

  try
  {
    if ( options.verbose >= VERBOSE_BASIC )
    {
      printf( " [%u] Reading events\n", mpiRank );
    }

    traceReader->readEvents( );
  }
  catch( RTException e )
  {
    /* dump */
    dumpAllocationTail( analysis );
    throw e;
  }
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
    if ( options.verbose >= VERBOSE_BASIC )
    {
      printf( "Combining results from all analysis streams\n" );
    }

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

void
Runner::getCriticalPath( )
{
  EventStream::SortedGraphNodeList criticalNodes;
  MPIAnalysis::CriticalSectionsMap sectionsMap;
  criticalNodes.clear( );

  if ( mpiSize > 1 )
  {
    /* better reverse replay strategy */
    MPIAnalysis::CriticalSectionsList sectionsList;
    reverseReplayMPICriticalPath( sectionsList );
    getCriticalLocalSections( sectionsList.data( ),
                              sectionsList.size( ), criticalNodes, sectionsMap );
  }
  else
  {
    /* compute local critical path on root, only */
    if ( options.verbose >= VERBOSE_BASIC )
    {
      printf( "[0] Single process: defaulting to CUDA/OMP only mode\n" );
    }

    Graph& subGraph = analysis.getGraph( );

    getCriticalPathIntern( analysis.getSourceNode( ),
                           analysis.getLastGraphNode( PARADIGM_COMPUTE_LOCAL ),
                           criticalNodes, subGraph );
  }

  /* compute the time-on-critical-path counter */
  if ( mpiRank == 0 )
  {
    printf( "[%u] Computing additional counters\n", mpiRank );
  }

  const uint32_t cpCtrId     = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  for ( EventStream::SortedGraphNodeList::const_iterator iter =
          criticalNodes.begin( );
        iter != criticalNodes.end( ); ++iter )
  {
    ( *iter )->setCounter( cpCtrId, 1 );
  }

  EventStreamGroup::EventStreamList streams;
  analysis.getStreams( streams );
  const uint32_t cpTimeCtrId = analysis.getCtrTable( ).getCtrId(
    CTR_CRITICALPATH_TIME );

#pragma omp parallel for
  for ( size_t i = 0; i < streams.size( ); ++i )
  {
    EventStream* p      = streams[i];
    if ( p->isRemoteStream( ) )
    {
      continue;
    }

    EventStream::SortedGraphNodeList& nodes = p->getNodes( );

    GraphNode* lastNode = NULL;
    std::stack< uint64_t > cpTime;

    for ( EventStream::SortedGraphNodeList::const_iterator nIter = nodes.begin( );
          nIter != nodes.end( ); ++nIter )
    {
      if ( ( *nIter )->isAtomic( ) )
      {
        continue;
      }

      GraphNode* node   = (GraphNode*)( *nIter );
      if ( !lastNode )
      {
        lastNode = node;
      }

      bool lastNodeOnCP = lastNode->getCounter( cpCtrId, NULL );
      if ( node->isEnter( ) )
      {
        if ( lastNodeOnCP && !cpTime.empty( ) && node->getCounter( cpCtrId, NULL ) )
        {
          cpTime.top( ) += node->getTime( ) - lastNode->getTime( );
        }

        cpTime.push( 0 );
      }
      else
      {
        uint64_t myCPTime = cpTime.top( );
        cpTime.pop( );

        /* compute time for non-stacked function (parts) */
        if ( lastNodeOnCP &&
             ( lastNode == node->getPartner( ) || lastNode->isLeave( ) ) )
        {
          myCPTime += node->getTime( ) - lastNode->getTime( );
        }
        node->getPartner( )->setCounter( cpTimeCtrId, myCPTime );
      }

      lastNode = node;
    }
  }

  if ( options.mergeActivities )
  {
    if ( mpiRank == 0 )
    {
      printf( "[%u] Calculate total length of critical path \n", mpiRank );
    }

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
    if ( mpiRank == 0 )
    {
      std::cout << "[0] Critical path length: " << analysis.getRealTime(
        globalLengthCP ) << std::endl;
    }

    MPI_Barrier( MPI_COMM_WORLD );
  }
}

void
Runner::getCriticalPathIntern( GraphNode*                        start,
                               GraphNode*                        end,
                               EventStream::SortedGraphNodeList& cpNodes,
                               Graph&                            subGraph )
{
  if ( options.printCriticalPath )
  {
    printf( "\n[%u] Longest path (%s,%s):\n",
            analysis.getMPIRank( ),
            start->getUniqueName( ).c_str( ),
            end->getUniqueName( ).c_str( ) );
  }

  GraphNode::GraphNodeList criticalPath;
  subGraph.getLongestPath( start, end, criticalPath );

  uint32_t i = 0;
  for ( GraphNode::GraphNodeList::const_iterator cpNode = criticalPath.begin( );
        cpNode != criticalPath.end( ); ++cpNode )
  {
    GraphNode* node = *cpNode;
    cpNodes.push_back( node );

    if ( options.printCriticalPath )
    {
      printf( "[%u] %u: %s (%f)\n",
              analysis.getMPIRank( ), i,
              node->getUniqueName( ).c_str( ),
              analysis.getRealTime( node->getTime( ) ) );
    }

    i++;
  }
}

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

    if ( options.verbose >= VERBOSE_BASIC )
    {
      printf(
        "[%u] computing local critical path between MPI nodes [%u, %u] on process %lu\n",
        mpiRank,
        section->nodeStartID,
        section->nodeEndID,
        section->streamID );
    }

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
      if ( options.verbose >= VERBOSE_BASIC )
      {
        printf( "[%u] node mapping: %u = %s (%f), %u = %s (%f)\n",
                mpiRank, section->nodeStartID,
                startNode->getUniqueName( ).c_str( ),
                analysis.getRealTime( startNode->getTime( ) ),
                section->nodeEndID, endNode->getUniqueName( ).c_str( ),
                analysis.getRealTime( endNode->getTime( ) ) );
      }
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
      if ( options.verbose >= VERBOSE_BASIC )
      {
        printf(
          "[%u] No local path possible between MPI nodes %s (link %p) and %s (link %p)\n",
          mpiRank,
          startNode->getUniqueName( ).c_str( ),
          startLocalNode,
          endNode->getUniqueName( ).c_str( ),
          endLocalNode );
        if ( startLocalNode && endLocalNode )
        {
          printf( "[%u] local nodes %s and %s\n",
                  mpiRank,
                  startLocalNode->getUniqueName( ).c_str( ),
                  endLocalNode->getUniqueName( ).c_str( ) );
        }

      }
      continue;
    }

    if ( options.verbose >= VERBOSE_BASIC )
    {
      printf( "[%u] MPI/local mapping: %u > %s, %u > %s\n",
              mpiRank, section->nodeStartID,
              startLocalNode->getUniqueName( ).c_str( ),
              section->nodeEndID, endLocalNode->getUniqueName( ).c_str( ) );

      printf( "[%d] Computing local critical path (%s, %s)\n", mpiRank,
              startLocalNode->getUniqueName( ).c_str( ),
              endLocalNode->getUniqueName( ).c_str( ) );
    }

    EventStream::SortedGraphNodeList sectionLocalNodes;
    sectionLocalNodes.push_back( startNode );

    /* This new version might be faster, but does not produce correct
     * results */
    /*getLocalCriticalPath(
            sectionLocalNodes,
            startLocalNode,
            endLocalNode,
            *subGraph);*/

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
      printf( "[%u] %lu%% ", mpiRank,
              (size_t)( 100.0 * (double)i / (double)numSections ) );
      fflush( NULL );
      lastSecCtr = i;
    }
  }

  omp_destroy_lock( &localNodesLock );

  if ( mpiRank == 0 )
  {
    printf( "[%u] 100%%\n", mpiRank );
  }

  delete subGraph;
}

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

    if ( options.verbose >= VERBOSE_ANNOY )
    {
      printf( "[%u] critical path reverse replay starts at node %s (%f)\n",
              mpiRank, myLastMpiNode->getUniqueName( ).c_str( ),
              analysis.getRealTime( myLastMpiNode->getTime( ) ) );
    }

  }
}

void
Runner::reverseReplayMPICriticalPath( MPIAnalysis::CriticalSectionsList& sectionsList )
{
  const uint32_t NO_MSG         = 0;
  const uint32_t PATH_FOUND_MSG = 1;
  const size_t   BUFFER_SIZE    = 3;

  /* decide on globally last MPI node to start with */
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

  Graph*   mpiGraph = analysis.getGraph( PARADIGM_MPI );
  uint32_t cpCtrId  = analysis.getCtrTable( ).getCtrId( CTR_CRITICALPATH );
  uint64_t sendBfr[BUFFER_SIZE];
  uint64_t recvBfr[BUFFER_SIZE];

  while ( true )
  {
    if ( isMaster )
    {
      /* master */
      if ( options.verbose >= VERBOSE_ANNOY )
      {
        if ( lastNode )
        {
          printf( "[%u] isMaster, currentNode = %s (%f), lastNode = %s (%f)\n",
                  mpiRank, currentNode->getUniqueName( ).c_str( ),
                  analysis.getRealTime( currentNode->getTime( ) ),
                  lastNode->getUniqueName( ).c_str( ),
                  analysis.getRealTime( lastNode->getTime( ) ) );
        }
        else
        {
          printf( "[%u] isMaster, currentNode = %s (%f)\n",
                  mpiRank, currentNode->getUniqueName( ).c_str( ),
                  analysis.getRealTime( currentNode->getTime( ) ) );
        }
      }

      if ( lastNode && ( lastNode->getId( ) <= currentNode->getId( ) ) )
      {
        printf( "[%u] ! [Warning] current node ID is not strictly decreasing\n",
                mpiRank );
      }

      if ( currentNode->isLeave( ) )
      {
        /* isLeave */
        Edge* activityEdge = analysis.getEdge(
          currentNode->getPartner( ), currentNode );

        if ( activityEdge->isBlocking( ) || currentNode->isMPIInit( ) )
        {
          /* create section */
          if ( currentNode != sectionEndNode )
          {
            MPIAnalysis::CriticalPathSection section;
            section.streamID    = currentNode->getStreamId( );
            section.nodeStartID = currentNode->getId( );
            section.nodeEndID   = sectionEndNode->getId( );
            sectionsList.push_back( section );

            currentNode->setCounter( cpCtrId, 1 );
          }
          sectionEndNode = NULL;
        }

        /* change stream for blocking edges */
        if ( activityEdge->isBlocking( ) )
        {
          /* make myself a new slave */
          isMaster = false;

          /* commnicate with slaves to decide new master */
          GraphNode* commMaster  = currentNode->getGraphPair( ).second;
          bool nodeHasRemoteInfo = false;
          analysis.getMPIAnalysis( ).getRemoteNodeInfo( commMaster,
                                                        &nodeHasRemoteInfo );
          if ( !nodeHasRemoteInfo )
          {
            commMaster = currentNode->getGraphPair( ).first;
          }

          if ( options.verbose >= VERBOSE_ANNOY )
          {
            printf(
              "[%u]  found waitstate for %s (%f), changing stream at %s\n",
              mpiRank, currentNode->getUniqueName( ).c_str( ),
              analysis.getRealTime( currentNode->getTime( ) ),
              commMaster->getUniqueName( ).c_str( ) );
          }

          std::set< uint32_t > mpiPartnerRanks =
            analysis.getMPIAnalysis( ).getMpiPartnersRank( commMaster );
          for ( std::set< uint32_t >::const_iterator iter =
                  mpiPartnerRanks.begin( );
                iter != mpiPartnerRanks.end( ); ++iter )
          {
            /* communicate with all slaves to find new master */
            uint32_t commMpiRank = *iter;

            if ( commMpiRank == (uint32_t)mpiRank )
            {
              continue;
            }

            sendBfr[0] = commMaster->getId( );
            sendBfr[1] = commMaster->getStreamId( );
            sendBfr[2] = NO_MSG;

            if ( options.verbose >= VERBOSE_ANNOY )
            {
              printf(
                "[%u]  testing remote MPI worker %u for remote edge to my node %u on stream %lu\n",
                mpiRank,
                commMpiRank,
                (uint32_t)sendBfr[0],
                sendBfr[1] );
            }

            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, 0, MPI_COMM_WORLD ) );

          }
          /* continue main loop as slave */
        }
        else
        {
          currentNode->setCounter( cpCtrId, 1 );
          lastNode    = currentNode;
          currentNode = activityEdge->getStartNode( );
          /* continue main loop as master */
        }
      }
      else
      {
        /* isEnter */
        if ( currentNode->isMPIInit( ) || currentNode->isAtomic( ) )
        {
          currentNode->setCounter( cpCtrId, 1 );

          /* notify all slaves that we are done */
          if ( options.verbose >= VERBOSE_ANNOY )
          {
            printf( "[%u] * asking all slaves to terminate\n", mpiRank );
          }

          sendBfr[0] = 0;
          sendBfr[1] = 0;
          sendBfr[2] = PATH_FOUND_MSG;

          std::set< uint64_t > mpiPartners =
            analysis.getMPIAnalysis( ).getMPICommGroup( 0 ).procs;
          for ( std::set< uint64_t >::const_iterator iter = mpiPartners.begin( );
                iter != mpiPartners.end( ); ++iter )
          {
            int commMpiRank = analysis.getMPIAnalysis( ).getMPIRank( *iter );
            if ( commMpiRank == mpiRank )
            {
              continue;
            }

            MPI_CHECK( MPI_Send( sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                 commMpiRank, 0, MPI_COMM_WORLD ) );
          }

          /* leave main loop */
          break;
        }

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
            /* continue main loop as master */
            break;
          }
        }

        if ( !foundPredecessor )
        {
          throw RTException( "[%u] No ingoing intra-stream edge for node %s",
                             currentNode->getUniqueName( ).c_str( ) );
        }

      }
    }
    else
    {
      /* slave */
      /* int flag = 0; */
      MPI_Status status;
      MPI_CHECK( MPI_Recv( recvBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                           MPI_ANY_SOURCE,
                           MPI_ANY_TAG, MPI_COMM_WORLD, &status ) );

      if ( recvBfr[2] == PATH_FOUND_MSG )
      {
        if ( options.verbose >= VERBOSE_ANNOY )
        {
          printf( "[%u] * terminate requested by master\n", mpiRank );
        }
        break;
      }

      /* find local node for remote node id and decide if we can
       *continue here */
      if ( options.verbose >= VERBOSE_ANNOY )
      {
        printf(
          "[%u]  tested by remote MPI worker %u for remote edge to its node %u on stream %lu\n",
          mpiRank,
          status.MPI_SOURCE,
          (uint32_t)recvBfr[0],
          recvBfr[1] );
      }

      /* check if the activity is a wait state */
      MPIAnalysis::MPIEdge mpiEdge;
      if ( analysis.getMPIAnalysis( ).getRemoteMPIEdge( (uint32_t)recvBfr[0],
                                                        recvBfr[1], mpiEdge ) )
      {
        GraphNode::GraphNodePair& slaveActivity =
          mpiEdge.localNode->getGraphPair( );
        /* we are the new master */

        isMaster       = true;
        slaveActivity.second->setCounter( cpCtrId, 1 );
        lastNode       = slaveActivity.second;
        currentNode    = slaveActivity.first;

        sectionEndNode = lastNode;

        if ( options.verbose >= VERBOSE_ANNOY )
        {
          printf( "[%u] becomes new master at node %s, lastNode = %s\n",
                  mpiRank,
                  currentNode->getUniqueName( ).c_str( ),
                  lastNode->getUniqueName( ).c_str( ) );
        }

        /* continue main loop as master */
      }
    }
  }

  MPI_Barrier( MPI_COMM_WORLD );

}

void
Runner::runAnalysis( Paradigm                          paradigm,
                     EventStream::SortedGraphNodeList& allNodes )
{
  switch ( paradigm )
  {
    case PARADIGM_CUDA:
      if ( mpiRank == 0 )
      {
        printf( "[%u] Running analysis: CUDA\n", mpiRank );
      }

      break;

    case PARADIGM_MPI:
      if ( mpiRank == 0 )
      {
        printf( "[%u] Running analysis: MPI\n", mpiRank );
      }

      break;

    case PARADIGM_OMP:
      if ( mpiRank == 0 )
      {
        printf( "[%u] Running analysis: OMP\n", mpiRank );
      }

      break;

    default:
      if ( mpiRank == 0 )
      {
        printf( "[%u] No analysis for unknown paradigm %d\n",
                mpiRank, paradigm );
      }
      return;
  }

  size_t ctr       = 0, last_ctr = 0;
  size_t num_nodes = allNodes.size( );

  for ( EventStream::SortedGraphNodeList::const_iterator nIter = allNodes.begin( );
        nIter != allNodes.end( ); ++nIter )
  {
    GraphNode* node = *nIter;
    ctr++;

    if ( !( node->getParadigm( ) & paradigm ) )
    {
      continue;
    }

    analysis.applyRules( node, paradigm, options.verbose >= VERBOSE_BASIC );

    if ( ( mpiRank == 0 ) && ( ctr - last_ctr > num_nodes / 10 ) )
    {
      printf( "[%u] %lu%% ", mpiRank,
              (size_t)( 100.0 * (double)ctr / (double)num_nodes ) );
      fflush( NULL );
      last_ctr = ctr;
    }
  }
  if ( mpiRank == 0 )
  {
    printf( "[%u] 100%%\n", mpiRank );
  }

#ifdef DEBUG
  analysis.runSanityCheck( mpiRank );
#endif
}

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
