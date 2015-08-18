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
 * This class glues the analysis engines for all processes together.
 * - create communicators
 * - create remoteEdges
 *
 */

#include "MPIAnalysis.hpp"
#include "AnalysisEngine.hpp"
#include "common.hpp"

using namespace casita;

MPIAnalysis::MPIAnalysis( uint32_t mpiRank, uint32_t mpiSize ) :
  mpiRank( mpiRank ),
  mpiSize( mpiSize )
{

}

MPIAnalysis::~MPIAnalysis( )
{
  for ( MPICommGroupMap::iterator iter = mpiCommGroupMap.begin( );
        iter != mpiCommGroupMap.end( ); ++iter )
  {
    if ( iter->second.comm != MPI_COMM_NULL && iter->second.comm !=
         MPI_COMM_SELF )
    {
      MPI_CHECK( MPI_Comm_free( &( iter->second.comm ) ) );
    }
  }
}

uint32_t
MPIAnalysis::getMPIRank( ) const
{
  return mpiRank;
}

uint32_t
MPIAnalysis::getMPISize( ) const
{
  return mpiSize;
}

uint32_t
MPIAnalysis::getMPIRank( uint64_t streamId ) const
{
  TokenTokenMap::const_iterator iter = processRankMap.find( streamId );
  if ( iter != processRankMap.end( ) )
  {
    return iter->second;
  }
  else
  {
    throw RTException( "Request for mpi rank with invalid stream ID %u",
                       streamId );
  }
}

uint32_t
MPIAnalysis::getMPIRank( uint64_t            streamId,
                         const MPICommGroup& commGroup ) const
{
  uint32_t ctr = 0;
  for ( std::set< uint64_t >::const_iterator iter = commGroup.procs.begin( );
        iter != commGroup.procs.end( ); ++iter )
  {
    if ( *iter == streamId )
    {
      return ctr;
    }
  }
  throw RTException( "Can not find rank for stream %u in MPI comm group",
                     streamId );
}

void
MPIAnalysis::setMPIRank( uint64_t streamId, uint32_t rank )
{
  processRankMap[streamId] = rank;
}

void
MPIAnalysis::setMPICommGroupMap( uint32_t group, uint32_t numProcs,
                                 const uint64_t* procs )
{
  for ( uint32_t i = 0; i < numProcs; ++i )
  {
    mpiCommGroupMap[group].procs.insert( procs[i] );
  }
  if ( numProcs == 0 )
  {
    mpiCommGroupMap[group].procs.clear( );
  }

}

void
MPIAnalysis::createMPICommunicatorsFromMap( )
{
  for ( MPICommGroupMap::iterator iter = mpiCommGroupMap.begin( );
        iter != mpiCommGroupMap.end( ); ++iter )
  {
    MPICommGroup& group = iter->second;

    int    ranks[group.procs.size( )];
    size_t i = 0;
    for ( std::set< uint64_t >::const_iterator iter = group.procs.begin( );
          iter != group.procs.end( ); ++iter )
    {
      ranks[i] = getMPIRank( *iter );
      ++i;
    }

    MPI_Group worldGroup, commGroup;
    if ( group.procs.empty( ) )
    {
      group.comm = MPI_COMM_SELF;
    }
    else
    {
      MPI_CHECK( MPI_Comm_group( MPI_COMM_WORLD, &worldGroup ) );
      MPI_CHECK( MPI_Group_incl( worldGroup, group.procs.size( ), ranks,
                                 &commGroup ) );
      MPI_CHECK( MPI_Comm_create( MPI_COMM_WORLD, commGroup, &( group.comm ) ) );
      MPI_CHECK( MPI_Group_free( &commGroup ) );
      MPI_CHECK( MPI_Group_free( &worldGroup ) );
    }
  }
}

const MPIAnalysis::MPICommGroup&
MPIAnalysis::getMPICommGroup( uint32_t group ) const
{
  MPICommGroupMap::const_iterator iter = mpiCommGroupMap.find( group );
  if ( iter != mpiCommGroupMap.end( ) )
  {
    return iter->second;
  }
  throw RTException( "Request for unknown MPI comm group %u", group );
}

void
MPIAnalysis::addRemoteMPIEdge( GraphNode*       localNode,
                               uint32_t         remoteNodeID,
                               uint64_t         remoteStreamID,
                               MPIEdgeDirection direction )
{
  MPIEdge edge;
  edge.direction      = direction;
  edge.localNode      = localNode;
  edge.remoteNodeID   = remoteNodeID;
  edge.remoteStreamID = remoteStreamID;
  remoteMpiEdgeMap[remoteStreamID][remoteNodeID] = edge;

  ProcessNodePair pair;
  pair.nodeID         = remoteNodeID;
  pair.streamID       = remoteStreamID;

  reverseRemoteNodeMap[localNode] = pair;
}

bool
MPIAnalysis::getRemoteMPIEdge( uint32_t remoteNodeId, uint64_t remoteProcessId,
                               MPIAnalysis::MPIEdge& edge )
{
  MPIRemoteEdgeMap::const_iterator pIter = remoteMpiEdgeMap.find(
    remoteProcessId );
  if ( pIter != remoteMpiEdgeMap.end( ) )
  {
    MPIIdEdgeMap::const_iterator nIter = pIter->second.find( remoteNodeId );
    if ( nIter != pIter->second.end( ) )
    {
      edge = nIter->second;
      return true;
    }
  }

  return false;
}

MPIAnalysis::ProcessNodePair
MPIAnalysis::getRemoteNodeInfo( GraphNode* localNode, bool* valid )
{
  ReverseRemoteNodeMap::const_iterator iter = reverseRemoteNodeMap.find(
    localNode );
  if ( iter != reverseRemoteNodeMap.end( ) )
  {
    if ( valid )
    {
      *valid = true;
    }
    return iter->second;
  }
  else
  {
    if ( valid )
    {
      *valid = false;
    }
    return MPIAnalysis::ProcessNodePair( );
  }
}

void
MPIAnalysis::reset( )
{
  reverseRemoteNodeMap.clear( );
}

std::set< uint32_t >
MPIAnalysis::getMpiPartnersRank( GraphNode* node )
{
  std::set< uint32_t > partners;

  if ( !node->isMPI( ) )
  {
    return partners;
  }

  if ( node->isEnter( ) )
  {
    node = node->getGraphPair( ).second;
  }

  if ( node->isMPIRecv( ) /*|| node->isMPIISend( ) || node->isMPIIRecv( )*/ )
  {
    partners.insert( getMPIRank( node->getReferencedStreamId( ) ) );
  }

  if ( node->isMPISend( ) )
  {
    partners.insert( getMPIRank( *( (uint64_t*)( node->getData( ) ) ) ) );
  }

  if ( node->isMPICollective( ) || node->isMPIOneToAll( ) ||
       node->isMPIAllToOne( ) )
  {
    uint32_t mpiGroupId = node->getReferencedStreamId( );
    const MPICommGroup& tmpMpiCommGroup = getMPICommGroup( mpiGroupId );
    for ( std::set< uint64_t >::const_iterator iter =
            tmpMpiCommGroup.procs.begin( );
          iter != tmpMpiCommGroup.procs.end( ); ++iter )
    {
      partners.insert( getMPIRank( *iter ) );
    }
  }

  if ( node->isMPISendRecv( ) )
  {
    partners.insert( getMPIRank( node->getReferencedStreamId( ) ) );
    partners.insert( getMPIRank( *( (uint64_t*)( node->getData( ) ) ) ) );
  }

  return partners;
}