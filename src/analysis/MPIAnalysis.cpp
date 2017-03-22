/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
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
  globalCollectiveCounter = 0;
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
    throw RTException( "Request for MPI rank with invalid stream ID %u",
                       streamId );
  }
}

/**
 * This routine seems to not work correctly. Check if needed!!!
 * 
 * @param streamId
 * @param commGroup
 * @return 

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
} */

void
MPIAnalysis::setMPIRank( uint64_t streamId, uint32_t rank )
{
  processRankMap[ streamId ] = rank;
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

/**
 * Add an MPI edge from the local node to a remote node that is identified by
 * node ID and stream ID.
 * 
 * @param localNode
 * @param remoteNodeID
 * @param remoteStreamID
 * @param blame
 */
void
MPIAnalysis::addRemoteMPIEdge( GraphNode* localNode,
                               uint64_t   remoteNodeID,
                               uint64_t   remoteStreamID )
{
  RemoteNode rnode;
  rnode.nodeID   = remoteNodeID;
  rnode.streamID = remoteStreamID;

  remoteNodeMap[localNode] = rnode;
}

MPIAnalysis::RemoteNode
MPIAnalysis::getRemoteNodeInfo( GraphNode* localNode, bool* valid )
{
  RemoteNodeMap::const_iterator iter = 
    remoteNodeMap.find( localNode );
  
  // if we found the edge
  if ( iter != remoteNodeMap.end() )
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
    return MPIAnalysis::RemoteNode( );
  }
}

void
MPIAnalysis::removeRemoteNode( GraphNode* localNode )
{
  remoteNodeMap.erase( localNode );
}

/**
 * Reset structures that are local to an interval in the trace.
 */
void
MPIAnalysis::reset( )
{
  // 
  if( remoteNodeMap.size() > 0 ) 
  {
    UTILS_MSG( Parser::getVerboseLevel() >= VERBOSE_BASIC, 
               "[%"PRIu32"] Clear %lu remote nodes. Critical path analysis "
               "might fail otherwise.", mpiRank, remoteNodeMap.size() );
    
    if( Parser::getVerboseLevel() > VERBOSE_BASIC )
    {
      for( RemoteNodeMap::const_iterator it = remoteNodeMap.begin(); 
           it != remoteNodeMap.end(); ++it )
      {
        UTILS_MSG( true, "[%"PRIu32"] Node %s has open remote node %"PRIu64
                         " on stream %"PRIu64, 
                   mpiRank, it->first->getUniqueName().c_str(), 
                   it->second.nodeID, it->second.streamID );
      }
    }
    
    remoteNodeMap.clear();
  }
}

std::set< uint32_t >
MPIAnalysis::getMpiPartnersRanks( GraphNode* node )
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

  if ( node->isMPIRecv() || node->isMPISend() || node->isMPIWait() )
  {
    partners.insert( getMPIRank( node->getReferencedStreamId( ) ) );
  }

  if ( node->isMPICollective( ) /*|| node->isMPIOneToAll( ) ||
       node->isMPIAllToOne( )*/ )
  {
    uint32_t mpiGroupId = node->getReferencedStreamId( );
    const MPICommGroup& tmpMpiCommGroup = getMPICommGroup( mpiGroupId );
    for ( std::set< uint64_t >::const_iterator iter =
            tmpMpiCommGroup.procs.begin();
          iter != tmpMpiCommGroup.procs.end(); ++iter )
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
