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

#include <sstream>
#include <limits>
#include <map>

#include "GraphNode.hpp"
#include "utils/ErrorUtils.hpp"

namespace casita
{

 enum EdgeProperties
 {
   EDGE_NONE             = 0,
   EDGE_IS_BLOCKING      = ( 1 << 0 ),
   EDGE_CAUSES_WAITSTATE = ( 1 << 1 )
 };

 class Edge
 {
   public:
     typedef std::map< uint32_t, uint64_t > TimeProfileMap;
     typedef std::map< Paradigm, Edge* > ParadigmEdgeMap;
     typedef struct
     {
       uint64_t startTime;
       uint64_t endTime;
     } cpuStartEndTime;

     Edge( GraphNode* start, GraphNode* end, uint64_t duration,
           int properties, Paradigm edgeParadigm ) :
       properties( EDGE_NONE )
     {
       pair                  = std::make_pair( start, end );
       if ( isReverseEdge( ) )
       {
         properties |= EDGE_IS_BLOCKING;
         duration    = 0;
       }

       cpuNodes              = 0;
       cpuBlame              = 0;
       /* set both to end->getTime() for initial CPU time to be 0 */
       cpuStartEnd.startTime = end->getTime( );
       cpuStartEnd.endTime   = end->getTime( );

       this->properties      = properties;
       this->edgeDuration    = duration;
       this->initialDuration = duration;
       this->edgeWeight      = computeWeight( duration, isBlocking( ) );
       this->edgeParadigm    = edgeParadigm;
       this->timeProfile     = NULL;
     }

     bool
     hasEdgeType( Paradigm edgeParadigm ) const
     {
       return edgeParadigm & this->edgeParadigm;
     }

     Paradigm
     getEdgeType( ) const
     {
       return edgeParadigm;
     }

     uint64_t
     getDuration( ) const
     {
       return edgeDuration;
     }

     uint64_t
     getInitialDuration( ) const
     {
       return initialDuration;
     }

     uint64_t
     getWeight( ) const
     {
       return edgeWeight;
     }

     const std::string
     getName( ) const
     {
       std::stringstream name;
       name << "[" << pair.first->getUniqueName( ).c_str( ) << ", " <<
       pair.second->getUniqueName( ).c_str( ) << ", (";

       if ( edgeParadigm == PARADIGM_ALL )
       {
         name << "ALL";
       }
       else
       {
         if ( edgeParadigm & PARADIGM_CUDA )
         {
           name << "CUDA,";
         }
         if ( edgeParadigm & PARADIGM_MPI )
         {
           name << "MPI,";
         }
         if ( edgeParadigm & PARADIGM_OMP )
         {
           name << "OMP,";
         }
         if ( edgeParadigm & PARADIGM_CPU )
         {
           name << "CPU,";
         }
       }

       name << ") ";

       if ( isInterProcessEdge( ) )
       {
         name << "(inter)]";
       }
       else
       {
         name << "(intra)]";
       }

       return name.str( );
     }

     void
     setDuration( uint64_t duration )
     {
       edgeDuration = duration;
       edgeWeight   = computeWeight( edgeDuration, isBlocking( ) );
     }

     bool
     isBlocking( ) const
     {
       return properties & EDGE_IS_BLOCKING;
     }

     void
     makeBlocking( )
     {
       edgeWeight  = std::numeric_limits< uint64_t >::max( );
       properties |= EDGE_IS_BLOCKING;
     }

     bool
     causesWaitState( ) const
     {
       return properties & EDGE_CAUSES_WAITSTATE;
     }

     GraphNode*
     getStartNode( ) const
     {
       return pair.first;
     }

     GraphNode*
     getEndNode( ) const
     {
       return pair.second;
     }

     GraphNode::GraphNodePair&
     getNodes( )
     {
       return pair;
     }

     bool
     isReverseEdge( ) const
     {
       return pair.first->getTime( ) > pair.second->getTime( );
     }

     bool
     isIntraStreamEdge( ) const
     {
       return pair.first->getStreamId( ) == pair.second->getStreamId( );
     }

     bool
     isInterProcessEdge( ) const
     {
       return pair.first->getStreamId( ) != pair.second->getStreamId( );
     }

     TimeProfileMap*
     getTimeProfile( )
     {
       return timeProfile;
     }

     void
     addCPUData( uint32_t nodes, uint64_t startTime, uint64_t endTime )
     {
       UTILS_ASSERT( cpuNodes == 0, "Can not set CPU data multiple times" );

       if ( nodes > 0 )
       {
         cpuNodes = nodes;
         cpuStartEnd.startTime = startTime;
         cpuStartEnd.endTime = endTime;
       }
     }

     uint64_t
     getCPUNodesStartTime( )
     {
       return cpuStartEnd.startTime;
     }

     uint64_t
     getCPUNodesEndTime( )
     {
       return cpuStartEnd.endTime;
     }

     uint32_t
     getNumberOfCPUNodes( )
     {
       return cpuNodes;
     }

     void
     addCPUBlame( uint64_t blame )
     {
       cpuBlame += blame;
     }

     uint64_t
     getCPUBlame( )
     {
       return cpuBlame;
     }

   private:
     int      properties;
     uint64_t edgeDuration;
     uint64_t initialDuration;
     uint64_t edgeWeight;
     Paradigm edgeParadigm;
     GraphNode::GraphNodePair pair;
     TimeProfileMap* timeProfile;

     uint32_t cpuNodes;
     cpuStartEndTime cpuStartEnd;
     uint64_t cpuBlame;

     void
     setWeight( uint64_t weight )
     {
       edgeWeight = weight;
     }

     static uint64_t
     computeWeight( uint64_t duration, bool blocking )
     {
       if ( !blocking )
       {
         uint64_t tmpDuration = duration;
         if ( tmpDuration == 0 )
         {
           tmpDuration = 1;
         }

         return std::numeric_limits< uint64_t >::max( ) - tmpDuration;
       }
       else
       {
         return std::numeric_limits< uint64_t >::max( );
       }

     }
 };

}
