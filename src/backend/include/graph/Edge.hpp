/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <sstream>
#include <map>

#include "GraphNode.hpp"
#include "utils/ErrorUtils.hpp"

namespace casita
{
  
 typedef std::map< BlameReason, double > BlameMap;
  
 class Edge
 {
   public:
     Edge( GraphNode* start, GraphNode* end, uint64_t duration,
           bool blocking, Paradigm edgeParadigm ) :
       startNode( start ),
       endNode( end ),
       blocking( blocking ),
       edgeParadigm( edgeParadigm )//,
       //blame ( 0 )
     {
       if ( isReverseEdge() )
       {
         blocking = true;
         duration = 0;
       }

       this->edgeDuration = duration;      
     }

     bool
     hasEdgeType( Paradigm edgeParadigm ) const
     {
       return edgeParadigm & this->edgeParadigm;
     }

     uint64_t
     getDuration() const
     {
       return this->edgeDuration;
     }

     uint64_t
     getWeight() const
     {
       return computeWeight( this->edgeDuration, this->blocking );
     }

     const std::string
     getName() const
     {
       std::stringstream name;
       
       if( startNode != NULL )
       {
         name << "[" << startNode->getUniqueName() << ", ";
       }
       
       if( endNode != NULL )
       {
         name << endNode->getUniqueName() << ", (";
       }

       if ( edgeParadigm == PARADIGM_ALL )
       {
         name << "ALL";
       }
       else
       {
         if ( edgeParadigm & PARADIGM_CUDA )
         {
           name << "CUDA";
         }
         if ( edgeParadigm & PARADIGM_MPI )
         {
           name << ",MPI";
         }
         if ( edgeParadigm & PARADIGM_OMP )
         {
           name << ",OMP";
         }
       }

       name << ") ";

       if ( isInterProcessEdge() )
       {
         name << "(inter)";
       }
       else
       {
         name << "(intra)";
       }
       
       if( blocking )
       {
         name << " is blocking";
       }
       
       if( isReverseEdge() )
       {
         name << " is reverse edge";
       }

       name << "]";
       
       return name.str();
     }

     bool
     isBlocking() const
     {
       return this->blocking;
     }

     void
     makeBlocking()
     {
       this->blocking = true;
     }
     
     void
     unblock()
     {
       this->blocking = false;
     }

     GraphNode*
     getStartNode() const
     {
       return startNode;
     }

     GraphNode*
     getEndNode() const
     {
       return endNode;
     }

     bool
     isReverseEdge() const
     {
       return startNode->getTime() > endNode->getTime();
     }

     bool
     isIntraStreamEdge() const
     {
       return startNode->getStreamId() == endNode->getStreamId();
     }

     bool
     isInterProcessEdge() const
     {
       return startNode->getStreamId() != endNode->getStreamId();
     }
          
     void addBlame( double value, BlameReason type = REASON_UNCLASSIFIED )
     {
       BlameMap::iterator blameIt = this->blame.find( type );
       if ( blameIt == this->blame.end() )
       {
         this->blame[ type ] = value;
       }
       else
       {
         blameIt->second += value;
       }
     }
     
     double getBlame( BlameReason type = REASON_UNCLASSIFIED )
     {
       BlameMap::const_iterator iter = this->blame.find( type );
       if ( iter == this->blame.end() )
       {
         return 0;
       }
       else
       {
         return iter->second;
       }
     }
     
     /**
      * Get the accumulated blame, independent of the blame reason.
      * @return 
      */
     double getTotalBlame()
     {
       double total_blame = 0;
       for( BlameMap::const_iterator it = this->blame.begin();
           it != this->blame.end(); it++)
       {
         total_blame += it->second;
       }
       return total_blame;
     }
     
     /**
      * Return the address of the blame map.
      * 
      * @return address of the blame map
      */
     BlameMap* getBlameMap()
     {
       return &(this->blame);
     }

   private:
     GraphNode* startNode;
     GraphNode* endNode;
     
     uint64_t edgeDuration;
     bool     blocking;
     Paradigm edgeParadigm;

     //double blame;
     
     //!< blame type, blame value
     BlameMap blame;

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

         return std::numeric_limits< uint64_t >::max() - tmpDuration;
       }
       else
       {
         return std::numeric_limits< uint64_t >::max();
       }

     }
 };

}
