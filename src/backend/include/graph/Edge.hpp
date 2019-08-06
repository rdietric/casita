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
       paradigm( edgeParadigm )//,
       //blame ( 0 )
     {
       if ( isReverseEdge() )
       {
         blocking = true;
         duration = 0;
       }

       this->duration = duration;      
     }

     bool
     hasEdgeType( Paradigm edgeParadigm ) const
     {
       return edgeParadigm & this->paradigm;
     }

     uint64_t
     getDuration() const
     {
       return this->duration;
     }

     uint64_t
     getWeight() const
     {
       return computeWeight( this->duration, this->blocking );
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

       if ( paradigm == PARADIGM_ALL )
       {
         name << "ALL";
       }
       else
       {
         if ( paradigm & PARADIGM_CUDA )
         {
           name << "CUDA";
         }
         if ( paradigm & PARADIGM_MPI )
         {
           if( paradigm != PARADIGM_MPI )
           {
             name << ",";
           }
           name << "MPI";
         }
         if ( paradigm & PARADIGM_OMP )
         {
           if( paradigm != PARADIGM_OMP )
           {
             name << ",";
           }
           name << "OMP";
         }
       }

       name << ") ";

       if ( isInterStreamEdge() )
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
       
       // reset duration
       if( this->duration == 0)
       {
         if( startNode->getTime() > endNode->getTime() )
         {
           this->duration = startNode->getTime() - endNode->getTime();
         }
         else
         {
           this->duration = endNode->getTime() - startNode->getTime();
         }
       }
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
     isInterStreamEdge() const
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
     
     uint64_t duration;
     bool     blocking;
     Paradigm paradigm;

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
