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

#include <list>
#include <set>
#include <map>

#include "Node.hpp"

namespace casita
{
 class GraphNode :
   public Node
 {
   public:

     typedef std::list< GraphNode* > GraphNodeList;
     typedef std::set< GraphNode* > GraphNodeSet;
     typedef std::pair< GraphNode*, GraphNode* > GraphNodePair;
     typedef std::map< Paradigm, GraphNode* > ParadigmNodeMap;

     GraphNode( uint64_t time, uint64_t streamId, const std::string name,
                Paradigm paradigm, NodeRecordType recordType, int nodeType ) :
       Node( time, streamId, name, paradigm, recordType, nodeType ),
       linkLeft( NULL ),
       linkRight( NULL ),
       caller( NULL ),
       data( NULL )
     {
       pair.first  = ( this );
       pair.second = ( NULL );
     }

     virtual
     ~GraphNode( )
     {

     }

     void
     setName( const std::string newName )
     {
       name = newName;
     }

     void
     setPartner( GraphNode* partner )
     {
       if ( partner == NULL || ( this->time < partner->time ) )
       {
         pair.first  = ( this );
         pair.second = ( partner );
       }
       else
       {
         pair.first  = ( partner );
         pair.second = ( this );
       }
     }

     virtual bool
     hasPartner( ) const
     {
       return pair.first && pair.second;
     }

     GraphNode*
     getPartner( ) const
     {
       if ( isEnter( ) )
       {
         return pair.second;
       }
       else
       {
         return pair.first;
       }
     }

     GraphNode*
     getCaller( ) const
     {
       return caller;
     }

     void
     setCaller( GraphNode* caller )
     {
       this->caller = caller;
     }

     GraphNodePair&
     getGraphPair( )
     {
       return pair;
     }

     void
     reduceTimestamp( uint64_t delta )
     {
       this->time -= delta;
     }

     void
     setLinkLeft( GraphNode* linkLeft )
     {
       this->linkLeft = linkLeft;
     }

     void
     setLinkRight( GraphNode* linkRight )
     {
       this->linkRight = linkRight;
     }

     GraphNode*
     getLinkLeft( )
     {
       return linkLeft;
     }

     GraphNode*
     getLinkRight( )
     {
       return linkRight;
     }

     void
     setData( void* value )
     {
       this->data = value;
     }

     /**
      * Return void pointer to node-type-specific data.
      * 
      * @return void pointer to node-type-specific data.
      */
     void*
     getData( ) const
     {
       return this->data;
     }

   protected:
     GraphNodePair pair;
     GraphNode*    linkLeft, * linkRight;
     GraphNode*    caller;
     void* data; /**< node specific data pointer */
 };

 typedef GraphNode* GraphNodePtr;
}
