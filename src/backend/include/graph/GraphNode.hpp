/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2017
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
#include <algorithm> // binary search

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
                Paradigm paradigm, RecordType recordType, int nodeType ) :
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
     ~GraphNode()
     {

     }

     void
     setName( const std::string newName )
     {
       name = newName;
     }

     /**
      * Set the partner node of an enter or leave.
      * 
      * @param partner
      */
     void
     setPartner( GraphNode* partner )
     {
       // set pair so that the first has a smaller time stamp
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
       
       assert( partner->getRecordType() != this->getRecordType() );
     }

     virtual bool
     hasPartner() const
     {
       return pair.first && pair.second;
     }

     /**
      * Get the other part of the activity.
      * @return 
      */
     GraphNode*
     getPartner() const
     {
       if ( isEnter() )
       {
         return pair.second;
       }
       else
       {
         return pair.first;
       }
     }

     GraphNode*
     getCaller() const
     {
       return caller;
     }

     void
     setCaller( GraphNode* caller )
     {
       this->caller = caller;
     }

     GraphNodePair&
     getGraphPair() 
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
     getLinkLeft()
     {
       return linkLeft;
     }

     GraphNode*
     getLinkRight()
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
     getData() const
     {
       return this->data;
     }
     
     // TODO: This function might not be correct implemented.
    static std::vector< GraphNode* >::const_reverse_iterator
    findNode( const GraphNode* node, const std::vector< GraphNode* >& nodes )
    {
      // the vector is empty
      if ( nodes.size() == 0 )
      {
        return nodes.rend();
      }

      // there is only one node in the vector
      if ( nodes.size() == 1 )
      {
        return nodes.rbegin();
      }

      // set start boundaries for the search
      size_t indexMin = 0;
      size_t indexMax = nodes.size() - 1;

      size_t indexPrevMin = indexMin;
      size_t indexPrevMax = indexMax;

      size_t indexPrev = 0;
      size_t indexPrev2 = 0;

      // do a binary search
      do
      {
        indexPrev2 = indexPrev;
        indexPrev = indexPrevMax - ( indexPrevMax - indexPrevMin ) / 2;
        size_t index = indexMax - ( indexMax - indexMin ) / 2;

        assert( index < nodes.size() ); //, "index %lu indexMax %lu indexMin %lu", index, indexMax, indexMin );

        // if we found the node at index ('middle' element)
        // for uneven elements, index points on the element after the half
        if ( nodes[index] == node )
        {
          return nodes.rbegin() + ( nodes.size() - index - 1 );
        }

        // indexMin == indexMax == index
        // only the nodes[index] element was left, which did not match
        // we can leave the loop
        if ( indexMin == indexMax )
        {
          std::cerr << "Stream " << node->getStreamId() << " Looking for node " 
                    << node->getUniqueName() << " - Wrong node found! Index (" 
                    << index << ") node on break: "
                    << nodes[index]->getUniqueName() << std::endl;

          std::cerr << "Node sequence:" << std::endl;
          for(size_t i = index - 3; i < index + 4; i++)
          {
            if( nodes[i] )
              std::cerr << nodes[i]->getUniqueName() << std::endl;
          }

          std::cerr << " Previous compare node [" << indexPrevMin << ":" << indexPrevMax 
                    << "]:" << nodes[indexPrev]->getUniqueName()
                    << " with result: " << Node::compareLess( node, nodes[indexPrev] ) 
                    << std::endl;

          std::cerr << " Pre-Previous compare node: " << nodes[indexPrev2]->getUniqueName()
                    << " with result: " << Node::compareLess( node, nodes[indexPrev2] ) 
                    << std::endl;
          //std::cerr << "return nodes.rbegin() = " << nodes.rbegin() << std::endl;
          //std::cerr << "return nodes.rend() = " << nodes.rend() << std::endl;

          break;
        }

        // use the sorted property of the list to halve the search space
        // if node is before (less) than the node at current index
        // nodes are not the same
        if ( Node::compareLess( node, nodes[index] ) )
        {
          // left side
          indexPrevMax = indexMax;
          indexMax = index - 1;
        }
        else
        {
          // right side
          indexPrevMin = indexMin;
          indexMin = index + 1;
        }

        // if node could not be found
        if ( indexMin > indexMax )
        {
          break;
        }

      }
      while ( true );

      // return iterator to first element, if node could not be found
      return nodes.rend();
    }
    
    /**
     * Binary search for a GraphNode in a vector of GraphNodes based on the node ID.
     * 
     * @param nodeId
     * @param nodes
     * @return 
     */
    static GraphNode*
    findNode( uint64_t nodeId, const std::vector< GraphNode* >& nodes )
    {
      // the vector is empty
      if ( nodes.size() == 0 )
      {
        return NULL;
      }

      // there is only one node in the vector
      if ( nodes.size() == 1 )
      {
        return nodes.front();
      }

      // set start boundaries for the search
      size_t indexMin = 0;
      size_t indexMax = nodes.size() - 1;

      size_t indexPrevMin = indexMin;
      size_t indexPrevMax = indexMax;

      size_t indexPrev = 0;
      size_t indexPrev2 = 0;

      // do a binary search
      do
      {
        indexPrev2 = indexPrev;
        indexPrev = indexPrevMax - ( indexPrevMax - indexPrevMin ) / 2;
        size_t index = indexMax - ( indexMax - indexMin ) / 2;

        assert( index < nodes.size() ); //, "index %lu indexMax %lu indexMin %lu", index, indexMax, indexMin );

        // if we found the node at index ('middle' element)
        // for uneven elements, index points on the element after the half
        if ( nodes[index]->getId() == nodeId )
        {
          return nodes[index];
        }

        // indexMin == indexMax == index
        // only the nodes[index] element was left, which did not match
        // we can leave the loop
        if ( indexMin == indexMax )
        {
          std::cerr << "Looking for node ID " 
                    << nodeId << " - Wrong node found! Index (" 
                    << index << ") node on break: "
                    << nodes[index]->getUniqueName() << std::endl;

          std::cerr << "Node sequence:" << std::endl;
          for(size_t i = index - 3; i < index + 4; i++)
          {
            if( nodes[i] )
              std::cerr << nodes[i]->getUniqueName() << std::endl;
          }

          std::cerr << " Previous compare node [" << indexPrevMin << ":" << indexPrevMax 
                    << "]:" << nodes[indexPrev]->getUniqueName()
                    << " with result: " << ( nodeId < nodes[indexPrev]->getId() )
                    << std::endl;

          std::cerr << " Pre-Previous compare node: " << nodes[indexPrev2]->getUniqueName()
                    << " with result: " << ( nodeId < nodes[indexPrev2]->getId() )
                    << std::endl;
          break;
        }

        // use the sorted property of the list to halve the search space
        // if node is before (less) than the node at current index
        // nodes are not the same
        if ( nodeId < nodes[index]->getId() )
        {
          // left side
          indexPrevMax = indexMax;
          indexMax = index - 1;
        }
        else
        {
          // right side
          indexPrevMin = indexMin;
          indexMin = index + 1;
        }

        // if node could not be found
        if ( indexMin > indexMax )
        {
          break;
        }

      }
      while ( true );

      // return iterator to first element, if node could not be found
      return nodes.front();
    }
    
    /**
     * Binary search for a GraphNode in a list of GraphNodes based on the node ID.
     * 
     * @param nodeId
     * @param nodes
     * @return 
     */
    static bool
    search( const GraphNode* node, const std::list< GraphNode* >& nodes )
    {
      // the list is empty
      if ( nodes.size() == 0 )
      {
        return false;
      }
      
      return std::binary_search( nodes.begin(), nodes.end(), node, Node::compareLess );
    }
    
    /**
     * Binary search for a GraphNode in a vector of GraphNodes based on the time.
     * 
     * @param nodeId
     * @param nodes
     * @return 
     */
    static GraphNode*
    findLastNodeBefore( uint64_t time, const std::vector< GraphNode* >& nodes )
    {
      // the vector is empty
      if ( nodes.size() == 0 )
      {
        return NULL;
      }

      // there is only one node in the vector
      if ( nodes.size() == 1 )
      {
        return nodes.front();
      }

      // set start boundaries for the search
      size_t indexMin = 0;
      size_t indexMax = nodes.size() - 1;
      
      // do a binary search
      do
      {
        size_t index = indexMax - ( indexMax - indexMin ) / 2;
        
        //std::cerr << "Index = " << index << "[" << indexMin << "," << indexMax << "]" << std::endl;

        assert( index < nodes.size() ); //, "index %lu indexMax %lu indexMin %lu", index, indexMax, indexMin );

        // if boundaries are directly next to each other choose the right one
        if( indexMin + 1 == indexMax )
        {
          if( nodes[indexMax]->getTime() <= time )
          {
            return nodes[indexMax];
          }
          else
          {
            return nodes[indexMin];
          }
        }
        // indexMin == indexMax == index
        // last left node has to fit the condition nodes[indexMax]->getTime() <= time
        if ( indexMin == indexMax )
        {
          if( nodes[indexMax]->getTime() <= time )
          {
            return nodes[indexMax];
          }
          else
          {
            std::cerr << "last left node: " << nodes[indexMax]->getUniqueName() << std::endl;
            break;
          }
        }

        // use the sorted property of the list to halve the search space
        // if target time is before the node at current index
        if ( time < nodes[index]->getTime() )
        {
          // choose left side including the index element, which we did not check
          indexMax = index;
        }
        else
        {
          // choose right side including the index element, which we did not check
          indexMin = index; 
        }

        // if node could not be found
        if ( indexMin > indexMax )
        {
          std::cerr << "Node not found! Overlapping indices!" << std::endl;
          break;
        }

      }
      while ( true );

      // return first node, if node could not be found
      std::cerr << "Node not found! Return first node" << std::endl;
      return nodes.front();
    }

   protected:
     GraphNodePair pair; //<! enter, leave node pair
     GraphNode*    linkLeft, * linkRight; //<! link nodes on a stream
     GraphNode*    caller;
     void* data; /**< node specific data pointer */
 };

 typedef GraphNode* GraphNodePtr;
}
