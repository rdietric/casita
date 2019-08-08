/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014, 2017, 2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <list>
#include <deque>
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

     //\todo: replace with dequeue
     typedef std::list< GraphNode* > GraphNodeList;
     typedef std::set< GraphNode* > GraphNodeSet;
     typedef std::pair< GraphNode*, GraphNode* > GraphNodePair;
     typedef std::map< Paradigm, GraphNode* > ParadigmNodeMap;

     GraphNode( uint64_t time, uint64_t streamId, const char* name,
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
     
     bool
     isOnCriticalPath() const
     {
       return (bool)( this->getCounter( CRITICAL_PATH ) );
     }
     
     uint64_t
     getWaitingTime() const
     {
       return this->getCounter( WAITING_TIME );
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
     * Binary search for a GraphNode in a list or queue of GraphNodes.
     * 
     * @param node
     * @param nodes
     * @return 
     */
    template <typename LIST_OR_QUEUE>
    static bool
    binarySearch( const GraphNode* node, const LIST_OR_QUEUE& nodes )
    {
      // the list is empty
      if ( nodes.size() == 0 )
      {
        return false;
      }
      
      return std::binary_search( nodes.begin(), nodes.end(), node, Node::compareLess );
    }
    
    /**
     * Linear search for a GraphNode in a list or queue of GraphNodes.
     * 
     * @param node
     * @param nodes
     * @return 
     */
    template <typename LIST_OR_QUEUE>
    static bool
    hasNode( const GraphNode* node, const LIST_OR_QUEUE& nodes )
    {
      if ( std::find( nodes.begin(), nodes.end(), node) != nodes.end() )
      {
        return true;
      }
  
      return false;
    }
    
    template <typename LIST_OR_QUEUE>
    static bool
    findInFirst( const GraphNode* node, const LIST_OR_QUEUE& nodes, short num )
    {
      short count = 0;
      for( typename LIST_OR_QUEUE::const_iterator it = nodes.begin(); it != nodes.end();
           ++it )
      {
        if( count == num )
        {
          return false;
        }
        
        GraphNode* g = *it;
        if( g->getId() == node->getId() )
        {
          return true;
        }
          
        count++;
      }
      
      return false;
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
      
      GraphNode* found = NULL;

      // set start boundaries for the search
      size_t indexMin = 0;
      size_t indexMax = nodes.size() - 1;
      
      size_t index = 0;
      
      // do a binary search
      do
      {
        index = indexMax - ( indexMax - indexMin ) / 2;
        
        //std::cerr << "Index = " << index << "[" << indexMin << "," << indexMax << "]" << std::endl;

        assert( index < nodes.size() ); //, "index %lu indexMax %lu indexMin %lu", index, indexMax, indexMin );

        // if boundaries are directly next to each other choose the right one
        if( indexMin + 1 == indexMax )
        {
          if( nodes[indexMax]->getTime() <= time )
          {
            //return nodes[indexMax];
            found = nodes[indexMax];
            index = indexMax;
            break;
          }
          else
          {
            //return nodes[indexMin];
            found = nodes[indexMin];
            index = indexMin;
            break;
          }
        }
        // indexMin == indexMax == index
        // last left node has to fit the condition nodes[indexMax]->getTime() <= time
        if ( indexMin == indexMax )
        {
          if( nodes[indexMax]->getTime() <= time )
          {
            //return nodes[indexMax];
            found = nodes[indexMax];
            index = indexMax;
            break;
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
      
      // if the found node does not fulfil the condition
      if( found )
      {
        if( found->getTime() > time )
        {
          std::cerr << "Binary search failed! Wrong node at index " << index << " found: " 
                    << found->getUniqueName() << " (" << nodes.size() << " nodes)" 
                    << std::endl;
          /*if(index > 0)
            std::cerr << "Node before: " << nodes[index-1]->getUniqueName() << std::endl;
          if(index+1 < nodes.size())
            std::cerr << " Node after: " << nodes[index+1]->getUniqueName() << std::endl;
          */
          
          // try to find a node before the found one that satisfies the condition
          while( index > 0 )
          {
            if( nodes[index]->getTime() <= time )
            {
              return nodes[index];
            }
            index--;
          }

          std::cerr << "Return node at index " << index << ": " 
                    << nodes[index]->getUniqueName() << std::endl;

          return nodes[index];
        }
        else
        {
          return found;
        }
      }

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
 
 typedef std::deque< GraphNode* > GraphNodeQueue;
}
