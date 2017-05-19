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

#include <vector>
#include <algorithm>
#include <string>
#include <list>
#include <iostream>
#include <map>
#include <math.h>
#include <mpi.h>

#include "graph/GraphNode.hpp"
#include "graph/Edge.hpp"
#include "common.hpp"

#include <sys/time.h>

/** Number of elements for replayed MPI communication */
#define CASITA_MPI_P2P_BUF_SIZE 5

#define CASITA_MPI_P2P_BUF_LAST 4

/** MPI type of buffer elements */
#if MPI_VERSION < 3
#define CASITA_MPI_P2P_ELEMENT_TYPE MPI_UNSIGNED_LONG_LONG
#else
#define CASITA_MPI_P2P_ELEMENT_TYPE MPI_UINT64_T
#endif

namespace casita
{

 class EventStream
 {
   public:
     
     //!< keep numbers to the power of two, as required by stream type identification
     enum EventStreamType
     {
       ES_OPENMP      = ( 1 << 0 ),
       ES_MPI         = ( 1 << 1 ),
       ES_DEVICE      = ( 1 << 2 ),
       ES_DEVICE_NULL = ( 1 << 3 ),
       
       ES_HOST = ( ES_OPENMP | ES_MPI )
     };

     typedef std::vector< GraphNode* > SortedGraphNodeList;

     typedef bool ( *StreamWalkCallback )( void* userData, GraphNode* node );

   private:

     typedef struct
     {
       GraphNode* firstNode;
       GraphNode* lastNode;
     } GraphData;

   public:

     EventStream( uint64_t id, uint64_t parentId, const std::string name,
                  EventStreamType eventStreamType );

     virtual
     ~EventStream();

     uint64_t
     getId() const;

     uint64_t
     getParentId() const;

     const char*
     getName() const;

     void
     setStreamType( EventStream::EventStreamType type );

     EventStream::EventStreamType
     getStreamType() const;

     bool
     isHostStream() const;
     
     bool
     isMpiStream() const;

     bool
     isDeviceStream() const;

     bool
     isDeviceNullStream() const;
     
     /**
      * Compare function:
      * Sort the streams by stream id, but with host streams first.
      */
     static bool
     streamSort( const EventStream* p1, const EventStream* p2 )
     {
       if ( p1->isDeviceStream() && p2->isHostStream() )
       {
         return false;
       }
        if ( p2->isDeviceStream() && p1->isHostStream() )
       {
         return true;
       }
        return p1->getId() <= p2->getId();
     }
     
     /**
      * Get the stream's first enter and last leave time stamps
      * 
      * @return a pair the first enter and last leave time stamp
      */
     std::pair< uint64_t, uint64_t >&
     getPeriod();
     
     /**
      * Does this stream contains the global first critical node?
      * 
      * @return true, if the critical path starts on this stream
      */
     bool&
     isFirstCritical();
     
     /**
      * Does this stream contains the global last event (of the trace)?
      * 
      * @return true, if the critical path ends on this stream
      */
     bool&
     hasLastGlobalEvent();

     GraphNode*
     getLastNode() const;

     GraphNode*
     getLastNode( Paradigm paradigm ) const;

     GraphNode*
     getLastParadigmNode( Paradigm paradigm ) const;
     
     GraphNode*
     getFirstParadigmNode( Paradigm paradigm ) const;

     /**
      * Get the time stamp of the last event read from an event stream. 
      * (This is often a RMA window destroy event.)
      * 
      * @return time stamp of the last event
      */
     uint64_t
     getLastEventTime() const;
     
     /**
      * Set the time of the last read event. 
      * (This is often a RMA window destroy event.)
      * 
      * @param time time to be set.
      */
     void
     setLastEventTime( uint64_t time );

     void
     addGraphNode( GraphNode* node, GraphNode::ParadigmNodeMap* predNodes );

     void
     insertGraphNode( GraphNode*                  node,
                      GraphNode::ParadigmNodeMap& predNodes,
                      GraphNode::ParadigmNodeMap& nextNodes );

     EventStream::SortedGraphNodeList&
     getNodes();
     
     void
     clearNodes();

     void
     setFilter( bool enable, uint64_t time );
     
     bool
     isFilterOn();
     
     uint64_t&
     getPredictionOffset();

     bool
     walkBackward( GraphNode* node, StreamWalkCallback callback, void* userData );

     bool
     walkForward( GraphNode* node, StreamWalkCallback callback, void* userData );
     
     /**
      * Did the stream change (new nodes added) since the interval start
      * 
      * @return true, if nodes have been added, otherwise false
      */
     bool
     hasNewNodes();
     
     virtual void
     reset();
     
    protected:
      uint64_t id;

    private:
     
      uint64_t            parentId;
      const std::string   name;
      EventStreamType     streamType;
      bool                nodesAdded; //!< has the stream new nodes?

      //!< first enter and last leave time
      std::pair< uint64_t, uint64_t > streamPeriod; 

      //!< Does this stream contain the first (global) critical path node?
      bool                hasFirstCriticalNode;

      //! Does this stream contain the last global event of the trace?
      bool                hasLastEvent;

      //!< pointer to the last node (paradigm independent) of the analysis interval
      GraphNode*          lastNode;

      //! time stamp of the last read event for this stream (e.g. RMA win destroy)
      uint64_t            lastEventTime;

      //<! first and last node of the analysis interval (for each paradigm)
      GraphData           graphData[ NODE_PARADIGM_COUNT ];

      //!< list of nodes in this stream
      SortedGraphNodeList nodes;

      bool                isFiltering;

      //!< time stamp when the filter has been enabled
      uint64_t            filterStartTime;

      //!< time offset due to removal of regions
      uint64_t            predictionOffset;
      
      //!< MPI nodes that have not yet been linked
      SortedGraphNodeList unlinkedMPINodes;

      EventStream::SortedGraphNodeList::const_reverse_iterator
      findNode( GraphNode* node ) const;

      void
      addNodeInternal( SortedGraphNodeList& nodes, GraphNode* node );
 };

}
