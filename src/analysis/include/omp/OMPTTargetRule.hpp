/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "IOMPRule.hpp"
#include "AnalysisParadigmOMP.hpp"

namespace casita
{
  namespace omp
  {
    class OMPTTargetRule : 
      public IOMPRule
    {
      public:

      OMPTTargetRule( int priority ) : 
        IOMPRule( "OMPTTargetRule", priority )
      {

      }

      private:
        
      bool
      apply( AnalysisParadigmOMP* ompAnalysis, GraphNode* node )
      {
        AnalysisEngine* analysis = ompAnalysis->getCommon();
        
        // remember enter node
        if( node->isEnter() )
        {
          EventStream* nodeStream  = analysis->getStream( node->getStreamId() );
          
          // set target enter node on the node's host stream
          if( node->isOMPTarget() )
          {
            ompAnalysis->setTargetEnter( node );
          }
          // if it is a device enter node
          else if ( nodeStream->getStreamType() == EventStream::ES_DEVICE )
          {
            // if this is the first event in the target region on the node's stream
            if( ompAnalysis->isFirstTargetOffloadEvent( node->getStreamId() ) )
            {
              ompAnalysis->setTargetOffloadFirstEvent( node );
              
              // get corresponding target enter
              GraphNode* targetEnter = 
                ompAnalysis->getTargetEnter( nodeStream->getDeviceId() );
              
              if( targetEnter )
              {
                // add dependency edge
                analysis->newEdge( targetEnter, node, EDGE_NONE );
              }
              else
              {
                UTILS_WARNING( "Could not create edge from target enter to %s ", 
                               node->getUniqueName().c_str() );
                return false;
              }
            }
          }
        }
        
        // start the analysis on the leave node
        else if( node->isLeave() )
        {
          if( node->isOMPTarget() )
          {
            GraphNode* targetEnter = node->getGraphPair().first;
            int deviceId = -1;
            if( targetEnter->getData() )
            {
              deviceId = targetEnter->getReferencedStreamId();
            }
            
            EventStreamGroup::EventStreamList deviceStreams;
            analysis->getDeviceStreams( deviceId , deviceStreams );

            // get last node on the device before target leave
            for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                  deviceStreams.begin( );
                pIter != deviceStreams.end( ); ++pIter )
            {
              EventStream* deviceStream = *pIter;

              GraphNode* lastOffloadNode = 
                GraphNode::findLastNodeBefore( node->getTime(), deviceStream->getNodes() );
              
              //UTILS_MSG( true, "Last offload node: %s < %lf", 
              //           analysis->getNodeInfo( lastOffloadNode ).c_str(),
              //           analysis->getRealTime( node->getTime() ) );
              
              analysis->newEdge( lastOffloadNode->getGraphPair().second, node, EDGE_NONE );
              
              // find last leave of closing target region?
            }
            
            // reset target enter node for next target region
            ompAnalysis->consumeOmpTargetBegin( node->getStreamId() );          
          }
          /* if it is a device enter node
          else if ( nodeStream->getStreamType( ) == EventStream::ES_DEVICE )
          {
            
          }*/
        }
        
        return true;
      }
    };
  }
}
