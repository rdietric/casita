/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "../IOffloadRule.hpp"
#include "../AnalysisParadigmOffload.hpp"

namespace casita
{
 namespace offload
 {
  class StreamWaitRule :
    public IOffloadRule
  {
    public:

      /**
       * This rule is applied at CUDA stream wait event and CUDA kernel leave nodes.
       * @param priority
       */
      StreamWaitRule( int priority ) :
        IOffloadRule( "StreamWaitRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* node )
      {
        if ( !node->isLeave() )
        {
          return false;
        }

        AnalysisEngine* analysis = ofldAnalysis->getAnalysisEngine();

        ////////////////////////////////////////////////////////////////////////
        // applied at streamWaitEvent leave
        if ( node->isEventNode() && node->isCUDAStreamWaitEvent() )
        {
          // get referenced device stream
          uint64_t referencedDevWaitProc = node->getReferencedStreamId();
          if ( !referencedDevWaitProc )
          {
            throw RTException(
                    "Stream wait %s does not reference any device stream",
                    node->getUniqueName().c_str() );
          }

          
          // link the stream wait event leave node with the event record leave node
          EventNode* swEventNode = (EventNode*)node;
          swEventNode->setLink( ofldAnalysis->getEventRecordLeave( 
                                  swEventNode->getEventId() ) );

          uint64_t eventProcessId = ofldAnalysis->getEventProcessId(
            swEventNode->getEventId() );
          if ( !eventProcessId )
          {
            throw RTException(
                    "Could not find device stream ID for event %" PRIu64 " from %s",
                    swEventNode->getEventId(),
                    swEventNode->getUniqueName().c_str() );
          }

          if ( swEventNode->getLink() &&
               ( referencedDevWaitProc != eventProcessId ) )
          {
            ofldAnalysis->addStreamWaitEvent( referencedDevWaitProc, swEventNode );
          }
          else
          {
            /* \todo: found unnecessary streamWaitEvent call */
            /* we could blame this here */
            /* UTILS_DBG_MSG(" * Ignoring unnecessary stream wait event node
             * %s", eNode->getUniqueName().c_str()); */
          }
          return true;
        }

        ////////////////////////////////////////////////////////////////////////
        // applied at kernel leave
        if ( node->isCUDAKernel() )
        {
          GraphNode* waitingKernelEnter = node->getGraphPair().first;

          bool ruleMatched = false;
          bool insertWaitState = false;
          std::set< GraphNode* > processedSyncKernelLeaves;

          uint64_t waitStateEnterTime = std::numeric_limits< uint64_t >::max();
          
          GraphNode* lastSyncKernelLeave = NULL;

          // get launch for this (waiting) kernel
          // the link is set in KernelExecutionRule
          GraphNode* waitingKernelLaunchEnter = (GraphNode*) waitingKernelEnter->getLink();
          if ( !waitingKernelLaunchEnter )
          {
            UTILS_OUT( "[%u] Applying StreamWaitRule failed. "
                       "Kernel %s has no matching kernel launch", 
                       analysis->getMPIRank(),
                       waitingKernelEnter->getUniqueName().c_str() );
            
            return false;
          }

          // We have to manage all streamWaitEvents that may reference this 
          // kernel's device stream, processed in chronological order (oldest first).
          while ( true )
          {
            // find the oldest streamWaitEvent that references this (waiting) device stream
            EventNode* streamWaitLeave = 
                      ofldAnalysis->getFirstStreamWaitEvent( node->getStreamId() );
            if ( !streamWaitLeave )
            {
              break;
            }

            // if the streamWaitEvent is after this (waiting) kernel was launched,
            // it's the wrong streamWaitEvent and we stop processing streamWaitEvents.
            GraphNode* streamWaitEnter = streamWaitLeave->getGraphPair().first;
            if ( streamWaitEnter->getTime() > waitingKernelLaunchEnter->getTime() )
            {
              break;
            }

            // remove from queue
            ofldAnalysis->consumeFirstStreamWaitEvent( node->getStreamId() );

            // find the eventLaunch for this event
            EventNode* eventLaunchLeave = (EventNode*)streamWaitLeave->getLink();
            if ( !eventLaunchLeave )
            {
              UTILS_OUT( " * Ignoring stream wait event %s without "
                         "matching event record for event %" PRIu64 " \n",
                         streamWaitLeave->getUniqueName().c_str(),
                         streamWaitLeave->getEventId() );
              break;
            }

            // find the device stream where the event of streamWaitEvent is enqueued
            uint64_t swEventRefDevProc = eventLaunchLeave->getReferencedStreamId();
            if ( !swEventRefDevProc )
            {
              break;
            }

            // find closest kernelLaunch leave before this eventLaunch
            GraphNode* launchLeave  = (GraphNode*)eventLaunchLeave->getLink();
            if ( !launchLeave )
            {
              break;
            }

            GraphNode* syncKernelEnter =
              (GraphNode*)launchLeave->getGraphPair().first->getLink();
            if ( !syncKernelEnter )
            {
              ErrorUtils::getInstance().throwError(
                "Depending kernel %s (%f) started before kernel from %s (%f) started"
                " (event id = %" PRIu64 ", recorded at %f, streamWaitEvent %s)",
                node->getUniqueName().c_str(),
                analysis->getRealTime( node->getTime() ),
                launchLeave->getUniqueName().c_str(),
                analysis->getRealTime( launchLeave->getTime() ),
                streamWaitLeave->getEventId(),
                analysis->getRealTime( eventLaunchLeave->getTime() ),
                streamWaitLeave->getUniqueName().c_str() );
              return false;
            }

            GraphNode* syncKernelLeave = syncKernelEnter->getGraphPair().second;
            if ( !syncKernelLeave )
            {
              ErrorUtils::getInstance().throwError(
                "Depending kernel %s (%f) started before kernel from %s (%f) finished",
                node->getUniqueName().c_str(),
                analysis->getRealTime( node->getTime() ),
                launchLeave->getUniqueName().c_str(),
                analysis->getRealTime( launchLeave->getTime() ) );
              return false;
            }

            // do not add multiple dependencies to the same (sync) kernel
            if ( processedSyncKernelLeaves.find( syncKernelLeave ) !=
                 processedSyncKernelLeaves.end() )
            {
              break;
            }

            processedSyncKernelLeaves.insert( syncKernelLeave );

            // add dependency
            analysis->newEdge( syncKernelLeave, waitingKernelEnter );

            // insert wait state only if launch of next (waiting) kernel is 
            // before the blocking kernel finishes
            if ( waitingKernelLaunchEnter->getTime() <
                 syncKernelLeave->getTime() )
            {
              //syncKernelLeave->incCounter( BLAME, 
              //  syncKernelLeave->getTime() - waitingKernelLaunchEnter->getTime() );
              
              Edge* syncKernelEdge = 
                analysis->getEdge( syncKernelEnter, syncKernelLeave );
              if( syncKernelEdge )
              {
                syncKernelEdge->addBlame( syncKernelLeave->getTime() - waitingKernelLaunchEnter->getTime() );
              }
              else
              {
                UTILS_WARNING( "CUDA StreamWaitRule: Could not find kernel edge %s -> %s",
                               analysis->getNodeInfo( syncKernelEnter ).c_str(),
                               analysis->getNodeInfo( syncKernelLeave ).c_str() );
              }

              waitStateEnterTime = std::min( waitStateEnterTime,
                                             waitingKernelLaunchEnter->getTime() );
              if ( !lastSyncKernelLeave ||
                   ( syncKernelLeave->getTime() > lastSyncKernelLeave->getTime() ) )
              {
                lastSyncKernelLeave = syncKernelLeave;
              }

              insertWaitState    = true;
            }

            ruleMatched = true;
          }

          if ( insertWaitState )
          {
            /* get last leave node on this device stream */
            EventStream* waitingDevProc = analysis->getStream(
              node->getStreamId() );
            EventStream::SortedGraphNodeList& nodes = waitingDevProc->getNodes();

            GraphNode* lastLeaveNode  = NULL;
            for ( EventStream::SortedGraphNodeList::const_reverse_iterator
                  rIter
                    =
                      nodes.rbegin();
                  rIter != nodes.rend(); ++rIter )
            {
              GraphNode* n = ( *rIter );
              if ( n->isMPI() )
              {
                continue;
              }

              if ( n->getTime() <= node->getTime() && n != node &&
                   n->isLeave() && n->isCUDAKernel() )
              {
                uint64_t lastLeaveNodeTime = n->getTime();
                lastLeaveNode = n;
                if ( lastLeaveNodeTime > waitStateEnterTime )
                {
                  waitStateEnterTime = lastLeaveNodeTime;
                }
                break;
              }
            }

            /* add wait state if a preceeding leave node in this stream has
             * been found which ends earlier than the current kernel started */
            if ( lastLeaveNode &&
                 ( waitStateEnterTime < waitingKernelEnter->getTime() ) )
            {
              Edge* kernelKernelEdge = 
                analysis->getEdge( lastLeaveNode, waitingKernelEnter );
              
              if ( !kernelKernelEdge )
              {
                ErrorUtils::getInstance().throwError(
                  "Did not find expected edge [%s (p %u), %s (p %u)]",
                  lastLeaveNode->getUniqueName().c_str(),
                  lastLeaveNode->getStreamId(),
                  waitingKernelEnter->getUniqueName().c_str(),
                  waitingKernelEnter->getStreamId() );
                return false;
              }

              FunctionDescriptor functionDesc;
              functionDesc.paradigm = PARADIGM_CUDA;
              functionDesc.functionType = OFLD_WAITSTATE;
              functionDesc.recordType = RECORD_ENTER;
              
              GraphNode* waitEnter = analysis->addNewGraphNode(
                waitStateEnterTime,
                waitingDevProc,
                NAME_WAITSTATE,
                &functionDesc );
              
              functionDesc.recordType = RECORD_LEAVE;
              GraphNode* waitLeave = analysis->addNewGraphNode(
                lastSyncKernelLeave->getTime(),
                waitingDevProc,
                NAME_WAITSTATE,
                &functionDesc);

              analysis->newEdge( lastLeaveNode, waitEnter );
              analysis->newEdge( waitEnter, waitLeave, true );
              analysis->newEdge( waitLeave, waitingKernelEnter );

              // set counters
              waitLeave->setCounter( WAITING_TIME, 1 );

              // add dependency to all sync kernel leave nodes
              // some dependencies can become reverse edges during optimization
              for ( std::set< GraphNode* >::const_iterator gIter =
                      processedSyncKernelLeaves.begin();
                    gIter != processedSyncKernelLeaves.end(); ++gIter )
              {
                //\todo check if this should have EDGE_CAUSES_WAITSTATE property
                analysis->newEdge( *gIter, waitLeave );
              }
            }
          }

          return ruleMatched;
        }

        return false;
      }
  };
 }
}
