/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2018
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "IOffloadRule.hpp"
#include "AnalysisParadigmOffload.hpp"

namespace casita
{
 namespace offload
 {
  class DeviceIdleRule :
    public IOffloadRule
  {
    public:
      /**
       * The device idle rule is triggered at kernel enter and leave nodes.  
       * It requires the kernel execution rule to be triggered and the kernel
       * launch link to be available.
       * 
       * @param priority
       */
      DeviceIdleRule( int priority ) :
        IOffloadRule( "DeviceIdleRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* kernelNode )
      {
        /* set initial idle time value based on the first offload leave
        if ( ofldAnalysis->idle_start_time == 0 && 
             kernelNode->isOffload() && kernelNode->isLeave() )
        {
          ofldAnalysis->idle_start_time = kernelNode->getTime();
          UTILS_OUT( "Setting initial idle time based on %s", 
                     ofldAnalysis->getCommon()->getNodeInfo( kernelNode ).c_str() );
        }*/
        
        // applied at kernel nodes 
        if ( !kernelNode->isOffloadKernel() )
        {
          return false;
        }
        
        // potential idle leave
        if ( kernelNode->isEnter() )
        {
          // ignore initial idle
          if ( ofldAnalysis->idle_start_time == 0 )
          {
            //UTILS_OUT("Ignore initial idle");
            
            // increase device usage count
            ofldAnalysis->active_compute_tasks++;
            
            return true;
          }

          //UTILS_OUT("Device Idle Rule");
          
          // if device was idle
          if ( ofldAnalysis->active_compute_tasks == 0 )
          {
            // start blame distribution on the host
            AnalysisEngine* analysis = ofldAnalysis->getAnalysisEngine();
            /*
            UTILS_OUT( "[DeviceIdleRule] Blame %d host streams for device idle "
                       "before kernel %s", 
                       analysis->getHostStreams().size(),
                       analysis->getNodeInfo( kernelNode ).c_str() );
            */
            // get the kernel launch enter
            GraphNode* launchEnter = (GraphNode*)( kernelNode->getLink() );
            
            uint64_t idleEndTime = kernelNode->getTime();

            // initialize launch time with idle end time (in case we do not get
            // the launch)
            uint64_t launchTime = idleEndTime;
            
            if( launchEnter )
            {
              // get launch (host) stream
              launchTime = launchEnter->getTime();
            }
            else
            {
              UTILS_WARNING( "[DeviceIdleRule] No launch for kernel %s",
                             analysis->getNodeInfo( kernelNode ).c_str() );
            }
            
            // blame is for all host streams the same
            // initially set it to the total idle time
            uint64_t blame = idleEndTime - ofldAnalysis->idle_start_time;
            
            // get last host node for each stream
            for( EventStreamGroup::EventStreamList::const_iterator pIter =
                 analysis->getHostStreams().begin(); 
                 pIter != analysis->getHostStreams().end(); ++pIter )
            {
              EventStream* hostStream = *pIter;
   
              /* create an internal node, which allows for more precise blaming
              GraphNode* virtualNode = analysis->GraphEngine::addNewGraphNode( 
                idleEndTime, hostStream, NULL, PARADIGM_CPU, RECORD_SINGLE, 0 );
              
              uint64_t blame = idleEndTime - ofldAnalysis->idle_start_time;
              distributeBlame( analysis, virtualNode, blame, 
                               streamWalkCallback );*/
              
              uint64_t openRegionTime = 0;
              
              // determine start node for blame distribution
              GraphNode* blameStartNode = NULL;
              if( launchEnter && ( hostStream->getId() == launchEnter->getStreamId() ) )
              {
                // this is the launch from the kernel that ended idle
                blameStartNode = launchEnter;
              }
              else // find a blame start node before the launch that ended idle
              {
                blameStartNode =
                  GraphNode::findLastNodeBefore( launchTime, hostStream->getNodes() );
                
                  // if we start at idleEndTime, we could accidently walk over 
                  // a cuLaunchKernel which would stop blame distribution early
                  //GraphNode::findLastNodeBefore( idleEndTime, hostStream->getNodes() );
                
                // if no start node for blaming was found, continue on next stream
                if( blameStartNode == NULL || 
                    blameStartNode == hostStream->getNodes().front() )
                {
                  UTILS_WARN_ONCE( "[DeviceIdleRule] No blame start node before"
                                   " kernel launch %s on stream %s found!", 
                                   analysis->getNodeInfo( launchEnter ).c_str(),
                                   hostStream->getName() );
                  
                  if( Parser::getOptions().analysisInterval )
                  {
                    UTILS_WARN_ONCE( "This might be due to an intermediate flush." );
                  }
                  
                  continue;
                }
                
                // if accidently a start node after the launch was found (should not happen)
                if ( launchTime < blameStartNode->getTime() )
                {
                  UTILS_WARNING( "[DeviceIdleRule] Error while reading trace! (%s < %s)",
                                 analysis->getNodeInfo( launchEnter ).c_str(),
                                 analysis->getNodeInfo( blameStartNode ).c_str() );
                  //openRegionTime = 0;
                }
                /*else // this is the intended case
                {
                  openRegionTime = launchTime - blameStartNode->getTime();
                }*/
              }
              
              //\todo walk forward until idleEndTime, return new blameStartNode
              // and blame forward list
              
              // set this time initially to the total idle time
              uint64_t totalTimeToBlame = blame;
              
              // start blaming (backwards) from the first launch in the idle phase
              GraphNode* newBlameStartNode = ofldAnalysis->findFirstLaunchInIdle( 
                ofldAnalysis->idle_start_time, blameStartNode );
              if( newBlameStartNode )
              {
                blameStartNode = newBlameStartNode;
              }
              
              // if blame start node is a device synchr. leave or a launch leave, 
              // no walkback is required
              if ( blameStartNode->isLeave() && ( blameStartNode->isOffloadWait() ||
                   blameStartNode->isOffloadEnqueueKernel() ) )
              {
                totalTimeToBlame = openRegionTime;
              }
              else // last node is not a device sync or launch leave
              {
                // regions before the launch should not be blamed for idle time
                // that occurs after the launch
                if( blameStartNode->getTime() > ofldAnalysis->idle_start_time )
                {
                  blame = blameStartNode->getTime() - ofldAnalysis->idle_start_time;
                  
                  //blame at least until the device wait all or kernel launch
                  totalTimeToBlame = 
                    distributeBlame( analysis, blameStartNode, blame, 
                                     streamWalkCallback,
                                     REASON_OFLD_DEVICE_IDLE );
                }
                /*else{
                  UTILS_WARN_ONCE( "[DeviceIdleRule] Blame start node %s is "
                                   "before idle begin at %lf!\n"
                                   "(kernel: %s, initial launch enter: %s)",
                                   analysis->getNodeInfo(blameStartNode).c_str(),
                                   analysis->getRealTime( ofldAnalysis->idle_start_time ),
                                   analysis->getNodeInfo( kernelNode ).c_str(),
                                   analysis->getNodeInfo( launchEnter ).c_str());
                }*/
              }
                    
              //\todo: blame with forward walk if another node is before idle end
              
              // determine remaining blame
              if ( totalTimeToBlame > 0 && openRegionTime > 0 )
              {
                double openBlame = (double) blame
                                 * (double) openRegionTime
                                 / (double) totalTimeToBlame;
                
                // if we have open blame and no kernel launch enter as start node
                if( openBlame > 0 )
                {
                  const Graph::EdgeList* edges =
                    analysis->getGraph().getOutEdgesPtr( blameStartNode );
                  
                  if( edges && edges->size() == 1 )
                  {
                    //UTILS_OUT( "Blame open region with %lf", openBlame );
                    
                    edges->front()->addBlame( openBlame, REASON_OFLD_DEVICE_IDLE );
                  }
                  else
                  {
                    UTILS_WARN_ONCE( "[DeviceIdleRule] %llu open edges at %s. "
                                     "Blame first intra stream edge.",
                                     edges->size(), 
                                     analysis->getNodeInfo( blameStartNode ).c_str() );
                    for ( Graph::EdgeList::const_iterator edgeIter = edges->begin();
                          edgeIter != edges->end(); ++edgeIter)
                    {
                      Edge* edge = *edgeIter;
                      
                      //UTILS_WARNING( "  %s", edge->getName().c_str() );
                      
                      if( edge->isIntraStreamEdge() )
                      {
                        edge->addBlame( openBlame, REASON_OFLD_DEVICE_IDLE );
                        break;
                      }
                    }
                  }
                }
                else
                {
                  UTILS_WARNING( "No blame for open region!" );
                }
              }
            }
          }
          
          // increase device usage count
          ofldAnalysis->active_compute_tasks++;
        }
        else /* kernel leave node */
        {
          // decrease device usage count
          ofldAnalysis->active_compute_tasks--;
          
          // if there are no active tasks, the device is idle
          if ( ofldAnalysis->active_compute_tasks == 0 )
          {
            // remember idle start time (to compute blame for host later)
            ofldAnalysis->idle_start_time = kernelNode->getTime();
          }
          else if ( ofldAnalysis->active_compute_tasks < 0 )
          {
            UTILS_WARNING( "Active device tasks cannot be less than zero!" );
            ofldAnalysis->active_compute_tasks = 0;
          }
            }
        
        return true;
      }
  };
 }
}
