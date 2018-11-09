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
  class KernelOverlapRule :
    public IOffloadRule
  {
    public:
      /**
       * The kernel overlap rule is triggered at kernel enter and leave nodes.
       * It requires the DeviceIdleRule to be already executed, as active tasks
       * are evaluated.
       * 
       * The rule tries to create edges between (shortly) overlapping kernels 
       * to get more reasonable critical path results.
       * 
       * This rule uses a look ahead!
       * 
       * (Remember that critical path detection is backwards in time and this
       * rule is applied forward in time!)
       * 
       * @param priority
       */
      KernelOverlapRule( int priority ) :
        IOffloadRule( "KernelOverlapRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOffload* ofldAnalysis, GraphNode* kernelNode )
      {
        // applied at kernel nodes
        if ( !kernelNode->isOffloadKernel() )
        {
          return false;
        }
        
        if ( kernelNode->isLeave() )
        {
          // return, if no overlapping kernel is available
          GraphNode *oKernelEnter = ofldAnalysis->oKernelEnter;
          if( !oKernelEnter )
          {
            return true;
          }
          
          GraphNode *kernelEnter = kernelNode->getGraphPair().first;
          if( !kernelEnter )
          {
            UTILS_OUT( "[KernelOverlapRule] Kernel enter is NULL." );
            return false;
          }

          GraphNode *oKernelLeave = oKernelEnter->getGraphPair().second;
          if( !oKernelLeave )
          {
            UTILS_OUT( "[KernelOverlapRule] Overlapping kernel leave is NULL." );
            return false;
          }

          uint64_t oKernelStartTime = oKernelEnter->getTime();
          uint64_t oKernelEndTime = oKernelLeave->getTime();
          uint64_t kernelStartTime = kernelEnter->getTime();
          uint64_t kernelEndTime = kernelNode->getTime();
          
          
          // if current kernel starts earlier and ends before overlapping kernel
          if( kernelStartTime < oKernelStartTime && // current kernel starts before overlapping kernel starts
              kernelEndTime > oKernelStartTime && // may be unnecessary
              kernelEndTime < oKernelEndTime ) // current kernel ends before overlapping kernel ends
          {
            uint64_t overlapTime = kernelEndTime - oKernelStartTime;
            uint64_t kernelNoOverlap = oKernelStartTime - kernelStartTime;

            // if the overlapping part is less than the execution time of 
            // the not overlapping part of the overlapping kernel
            if( overlapTime < kernelNoOverlap )
            {
              // criteria: 1 ms between launch leave and kernel start
              GraphNode* oLaunchEnter = (GraphNode*)( kernelEnter->getLink() );
              if( !oLaunchEnter )
              {
                UTILS_OUT( "[KernelOverlapRule] Overlapping kernel launch enter"
                           " is NULL." );
                return false;
              }
              
              GraphNode* oLaunchLeave = oLaunchEnter->getGraphPair().second;
              if( !oLaunchLeave )
              {
                UTILS_OUT( "[KernelOverlapRule] Overlapping kernel launch leave"
                           " is NULL." );
                return false;
              }
              
              // check whether the current kernel delayed the overlapping kernel
              uint64_t oLaunchEndTime = oLaunchLeave->getTime();
              if( oKernelStartTime > oLaunchEndTime )
              {
                AnalysisEngine* analysis = ofldAnalysis->getCommon();
                double oKernelStartDelay = 
                  analysis->getRealTime( oKernelStartTime - oLaunchEndTime );
                
                /*UTILS_MSG( analysis->getMPIRank() == 0 && 
                           analysis->getRealTime( kernelNode->getTime() ) > 53.328 &&
                           analysis->getRealTime( kernelNode->getTime() ) < 53.329,
                           "Kernel leave: %s",
                           analysis->getNodeInfo( kernelNode ).c_str() );*/

                // \todo: if the startup delay is more than 500us, 
                //        a delayed start is assumed
                if( oKernelStartDelay > 0.0005 )
                {
                  bool addEdge = false;

                  // check whether we should link the overlapping kernel as a 
                  // better choice than the currently linked one
                  GraphNode* linkedKernel = oKernelEnter->getLinkLeft();
                  if( linkedKernel )
                  {
                    // ensure that we have a linked leave node
                    if( !linkedKernel->isLeave() )
                    {
                      linkedKernel = linkedKernel->getGraphPair().second;
                    }
                    
                    // if currently linked node also overlaps
                    if( linkedKernel && ( linkedKernel->getTime() > oKernelStartTime ) )
                    {
                      // designated link node has less overlap -> overwrite link
                      if( linkedKernel->getTime() > kernelEndTime )
                      {
                        addEdge = true;
                      }
                    }
                    else // no node linked or currently linked kernel does not overlap
                    {
                      addEdge = true;
                    }
                  }
                  else
                  {
                    addEdge = true;
                  }

                  if( addEdge )
                  {
                    // add edge between overlapping kernel leave
                    // and kernel enter node
//                    Edge *e = analysis->newEdge( kernelNode, oKernelEnter,
//                                kernelEndTime - oKernelStartTime);
//                    e->unblock(); // as it is a reverse edge
                    
                    // Add only a link. A dependency is created, if required.
                    oKernelEnter->setLinkLeft( kernelNode );

                    // add new edge
//                    UTILS_MSG( analysis->getMPIRank() == 0, 
//                               "[KernelOverlapRule] Add dependency "
//                               "edge between %s -> %s!", 
//                               analysis->getNodeInfo( kernelNode ).c_str(),
//                               analysis->getNodeInfo( oKernelEnter ).c_str() );
                  }
                }
              }
            }
          }
        }
        else /* kernel enter node */
        {
          // active tasks are counted in DeviceIdleRule
          // current kernel has already increased active_task counter
          if( ofldAnalysis->active_tasks > 1 )
          {
            // set this kernel enter as overlapping
            ofldAnalysis->oKernelEnter = kernelNode;
            UTILS_MSG( false && ofldAnalysis->getCommon()->getMPIRank() == 0 && 
              ofldAnalysis->getCommon()->getRealTime( kernelNode->getTime() ) > 79.245,
              "### Set overlapping kernel: %s", 
              ofldAnalysis->getCommon()->getNodeInfo( kernelNode ).c_str() );

          }
          else
          {
            ofldAnalysis->oKernelEnter = NULL;
          }
        }
        
        return true;
      }
  };
 }
}
