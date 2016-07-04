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

#include "IOpenCLRule.hpp"
#include "AnalysisParadigmOpenCL.hpp"

namespace casita
{
 namespace opencl
 {
  class BlameKernelRule :
    public IOpenCLRule
  {
    public:

      BlameKernelRule( int priority ) :
        IOpenCLRule( "BlameKernelRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmOpenCL* analysis, GraphNode* syncLeave )
      {
        // applied at OpenCL command queue synchronization
        if ( !syncLeave->isOpenCLQueueSync( ) || !syncLeave->isLeave( ) )
        {
          return false;
        }
        
        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode* syncEnter = syncLeave->getGraphPair( ).first;

        bool ruleResult = false;
        /* find all referenced (device) streams */
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );        
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( ); ++pIter )
        {
          EventStream* deviceStream = *pIter;

          if ( !syncEnter->referencesStream( deviceStream->getId( ) ) )
          {
            continue;
          }
          
          ErrorUtils::getInstance( ).outputMessage("OpenCL blame kernel rule: Found referenced queue" );

          // test that there is a pending kernel (leave)
          bool isFirstKernel = true;
          while ( true )
          {
            GraphNode* kernelLeave = deviceStream->getPendingKernel( );
            if ( !kernelLeave )
            {
              ErrorUtils::getInstance( ).outputMessage("OpenCL blame kernel rule: NO pending kernel found" );

              
              break;
            }
            
            ErrorUtils::getInstance( ).outputMessage("OpenCL blame kernel rule: Pending kernel found" );

            GraphNode::GraphNodePair& kernel = kernelLeave->getGraphPair( );

            // if sync start time < kernel end time
            //\todo: What if other kernels are concurrently executed during the sync?
            if ( syncEnter->getTime( ) < kernel.second->getTime( ) )
            {
              if ( isFirstKernel )
              {
                commonAnalysis->newEdge( kernel.second, syncLeave,
                                         EDGE_CAUSES_WAITSTATE );
              }

              commonAnalysis->getEdge( syncEnter, syncLeave )->makeBlocking( );

              // set counters (to sync leave node)
              syncLeave->incCounter( WAITING_TIME,
                                     std::min( syncLeave->getTime( ),
                                               kernel.second->getTime( ) ) -
                                     std::max( syncEnter->getTime( ),
                                               kernel.first->getTime( ) ) );
              
              kernel.second->incCounter( BLAME,
                                         std::min( syncLeave->getTime( ),
                                                   kernel.second->getTime( ) ) -
                                         std::max( syncEnter->getTime( ),
                                                   kernel.first->getTime( ) ) );
              
              ErrorUtils::getInstance( ).outputMessage("OpenCL blame kernel rule: set blame");

              ruleResult    = true;
              isFirstKernel = false;
              deviceStream->consumePendingKernel( );
            }
            else
            {
              deviceStream->clearPendingKernels( );
              break;
            }
          }
        }

        return ruleResult;
      }
  };
 }
}
