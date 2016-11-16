/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include "ICUDARule.hpp"
#include "AnalysisParadigmCUDA.hpp"

namespace casita
{
 namespace cuda
 {
  class BlameKernelRule :
    public ICUDARule
  {
    public:

      /**
       * Blame kernels for causing wait states. It is basically an early
       * blocking synchronization.
       * This rule uses the pending kernel list and needs access to kernel
       * enter and leave nodes.
       * 
       * @param priority
       */
      BlameKernelRule( int priority ) :
        ICUDARule( "BlameKernelRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* syncLeave )
      {
        // applied at sync
        if ( !syncLeave->isCUDASync( ) || !syncLeave->isLeave( ) )
        {
          return false;
        }
        
        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        GraphNode* syncEnter = syncLeave->getGraphPair( ).first;

        bool ruleResult = false;
        
        // find all referenced (device) streams of this synchronization
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( ); ++pIter )
        {
          EventStream* deviceStream = *pIter;

          // ignore device streams that are not referenced by this sync
          if ( !syncEnter->referencesStream( deviceStream->getId( ) ) )
          {
            continue;
          }

          // test that there is a pending kernel (leave)
          bool isFirstKernel = true;
          while ( true )
          {
            GraphNode* kernelLeave = deviceStream->getFirstPendingKernel( );
            if ( !kernelLeave )
            {
              break;
            }

            GraphNode* kernelEnter = kernelLeave->getGraphPair( ).first;

            // Early sync: sync start time < kernel end time
            //\todo: What if other kernels are concurrently executed during the sync?
            if ( syncEnter->getTime( ) < kernelLeave->getTime( ) )
            {
              if ( isFirstKernel )
              {
                commonAnalysis->newEdge( kernelLeave, syncLeave,
                                         EDGE_CAUSES_WAITSTATE );
              }

              commonAnalysis->getEdge( syncEnter, syncLeave )->makeBlocking( );

              // set counters (to sync leave node)
              //\todo: set counters of enter nodes
              syncLeave->incCounter( WAITING_TIME,
                                     std::min( syncLeave->getTime(),
                                               kernelLeave->getTime() ) -
                                     std::max( syncEnter->getTime(),
                                               kernelEnter->getTime() ) );
              
              kernelLeave->incCounter( BLAME,
                                       std::min( syncLeave->getTime(),
                                                 kernelLeave->getTime() ) -
                                       std::max( syncEnter->getTime(),
                                                 kernelEnter->getTime() ) );
              
              // set link to sync leave node (mark kernel as synchronized)
              kernelLeave->setLink( syncLeave );

              ruleResult    = true;
              isFirstKernel = false;
              deviceStream->consumeFirstPendingKernel();
            }
            else // late sync: all pending kernels are synchronized
            {
              // set link to sync leave node (mark kernel as synchronized)
              deviceStream->setPendingKernelsSyncLink( syncLeave );
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
