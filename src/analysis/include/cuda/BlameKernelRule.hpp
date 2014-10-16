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

      BlameKernelRule( int priority ) :
        ICUDARule( "BlameKernelRule", priority )
      {

      }

    private:

      bool
      apply( AnalysisParadigmCUDA* analysis, GraphNode* node )
      {
        /* applied at sync */
        if ( !node->isCUDASync( ) || !node->isLeave( ) )
        {
          return false;
        }

        AnalysisEngine* commonAnalysis = analysis->getCommon( );

        /* get the complete execution */
        GraphNode::GraphNodePair& sync = node->getGraphPair( );

        /* ignore delta ticks for now until we have a better heuristic */
        /* uint64_t syncDeltaTicks        = commonAnalysis->getDeltaTicks( ); */

        bool ruleResult = false;
        /* find all referenced (device) streams */
        EventStreamGroup::EventStreamList deviceStreams;
        commonAnalysis->getAllDeviceStreams( deviceStreams );
        for ( EventStreamGroup::EventStreamList::const_iterator pIter =
                deviceStreams.begin( );
              pIter != deviceStreams.end( ); ++pIter )
        {
          EventStream* deviceStream = *pIter;

          if ( !sync.first->referencesStream( deviceStream->getId( ) ) )
          {
            continue;
          }

          /* test that there is a pending kernel (leave) */
          bool isFirstKernel        = true;
          while ( true )
          {
            GraphNode* kernelLeave           = deviceStream->getPendingKernel( );
            if ( !kernelLeave )
            {
              break;
            }

            GraphNode::GraphNodePair& kernel = kernelLeave->getGraphPair( );

            if ( ( isFirstKernel &&
                   ( sync.first->getTime( ) < kernel.second->getTime( ) ) ) ||
                 ( !isFirstKernel &&
                   ( sync.first->getTime( ) < kernel.second->getTime( ) ) ) )
            {
              if ( isFirstKernel )
              {
                commonAnalysis->newEdge( kernel.second,
                                         sync.second,
                                         EDGE_CAUSES_WAITSTATE );
              }

              commonAnalysis->getEdge( sync.first, sync.second )->makeBlocking( );

              /* set counters */
              sync.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                         CTR_WAITSTATE ),
                                       std::min( sync.second->getTime( ),
                                                 kernel.second->getTime( ) ) -
                                       std::max( sync.first->getTime( ),
                                                 kernel.first->getTime( ) ) );
              kernel.second->incCounter( commonAnalysis->getCtrTable( ).getCtrId(
                                           CTR_BLAME ),
                                         std::min( sync.second->getTime( ),
                                                   kernel.second->getTime( ) ) -
                                         std::max( sync.first->getTime( ),
                                                   kernel.first->getTime( ) ) );

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
