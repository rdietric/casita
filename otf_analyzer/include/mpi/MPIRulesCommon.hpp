/* 
 * File:   MPIRulesCommon.hpp
 * Author: felix
 *
 * Created on 7. Februar 2014, 14:22
 */

#ifndef MPIRULESCOMMON_HPP
#define	MPIRULESCOMMON_HPP

#include "graph/GraphNode.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace mpi
    {

        static bool processWalkCallback(void *userData, GraphNode* node)
        {
            ProcessWalkInfo *listAndWaitTime = (ProcessWalkInfo *) userData;
            listAndWaitTime->list.push_back(node);
            listAndWaitTime->waitStateTime += node->getCounter(CTR_WAITSTATE, NULL);
            
            if (node->isProcess() || (node->isMPI() && node->isLeave()))
                return false;

            return true;
        }

    }
}


#endif	/* MPIRULESCOMMON_HPP */

