/* 
 * File:   OMPRulesCommon.hpp
 * Author: felix
 *
 * Created on 10. Februar 2014, 14:22
 */

#ifndef OMPRULESCOMMON_HPP
#define	OMPRULESCOMMON_HPP

#include "graph/GraphNode.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace omp
    {

        static bool processWalkCallback(void *userData, GraphNode* node)
        {
            ProcessWalkInfo *listAndWaitTime = (ProcessWalkInfo*) userData;
            listAndWaitTime->list.push_back(node);
            listAndWaitTime->waitStateTime += node->getCounter(CTR_WAITSTATE, NULL);
            
            if (node->isProcess() || (node->isOMP() && node->isOMPSync() && node->isLeave()))
                return false;

            return true;
        }
    }
}


#endif	/* OMPRULESCOMMON_HPP */

