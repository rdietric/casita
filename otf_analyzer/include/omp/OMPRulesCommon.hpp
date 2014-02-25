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
            if (listAndWaitTime->list.size() > 0)
            {
                if (node->getTime() < listAndWaitTime->list.back()->getTime() &&
                       node->getCaller() == NULL)
                {
                    return false;
                }
            }
            
            listAndWaitTime->list.push_back(node);
            listAndWaitTime->waitStateTime += node->getCounter(CTR_WAITSTATE, NULL);
            
            if (node->isProcess() || (node->isLeave() && node->isOMPSync()))
                return false;

            return true;
        }
    }
}


#endif	/* OMPRULESCOMMON_HPP */

