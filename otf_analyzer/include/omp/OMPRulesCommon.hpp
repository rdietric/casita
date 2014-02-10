/* 
 * File:   OMPRulesCommon.hpp
 * Author: felix
 *
 * Created on 10. Februar 2014, 14:22
 */

#ifndef OMPRULESCOMMON_HPP
#define	OMPRULESCOMMON_HPP

#include "graph/GraphNode.hpp"

namespace cdm
{
    namespace omp
    {

        static bool processWalkCallback(void *userData, GraphNode* node)
        {
            GraphNode::GraphNodeList *list = (GraphNode::GraphNodeList *)userData;
            list->push_back(node);

            if (node->isProcess() || (node->isOMP() && node->isOMPSync() && node->isLeave()))
                return false;

            return true;
        }
    }
}


#endif	/* OMPRULESCOMMON_HPP */

