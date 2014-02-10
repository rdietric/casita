/* 
 * File:   MPIRulesCommon.hpp
 * Author: felix
 *
 * Created on 7. Februar 2014, 14:22
 */

#ifndef MPIRULESCOMMON_HPP
#define	MPIRULESCOMMON_HPP

#include "graph/GraphNode.hpp"

namespace cdm
{
    namespace mpi
    {

        static bool processWalkCallback(void *userData, GraphNode* node)
        {
            GraphNode::GraphNodeList *list = (GraphNode::GraphNodeList *)userData;
            list->push_back(node);
            
            if (node->isProcess() || (node->isMPI() && node->isLeave()))
                return false;

            return true;
        }

    }
}


#endif	/* MPIRULESCOMMON_HPP */

