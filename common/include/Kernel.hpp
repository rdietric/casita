/* 
 * File:   Kernel.hpp
 * Author: felix
 *
 * Created on 17. Juni 2013, 13:29
 */

#ifndef KERNEL_HPP
#define	KERNEL_HPP

#include "graph/GraphNode.hpp"

namespace cdm
{

    class Activity
    {
    public:

        Activity(GraphNode *start, GraphNode *end) :
        start(start),
        end(end)
        {

        }
        
        Activity(GraphNode::GraphNodePair pair) :
        start(pair.first),
        end(pair.second)
        {

        }

        uint64_t getDuration() const
        {
            return end->getTime() - start->getTime();
        }

        const char *getName() const
        {
            return start->getName();
        }
        
        uint64_t getProcessId() const
        {
            return start->getProcessId();
        }
        
        uint64_t getFunctionId() const
        {
            return start->getFunctionId();
        }
        
        GraphNode *getStart() const
        {
            return start;
        }
        
        GraphNode *getEnd() const
        {
            return end;
        }

    private:
        GraphNode *start, *end;
    };
}

#endif	/* KERNEL_HPP */

