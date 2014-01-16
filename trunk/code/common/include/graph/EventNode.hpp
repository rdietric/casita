/* 
 * File:   EventNode.hpp
 * Author: felix
 *
 * Created on 11. Juni 2013, 10:12
 */

#ifndef EVENTNODE_HPP
#define	EVENTNODE_HPP

#include <stack>

#include "GraphNode.hpp"

namespace cdm
{

    class EventNode : public GraphNode
    {
    public:
        
        typedef std::stack<EventNode*> EventNodeStack;
        typedef std::list<EventNode*> EventNodeList;
        
        enum FunctionResultType
        {
            FR_UNKNOWN = 0,
            FR_SUCCESS = 1
        };

        EventNode(uint64_t time, uint32_t processId, uint32_t eventId,
                FunctionResultType fResult, const std::string name, int nodeType) :
        GraphNode(time, processId, name, nodeType),
        eventId(eventId),
        fResult(fResult)
        {
        }

        uint32_t getEventId() const
        {
            return eventId;
        }

        FunctionResultType getFunctionResult() const
        {
            return fResult;
        }
        
        bool isEventNode() const
        {
            return true;
        }

    protected:
        uint32_t eventId;
        FunctionResultType fResult;
    };

}

#endif	/* EVENTNODE_HPP */

