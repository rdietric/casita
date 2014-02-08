/* 
 * File:   EventQueryRule.hpp
 * Author: felix
 *
 * Created on 18. Juni 2013, 12:31
 */

#ifndef EVENTQUERYRULE_HPP
#define	EVENTQUERYRULE_HPP

#include "AnalysisEngine.hpp"


namespace cdm
{

    class EventQueryRule : public AbstractRule
    {
    public:

        EventQueryRule(int priority) :
        AbstractRule("EventQueryRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("EventQueryRule");

            if (!node->isCUDAEventQuery() || !node->isLeave())
                return false;

            EventNode *evQueryLeave = (EventNode*) node;

            // link to previous matching event query
            analysis->linkEventQuery(evQueryLeave);

            if (evQueryLeave->getFunctionResult() == EventNode::FR_UNKNOWN)
            {
                // nothing to do here
                return true;
            }

            // get the complete execution
            GraphNode::GraphNodePair& evQuery = evQueryLeave->getGraphPair();

            // consume mapping for this event ID
            analysis->removeEventQuery(evQueryLeave->getEventId());

            // get the device process ID this event is queued on
            uint32_t refDeviceProcessId = analysis->getEventProcessId(
                    evQueryLeave->getEventId());
            if (!refDeviceProcessId)
                ErrorUtils::getInstance().throwFatalError("Could not find device process ID for event %u from %s",
                    evQueryLeave->getEventId(), evQueryLeave->getUniqueName().c_str());

            // get the first kernel launch before eventLaunch/enter
            EventNode *eventLaunchLeave = analysis->getLastEventLaunchLeave(evQueryLeave->getEventId());
            if (!eventLaunchLeave)
                throw RTException("Could not find event record for event %u",
                    evQueryLeave->getEventId());

            GraphNode *kernelLaunchLeave = analysis->getLastLaunchLeave(
                    eventLaunchLeave->getGraphPair().first->getTime(),
                    refDeviceProcessId);

            if (kernelLaunchLeave)
            {
                GraphNode::GraphNodePair& kernelLaunch =
                        ((GraphNode*) kernelLaunchLeave)->getGraphPair();
                GraphNode *kernelEnter = (GraphNode*) kernelLaunch.first->getLink();
                if (!kernelEnter)
                {
                    ErrorUtils::getInstance().throwError("Event query %s (%f) returns success but kernel from %s (%f) did not finish yet",
                        evQueryLeave->getUniqueName().c_str(),
                        analysis->getRealTime(evQueryLeave->getTime()),
                        kernelLaunch.first->getUniqueName().c_str(),
                        analysis->getRealTime(kernelLaunch.first->getTime()));
                    return false;
                }

                GraphNode *kernelLeave = kernelEnter->getGraphPair().second;
                GraphNode::GraphNodePair kernel = kernelLeave->getGraphPair();

                if (evQuery.second->getTime() < kernelLeave->getTime())
                    throw RTException("Incorrect timing between %s and %s\n",
                        evQuery.second->getUniqueName().c_str(),
                        kernelLeave->getUniqueName().c_str());

                // walk all event query nodes and make blocking if they
                // depend on the kernel
                EventNode *firstEventQueryLeave = evQueryLeave;
                while (true)
                {
                    EventNode *prev = (EventNode*) (firstEventQueryLeave->getLink());
                    if (!prev)
                        break;

                    GraphNode::GraphNodePair& prevQuery = prev->getGraphPair();

                    if (kernel.first->getTime() <= prevQuery.first->getTime())
                    {
                        analysis->getEdge(
                                prevQuery.first, prevQuery.second)->makeBlocking();

                        // set counters
                        prevQuery.first->setCounter(
                                analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
                        kernel.first->incCounter(
                                analysis->getCtrTable().getCtrId(CTR_BLAME),
                                prevQuery.second->getTime() - prevQuery.first->getTime());

                        // add a blocking dependency, so it cannot be used
                        // for critical path analysis
                        analysis->newEdge(kernelLeave, prevQuery.second, EDGE_IS_BLOCKING);
                    }

                    firstEventQueryLeave = prev;
                }

                // add kernel/last event query leave dependency
                analysis->newEdge(kernelLeave, evQuery.second);
                return true;
            }

            return false;
        }
    };
}

#endif	/* EVENTQUERYRULE_HPP */

