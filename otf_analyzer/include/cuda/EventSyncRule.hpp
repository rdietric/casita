/* 
 * File:   EventSyncRule.hpp
 * Author: felix
 *
 * Created on 14. Juni 2013, 10:30
 */

#ifndef EVENTSYNCRULE_HPP
#define	EVENTSYNCRULE_HPP

#include "AbstractRule.hpp"

namespace cdm
{

    class EventSyncRule : public AbstractRule
    {
    public:

        EventSyncRule(int priority) :
        AbstractRule("EventSyncRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("EventSyncRule");

            if (!node->isEventSync())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair& sync = ((GraphNode*) node)->getGraphPair();

            EventNode *eventLaunchLeave = analysis->getLastEventLaunchLeave(
                    ((EventNode*) sync.second)->getEventId());

            if (!eventLaunchLeave)
            {
                printf(" * Ignoring event sync %s without matching event record\n",
                        node->getUniqueName().c_str());
                return false;
            }

            GraphNode *eventLaunchEnter = eventLaunchLeave->getGraphPair().first;
            Process *refProcess = analysis->getProcess(eventLaunchLeave->getReferencedProcessId());
            Allocation::ProcessList deviceProcs;

            if (refProcess->isDeviceNullProcess())
            {
                analysis->getAllDeviceProcesses(deviceProcs);
            } else
                deviceProcs.push_back(refProcess);

            bool ruleResult = false;
            for (Allocation::ProcessList::const_iterator iter = deviceProcs.begin();
                    iter != deviceProcs.end(); ++iter)
            {
                // last kernel launch before event record for this process
                GraphNode *kernelLaunchLeave = analysis->getLastLaunchLeave(
                        eventLaunchEnter->getTime(), (*iter)->getId());
                if (!kernelLaunchLeave)
                    continue;

                GraphNode::GraphNodePair& kernelLaunch = ((GraphNode*) kernelLaunchLeave)->getGraphPair();

                GraphNode *kernelEnter = (GraphNode*) kernelLaunch.first->getLink();
                if (!kernelEnter)
                {
                    throw RTException("Event sync %s (%f) returned but kernel from %s (%f) on process [%u, %s] did not start/finish yet",
                            node->getUniqueName().c_str(), analysis->getRealTime(node->getTime()),
                            kernelLaunch.first->getUniqueName().c_str(),
                            analysis->getRealTime(kernelLaunch.first->getTime()),
                            kernelLaunch.first->getReferencedProcessId(),
                            analysis->getProcess(kernelLaunch.first->getReferencedProcessId())->getName());
                }

                GraphNode *kernelLeave = kernelEnter->getGraphPair().second;
                if (!kernelLeave || kernelLeave->getTime() > sync.second->getTime())
                {
                    throw RTException("Event sync %s (%f) returned but kernel from %s (%f) on process [%u, %s] did not finish yet",
                            node->getUniqueName().c_str(), analysis->getRealTime(node->getTime()),
                            kernelLaunch.first->getUniqueName().c_str(),
                            analysis->getRealTime(kernelLaunch.first->getTime()),
                            kernelLaunch.first->getReferencedProcessId(),
                            analysis->getProcess(kernelLaunch.first->getReferencedProcessId())->getName());
                }

                uint64_t syncDeltaTicks = analysis->getDeltaTicks();

                if ((sync.first->getTime() < kernelLeave->getTime()) &&
                        (sync.second->getTime() - kernelLeave->getTime() <=
                        syncDeltaTicks))
                {
                    analysis->getEdge(sync.first, sync.second)->makeBlocking();

                    // set counters
                    sync.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
                    kernelEnter->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                            sync.second->getTime() -
                            std::max(sync.first->getTime(), kernelEnter->getTime()));
                }

                analysis->newEdge(kernelLeave, sync.second, false);
                ruleResult = true;
            }

            return ruleResult;
        }
    };

}

#endif	/* EVENTSYNCRULE_HPP */

