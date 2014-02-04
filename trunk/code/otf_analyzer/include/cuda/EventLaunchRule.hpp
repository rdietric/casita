/* 
 * File:   EventlLaunchRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef EVENTLAUNCHRULE_HPP
#define	EVENTLAUNCHRULE_HPP

#include "AbstractRule.hpp"

namespace cdm
{

    class EventLaunchRule : public AbstractRule
    {
    public:

        EventLaunchRule(int priority) :
        AbstractRule("EventLaunchRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            if (!node->isCUDAEventLaunch() || !node->isLeave())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair& evLaunch = ((GraphNode*) node)->getGraphPair();
            Process *refProcess = analysis->getProcess(node->getReferencedProcessId());
            if (!refProcess)
                RTException("Event launch %s (%f) does not reference any process (id = %u)",
                    node->getUniqueName().c_str(), analysis->getRealTime(node->getTime()),
                    node->getReferencedProcessId());

            if (refProcess->isHostProcess())
                RTException("Process %s referenced by event launch %s is a host process",
                    refProcess->getName(), node->getUniqueName().c_str());

            analysis->setEventProcessId(((EventNode*) evLaunch.second)->getEventId(),
                    refProcess->getId());

            GraphNode *kernelLaunchLeave = NULL;

            // if event is on NULL stream, test if any kernel launch can be found
            if (refProcess->isDeviceNullProcess())
            {
                /*Allocation::ProcessList deviceProcs;
                analysis->getAllDeviceProcesses(deviceProcs);
                
                for (Allocation::ProcessList::const_iterator iter = deviceProcs.begin();
                        iter != deviceProcs.end(); ++iter)
                {
                    kernelLaunchLeave = analysis->getLastLaunchLeave(
                            evLaunch.first->getTime(), (*iter)->getId());

                    if (kernelLaunchLeave)
                        break;
                }

                if (kernelLaunchLeave)
                {*/
                analysis->setLastEventLaunch((EventNode*) (evLaunch.second));
                return true;
                //}
            } else
                // otherwise, test on its stream only
            {
                kernelLaunchLeave = analysis->getLastLaunchLeave(
                        evLaunch.first->getTime(), refProcess->getId());

                if (kernelLaunchLeave)
                {
                    evLaunch.second->setLink((GraphNode*) kernelLaunchLeave);
                }

                analysis->setLastEventLaunch((EventNode*) (evLaunch.second));
                return true;
                //}
            }

            return false;
        }
    };

}

#endif	/* EVENTLAUNCHRULE_HPP */

