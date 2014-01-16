/* 
 * File:   EventQueryPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef EVENTQUERYPATTERN_HPP
#define	EVENTQUERYPATTERN_HPP

#include "IPattern.hpp"
#include "graph/Edge.hpp"
#include "graph/EventNode.hpp"

namespace cdm
{

    class EventQueryPattern : public IPattern
    {
    public:

        EventQueryPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }

        const char *getName()
        {
            return "EventQueryPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            if (maxHostProcesses == 0 || maxDeviceProcesses == 0)
                return false;
            *numHost = rand() % maxHostProcesses + 1;
            *numDevice = 1;
            *nullStream = false;
            return true;
        }

    private:

        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            Process *device = allocation.getDeviceProcesses()[0];
            const Allocation::ProcessList &hosts = allocation.getHostProcesses();

            size_t numKernels = rand() % 4 + 1;
            size_t numFailedQueries = rand() % 10;
            uint64_t hTime = allocation.getStartTime();

            const uint32_t eventId = rand() % 10000 + 1;

            uint32_t pIndex = 0;

            // launch kernels
            for (size_t i = 0; i < numKernels; ++i)
            {
                pIndex = rand() % hosts.size();
                GraphNode *launchEnter = generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_ENTER | NT_FT_LAUNCH);
                launchEnter->setReferencedProcessId(device->getId());

                hTime += getTimeOffset(100);
                GraphNode *kernelEnter = generator.addNewGraphNode(hTime, device,
                        NT_RT_ENTER | NT_FT_KERNEL);

                generator.newEdge(launchEnter, kernelEnter, false);

                hTime += getTimeOffset(100);
                generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_LEAVE | NT_FT_LAUNCH); 

                hTime += getTimeOffset(100);

                if (i < numKernels - 1)
                {
                    generator.addNewGraphNode(hTime, device,
                            NT_RT_LEAVE | NT_FT_KERNEL);
                }
            }

            // record event
            pIndex = rand() % hosts.size();
            generator.addNewEventNode(hTime, eventId, EventNode::FR_UNKNOWN,
                    hosts[pIndex], NT_RT_ENTER | NT_FT_EV_LAUNCH);
            hTime += getTimeOffset(100);
            EventNode *eventLaunchLeave = generator.addNewEventNode(hTime, eventId,
                    EventNode::FR_UNKNOWN, hosts[pIndex], NT_RT_LEAVE | NT_FT_EV_LAUNCH);
            eventLaunchLeave->setReferencedProcessId(device->getId());

            hTime += getTimeOffset(100);

            // failed queries
            std::vector<EventNode*> failedQueries;
            for (size_t i = 0; i < numFailedQueries; ++i)
            {
                pIndex = rand() % hosts.size();
                Edge *edge = NULL;
                generator.addNewEventNode(hTime, eventId,
                        EventNode::FR_UNKNOWN, hosts[pIndex],
                        NT_RT_ENTER | NT_FT_EV_QUERY);

                hTime += getTimeOffset(50);

                failedQueries.push_back(generator.addNewEventNode(hTime, eventId,
                        EventNode::FR_UNKNOWN, hosts[pIndex],
                        NT_RT_LEAVE | NT_FT_EV_QUERY, &edge));
                edge->makeBlocking();
                
                hTime += getTimeOffset(100);
            }

            hTime += getTimeOffset(50);

            GraphNode *kernelLeave = generator.addNewGraphNode(hTime, device,
                    NT_RT_LEAVE | NT_FT_KERNEL);

            hTime += getTimeOffset(50);

            // successful query
            generator.addNewEventNode(hTime, eventId,
                    EventNode::FR_SUCCESS, hosts[pIndex],
                    NT_RT_ENTER | NT_FT_EV_QUERY);

            hTime += getTimeOffset(50);

            EventNode *queryLeave = generator.addNewEventNode(hTime, eventId,
                    EventNode::FR_SUCCESS, hosts[pIndex],
                    NT_RT_LEAVE | NT_FT_EV_QUERY);

            // kernel/query dependency
            generator.newEdge(kernelLeave, queryLeave, false);
            for (std::vector<EventNode*>::iterator i = failedQueries.begin();
                    i != failedQueries.end(); ++i)
            {
                generator.newEdge(kernelLeave, *i, true);
            }
            
            hTime += getTimeOffset(100);

            return hTime;
        }
    };

}

#endif	/* EVENTQUERYPATTERN_HPP */

