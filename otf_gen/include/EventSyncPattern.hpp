/* 
 * File:   EventSyncPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef EVENTSYNCPATTERN_HPP
#define	EVENTSYNCPATTERN_HPP

#include "IPattern.hpp"
#include "graph/Edge.hpp"
#include "graph/EventNode.hpp"

namespace cdm
{

    class EventSyncPattern : public IPattern
    {
    public:

        EventSyncPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }

        const char *getName()
        {
            return "EventSyncPattern";
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
            uint64_t hTime = allocation.getStartTime();

            const uint32_t event1Id = rand() % 10000 + 1;
            const uint32_t event2Id = event1Id + rand() % 10 + 1;

            uint32_t pIndex = rand() % hosts.size();
            generator.addNewEventNode(hTime, event1Id, EventNode::FR_UNKNOWN,
                    hosts[pIndex], NT_RT_ENTER | NT_FT_EV_LAUNCH);
            hTime += getTimeOffset(100);
            EventNode *event1Leave = generator.addNewEventNode(hTime, event1Id,
                    EventNode::FR_UNKNOWN, hosts[pIndex], NT_RT_LEAVE | NT_FT_EV_LAUNCH);
            event1Leave->setReferencedProcessId(device->getId());

            hTime += getTimeOffset(100);

            for (size_t i = 0; i < numKernels; ++i)
            {
                pIndex = rand() % hosts.size();
                GraphNode *launchEnter = generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_ENTER | NT_FT_LAUNCH);
                launchEnter->setReferencedProcessId(device->getId());

                hTime += getTimeOffset(100);
                GraphNode *kernelEnter = generator.addNewGraphNode(hTime, device,
                        NT_RT_ENTER | NT_FT_KERNEL);

                generator.newEdge(launchEnter, kernelEnter);

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
            
            pIndex = rand() % hosts.size();
            generator.addNewEventNode(hTime, event2Id, EventNode::FR_UNKNOWN,
                    hosts[pIndex], NT_RT_ENTER | NT_FT_EV_LAUNCH);
            hTime += getTimeOffset(100);
            EventNode *event2Leave = generator.addNewEventNode(hTime, event2Id,
                    EventNode::FR_UNKNOWN, hosts[pIndex], NT_RT_LEAVE | NT_FT_EV_LAUNCH);
            event2Leave->setReferencedProcessId(device->getId());

            hTime += getTimeOffset(100);
            
            pIndex = rand() % hosts.size();
            generator.addNewEventNode(hTime, event2Id, EventNode::FR_UNKNOWN,
                    hosts[pIndex], NT_RT_ENTER | NT_FT_EV_SYNC);
            hTime += getTimeOffset(100);
            
            GraphNode *kernelLeave = generator.addNewGraphNode(hTime, device,
                    NT_RT_LEAVE | NT_FT_KERNEL);
            
            Edge *edge = NULL;
            EventNode *syncLeave = generator.addNewEventNode(hTime, event2Id,
                    EventNode::FR_SUCCESS, hosts[pIndex], NT_RT_LEAVE | NT_FT_EV_SYNC,
                    &edge);
            edge->makeBlocking();
            
            generator.newEdge(kernelLeave, syncLeave, EDGE_CAUSES_WAITSTATE);
            
            hTime += getTimeOffset(100);
            
            return hTime;
        }
    };

}

#endif	/* EVENTSYNCPATTERN_HPP */

