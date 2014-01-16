/* 
 * File:   StreamWaitPattern.hpp
 * Author: felix
 *
 * Created on 19. Juni 2013, 13:08
 */

#ifndef STREAMWAITPATTERN_HPP
#define	STREAMWAITPATTERN_HPP

#include "IPattern.hpp"

namespace cdm
{

    class StreamWaitPattern : public IPattern
    {
    public:

        StreamWaitPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }

        const char *getName()
        {
            return "StreamWaitPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            if (maxHostProcesses == 0 || maxDeviceProcesses == 0)
                return false;
            *numHost = rand() % maxHostProcesses + 1;
            *numDevice = 2;
            if (maxDeviceProcesses > 2)
                *numDevice += rand() % (maxDeviceProcesses - 1);
            *nullStream = false;
            return true;
        }

    private:

        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            const Allocation::ProcessList &allDeviceProcs = allocation.getDeviceProcesses();

            Process *deviceWait = allDeviceProcs[0];
            Allocation::ProcessList deviceSyncs;
            deviceSyncs.insert(deviceSyncs.begin(),
                    ++(allDeviceProcs.begin()), allDeviceProcs.end());

            const Allocation::ProcessList &hosts = allocation.getHostProcesses();

            uint64_t hTime = allocation.getStartTime();
            uint32_t pIndex = rand() % hosts.size();
            uint32_t eventId = rand() % 1000 + 1;

            bool hasPrevKernel = rand() % 2;

            if (hasPrevKernel)
            {
                // launch prev kernel on waiting device process
                GraphNode *launchPrevEnter = generator.addNewGraphNode(
                        hTime, hosts[pIndex], NT_RT_ENTER | NT_FT_LAUNCH);
                launchPrevEnter->setReferencedProcessId(deviceWait->getId());
                hTime += getTimeOffset(100);

                GraphNode *prevKernelEnter = generator.addNewGraphNode(
                        hTime, deviceWait, NT_RT_ENTER | NT_FT_KERNEL);
                generator.newEdge(launchPrevEnter, prevKernelEnter, false);

                hTime += getTimeOffset(100);

                generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_LEAVE | NT_FT_LAUNCH);

                hTime += getTimeOffset(100);
            }

            // launch long kernels on syncing device processes
            for (Allocation::ProcessList::const_iterator iter = deviceSyncs.begin();
                    iter != deviceSyncs.end(); ++iter)
            {
                Process *deviceSync = *iter;
                pIndex = rand() % hosts.size();

                GraphNode *launchSyncEnter = generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_ENTER | NT_FT_LAUNCH);
                launchSyncEnter->setReferencedProcessId(deviceSync->getId());
                hTime += getTimeOffset(50);
                generator.addNewGraphNode(hTime, hosts[pIndex],
                        NT_RT_LEAVE | NT_FT_LAUNCH);

                hTime += getTimeOffset(50);

                GraphNode *syncKernelEnter = generator.addNewGraphNode(hTime, deviceSync,
                        NT_RT_ENTER | NT_FT_KERNEL);
                generator.newEdge(launchSyncEnter, syncKernelEnter, false);

                // record event
                pIndex = rand() % hosts.size();
                generator.addNewEventNode(hTime, eventId, EventNode::FR_UNKNOWN,
                        hosts[pIndex], NT_RT_ENTER | NT_FT_EV_LAUNCH);
                hTime += getTimeOffset(50);
                EventNode *eventLeave = generator.addNewEventNode(hTime, eventId,
                        EventNode::FR_UNKNOWN, hosts[pIndex], NT_RT_LEAVE | NT_FT_EV_LAUNCH);
                eventLeave->setReferencedProcessId(deviceSync->getId());

                hTime += getTimeOffset(50);

                // cuStreamWaitEvent
                pIndex = rand() % hosts.size();
                generator.addNewEventNode(hTime, eventId, EventNode::FR_UNKNOWN,
                        hosts[pIndex], NT_RT_ENTER | NT_FT_STREAMWAIT);
                hTime += getTimeOffset(50);
                EventNode *streamWaitLeave = generator.addNewEventNode(hTime, eventId,
                        EventNode::FR_SUCCESS,
                        hosts[pIndex], NT_RT_LEAVE | NT_FT_STREAMWAIT);
                streamWaitLeave->setReferencedProcessId(deviceWait->getId());
                
                eventId++;

                hTime += getTimeOffset(50);
            }

            // start launching next kernel
            pIndex = rand() % hosts.size();
            GraphNode *launchNextEnter = generator.addNewGraphNode(
                    hTime, hosts[pIndex], NT_RT_ENTER | NT_FT_LAUNCH);
            launchNextEnter->setReferencedProcessId(deviceWait->getId());
            hTime += getTimeOffset(50);

            // finish prev kernel
            if (hasPrevKernel)
            {
                generator.addNewGraphNode(hTime, deviceWait,
                        NT_RT_LEAVE | NT_FT_KERNEL);
            }

            // insert wait state
            generator.addNewGraphNode(hTime, deviceWait,
                    NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);
            hTime += getTimeOffset(50);

            // finish launching next kernel
            generator.addNewGraphNode(hTime,
                    hosts[pIndex], NT_RT_LEAVE | NT_FT_LAUNCH);

            // finish sync kernels
            GraphNode::GraphNodeList syncKernelLeavesList;
            for (Allocation::ProcessList::const_iterator iter = deviceSyncs.begin();
                    iter != deviceSyncs.end(); ++iter)
            {
                hTime += getTimeOffset(100);
                
                Process *deviceSync = *iter;
                syncKernelLeavesList.push_back(generator.addNewGraphNode(hTime,
                        deviceSync, NT_RT_LEAVE | NT_FT_KERNEL));
            }

            // align waitState leave with last sync kernel leave
            GraphNode *waitLeave = generator.addNewGraphNode(hTime, deviceWait,
                    NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA); 

            hTime += getTimeOffset(100);

            // start next kernel
            GraphNode *nextKernelEnter = generator.addNewGraphNode(hTime,
                    deviceWait, NT_RT_ENTER | NT_FT_KERNEL);
            generator.newEdge(launchNextEnter, nextKernelEnter, false);

            // add dependencies to all sync kernel leaves
            for (GraphNode::GraphNodeList::const_iterator iter = syncKernelLeavesList.begin();
                    iter != syncKernelLeavesList.end(); ++iter)
            {
                generator.newEdge(*iter, waitLeave, false);
                generator.newEdge(*iter, nextKernelEnter, false);
            }

            hTime += getTimeOffset(200);
            generator.addNewGraphNode(hTime, deviceWait,
                    NT_RT_LEAVE | NT_FT_KERNEL);
            
            hTime += getTimeOffset(100);

            return hTime;
        }
    };
}

#endif	/* STREAMWAITPATTERN_HPP */

