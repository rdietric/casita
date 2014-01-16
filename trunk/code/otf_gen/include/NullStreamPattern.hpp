/* 
 * File:   NullStreamPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef NULLSTREAMPATTERN_HPP
#define	NULLSTREAMPATTERN_HPP

#include "IPattern.hpp"

namespace cdm
{

    class NullStreamPattern : public IPattern
    {
    public:

        NullStreamPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }

        const char *getName()
        {
            return "NullStreamPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            if (maxHostProcesses == 0 || maxDeviceProcesses == 0 || !hasNullStream)
                return false;
            *numHost = 1;
            *numDevice = maxDeviceProcesses;
            *nullStream = true;
            return true;
        }

    private:

        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            Allocation::ProcessList deviceProcs = allocation.getDeviceProcesses();
            Process *pHost = allocation.getHostProcesses()[0];
            Process *pNullStream = allocation.getNullStream();

            uint64_t time = allocation.getStartTime();
            GraphNode::GraphNodeList lastKernelLeaves;
            bool blameNullStreamSync = false;
            uint64_t syncDeltaTicks = generator.getTimerResolution() * SYNC_DELTA /
                    (1000 * 1000);

            for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                    pIter != deviceProcs.end(); ++pIter)
            {
                Process *pDevice = *pIter;
                size_t numKernels = rand() % 2 + 1;

                for (size_t i = 0; i < numKernels; ++i)
                {
                    // launch enter
                    GraphNode *launchEnter = generator.addNewGraphNode(time, pHost,
                            NT_RT_ENTER | NT_FT_LAUNCH);
                    launchEnter->setReferencedProcessId(pDevice->getId());
                    time += getTimeOffset(100);
                    // kernel enter
                    GraphNode *kernelEnter = generator.addNewGraphNode(time, pDevice,
                            NT_RT_ENTER | NT_FT_KERNEL);

                    generator.newEdge(launchEnter, kernelEnter, false);

                    time += getTimeOffset(100);
                    // launch leave
                    generator.addNewGraphNode(time, pHost,
                            NT_RT_LEAVE | NT_FT_LAUNCH);

                    if ((i == numKernels - 1) && (deviceProcs.back() == *pIter))
                    {
                        // null stream launch enter
                        time += getTimeOffset(100);
                        GraphNode *nullLaunchEnter = generator.addNewGraphNode(time, pHost,
                                NT_RT_ENTER | NT_FT_LAUNCH);
                        nullLaunchEnter->setReferencedProcessId(pNullStream->getId());
                    }

                    time += getTimeOffset(100);
                    // kernel leave
                    GraphNode *kernelLeave = generator.addNewGraphNode(time, pDevice,
                            NT_RT_LEAVE | NT_FT_KERNEL);
                    if (i == numKernels - 1)
                    {
                        lastKernelLeaves.push_back(kernelLeave);
                    }

                    if ((i == numKernels - 1) && (deviceProcs.back() == *pIter))
                    {
                        time += rand() % 100;
                        // null stream kernel enter
                        GraphNode *nsKernelEnter = generator.addNewGraphNode(time, pNullStream,
                                NT_RT_ENTER | NT_FT_KERNEL | NT_FT_COLLSYNC | NT_FT_SYNC);
                        generator.newEdge(launchEnter, nsKernelEnter, false);
                        // add wait state enter on all device processes for null stream kernel
                        for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                                pIter != deviceProcs.end(); ++pIter)
                        {
                            GraphNode *waitEnter = generator.addNewGraphNode(time,
                                    *pIter, NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);
                            generator.newEdge(nsKernelEnter, waitEnter, false);
                        }

                        for (GraphNode::GraphNodeList::const_iterator nIter = lastKernelLeaves.begin();
                                nIter != lastKernelLeaves.end(); ++nIter)
                        {
                            generator.newEdge((*nIter), nsKernelEnter, false);
                        }

                        time += getTimeOffset(100);
                        // null stream launch leave
                        generator.addNewGraphNode(time, pHost,
                                NT_RT_LEAVE | NT_FT_LAUNCH);
                    }
                }
            }

            time += getTimeOffset(100);

            generator.addNewGraphNode(time, pHost,
                    NT_RT_ENTER | NT_FT_COLLSYNC | NT_FT_SYNC);

            // null stream kernel leave and device procs wait states leave
            time += getTimeOffset(100);
            GraphNode *nsKernelLeave = generator.addNewGraphNode(time, pNullStream,
                    NT_RT_LEAVE | NT_FT_KERNEL | NT_FT_COLLSYNC | NT_FT_SYNC);

            GraphNode *syncLeave = NULL;
            if (!blameNullStreamSync)
            {
                Edge *syncEdge = NULL;
                syncLeave = generator.addNewGraphNode(time, pHost,
                        NT_RT_LEAVE | NT_FT_COLLSYNC | NT_FT_SYNC, &syncEdge);
                syncEdge->makeBlocking();
            }

            for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                    pIter != deviceProcs.end(); ++pIter)
            {
                GraphNode *waitLeave = generator.addNewGraphNode(time, *pIter,
                        NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA);
                generator.newEdge(nsKernelLeave, waitLeave, false);
                if (!blameNullStreamSync)
                {
                    generator.newEdge(syncLeave, waitLeave, false);
                }
            }

            if (blameNullStreamSync)
            {
                // null stream and device procs wait state enter
                generator.addNewGraphNode(time, pNullStream, NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);
                for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                        pIter != deviceProcs.end(); ++pIter)
                {
                    generator.addNewGraphNode(time, *pIter, NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);
                }

                // sync leave
                time += getTimeOffset(100) + syncDeltaTicks;
                syncLeave = generator.addNewGraphNode(time, pHost,
                        NT_RT_LEAVE | NT_FT_COLLSYNC | NT_FT_SYNC);

                // null stream wait state leave
                GraphNode *waitLeave = generator.addNewGraphNode(time, pNullStream,
                        NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA);
                generator.newEdge(syncLeave, waitLeave, false);
                // device procs wait state leave
                for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                        pIter != deviceProcs.end(); ++pIter)
                {
                    waitLeave = generator.addNewGraphNode(time, *pIter,
                            NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA);
                    generator.newEdge(syncLeave, waitLeave, false);
                }
            }
            
            time += getTimeOffset(100);

            return time;
        }
    };

}

#endif	/* NULLSTREAMPATTERN_HPP */

