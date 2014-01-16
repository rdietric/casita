/* 
 * File:   SingleBlameSyncPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef SINGLEBLAMESYNCPATTERN_HPP
#define	SINGLEBLAMESYNCPATTERN_HPP

#include "IPattern.hpp"

namespace cdm
{

    class SingleBlameSyncPattern : public IPattern
    {
    public:

        SingleBlameSyncPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }

        const char *getName()
        {
            return "SingleBlameSyncPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            if (maxHostProcesses == 0 || maxDeviceProcesses == 0)
                return false;
            *numHost = 1;
            *numDevice = 1;
            *nullStream = false;
            return true;
        }

    private:

        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            Process *pDevice = allocation.getDeviceProcesses()[0];
            Process *pHost = allocation.getHostProcesses()[0];

            size_t numKernels = rand() % 5 + 1;
            uint64_t hTime = allocation.getStartTime();
            uint64_t syncDeltaTicks = generator.getTimerResolution() * SYNC_DELTA /
                    (1000 * 1000);

            for (size_t i = 0; i < numKernels; ++i)
            {
                hTime += getTimeOffset(100);
                GraphNode *launchEnter = generator.addNewGraphNode(hTime, pHost,
                        NT_RT_ENTER | NT_FT_LAUNCH);
                launchEnter->setReferencedProcessId(pDevice->getId());
                hTime += getTimeOffset(100);
                GraphNode *kernelEnter = generator.addNewGraphNode(hTime, pDevice,
                        NT_RT_ENTER | NT_FT_KERNEL);

                generator.newEdge(launchEnter, kernelEnter, false);

                hTime += getTimeOffset(100);
                generator.addNewGraphNode(hTime, pHost,
                        NT_RT_LEAVE | NT_FT_LAUNCH);

                hTime += getTimeOffset(100);
                if (i < numKernels - 1)
                {
                    generator.addNewGraphNode(hTime, pDevice,
                            NT_RT_LEAVE | NT_FT_KERNEL);
                }
            }
            
            generator.addNewGraphNode(hTime, pHost, NT_RT_ENTER | NT_FT_SYNC);
            hTime += getTimeOffset(100);
            generator.addNewGraphNode(hTime, pDevice, NT_RT_LEAVE | NT_FT_KERNEL);
            generator.addNewGraphNode(hTime, pDevice, NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);

            
            hTime += getTimeOffset(50) + syncDeltaTicks;
            
            GraphNode *waitLeave = generator.addNewGraphNode(hTime, pDevice,
                    NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA);
            GraphNode *syncLeave = generator.addNewGraphNode(hTime, pHost,
                    NT_RT_LEAVE | NT_FT_SYNC);
            syncLeave->setReferencedProcessId(pDevice->getId());
            generator.newEdge(syncLeave, waitLeave, false);

            hTime += getTimeOffset(100);
            
            return hTime;
        }
    };

}

#endif	/* SINGLEBLAMESYNCPATTERN_HPP */

