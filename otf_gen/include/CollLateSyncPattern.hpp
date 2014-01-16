/* 
 * File:   CollLateSyncPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef COLLLATESYNCPATTERN_HPP
#define	COLLLATESYNCPATTERN_HPP

#include "IPattern.hpp"

namespace cdm
{

    class CollLateSyncPattern : public IPattern
    {
    public:

        CollLateSyncPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }
        
        const char *getName()
        {
            return "CollLateSyncPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            if (maxHostProcesses == 0 || maxDeviceProcesses == 0)
                return false;
            *numHost = 1;
            *numDevice = maxDeviceProcesses;
            *nullStream = false;
            return true;
        }

    private:
        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            Allocation::ProcessList deviceProcs = allocation.getDeviceProcesses();
            Process *pHost = allocation.getHostProcesses()[0];

            uint64_t hTime = allocation.getStartTime();

            size_t numKernels = rand() % 4 + 1;
            for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                    pIter != deviceProcs.end(); ++pIter)
            {
                Process *pDevice = *pIter;

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
                    generator.addNewGraphNode(hTime, pDevice,
                            NT_RT_LEAVE | NT_FT_KERNEL);
                }
            }

            hTime += getTimeOffset(100);

            generator.addNewGraphNode(hTime, pHost,
                    NT_RT_ENTER | NT_FT_COLLSYNC | NT_FT_SYNC);
            for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                    pIter != deviceProcs.end(); ++pIter)
            {
                generator.addNewGraphNode(hTime, *pIter, NT_RT_ENTER | NT_FT_WAITSTATE_CUDA);
            }

            hTime += getTimeOffset(100);
            GraphNode *syncLeave = generator.addNewGraphNode(hTime, pHost,
                    NT_RT_LEAVE | NT_FT_COLLSYNC | NT_FT_SYNC);

            for (Allocation::ProcessList::const_iterator pIter = deviceProcs.begin();
                    pIter != deviceProcs.end(); ++pIter)
            {
                GraphNode *waitLeave = generator.addNewGraphNode(hTime, *pIter,
                        NT_RT_LEAVE | NT_FT_WAITSTATE_CUDA);
                generator.newEdge(syncLeave, waitLeave, false);
            }
            
            hTime += getTimeOffset(100);
            
            return hTime;
        }
    };

}

#endif	/* COLLLATESYNCPATTERN_HPP */

