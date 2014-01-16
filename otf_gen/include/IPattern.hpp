/* 
 * File:   IPattern.hpp
 * Author: felix
 *
 * Created on May 13, 2013, 2:02 PM
 */

#ifndef IPATTERN_HPP
#define	IPATTERN_HPP

#include <stdlib.h>
#include <stdio.h>
#include "Allocation.hpp"
#include "FunctionTable.hpp"
#include "Generator.hpp"

namespace cdm
{

    class IPattern
    {
    public:

        IPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        maxHostProcesses(maxHostProcesses),
        maxDeviceProcesses(maxDeviceProcesses),
        hasNullStream(hasNullStream)
        {

        }
        
        virtual ~IPattern()
        {
            
        }

        virtual const char *getName() = 0;

        virtual bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream) = 0;

        uint64_t fill(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            Allocation::ProcessList processes;
            allocation.getAllProcesses(processes);
            
            printf("Pattern %s, %lu processes\n", getName(), processes.size());
            
            for (Allocation::ProcessList::const_iterator iter = processes.begin();
                    iter != processes.end(); ++iter)
            {
                printf(" %s\n", (*iter)->getName());
            }
            
            uint64_t lastTime = fillInternal(generator, allocation, functionTable);

            return lastTime + 1;
        }

        static uint64_t getTimeOffset(uint32_t range)
        {
            return rand() % (range * TICKS_MIN_INC) + 1;
        }

    protected:
        size_t maxHostProcesses;
        size_t maxDeviceProcesses;
        bool hasNullStream;

        virtual uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable) = 0;
    };

}

#endif	/* IPATTERN_HPP */

