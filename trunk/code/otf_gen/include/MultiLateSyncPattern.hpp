/* 
 * File:   MultiLateSyncPattern.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:25 AM
 */

#ifndef MULTILATESYNCPATTERN_HPP
#define	MULTILATESYNCPATTERN_HPP

#include <algorithm>
#include "IPattern.hpp"
#include "SingleLateSyncPattern.hpp"

namespace cdm
{

    class MultiLateSyncPattern : public IPattern
    {
    public:

        MultiLateSyncPattern(size_t maxHostProcesses, size_t maxDeviceProcesses, bool hasNullStream) :
        IPattern(maxHostProcesses, maxDeviceProcesses, hasNullStream)
        {

        }
        
        const char *getName()
        {
            return "MultiLateSyncPattern";
        }

        bool getAllocationSize(size_t *numHost, size_t *numDevice, bool *nullStream)
        {
            size_t numProcesses = std::min(maxHostProcesses, maxDeviceProcesses);
            if (numProcesses > 0)
                numProcesses = rand() % numProcesses + 1;
            else
                return false;
            *numHost = numProcesses;
            *numDevice = numProcesses;
            *nullStream = false;
            return true;
        }

    private:
        uint64_t fillInternal(Generator &generator, Allocation &allocation, FunctionTable &functionTable)
        {
            size_t numProcesses = allocation.getHostProcesses().size();
            Allocation::Splittings splittings;
            for (size_t i = 0; i < numProcesses; ++i)
            {
                splittings.push_back(Split(1, 1, false));
            }
            
            bool valid = false;
            uint64_t time = allocation.getStartTime();
            Allocation::AllocationList aList = allocation.split(splittings, &valid);
            if (valid)
            {
                SingleLateSyncPattern slsp(numProcesses, numProcesses, false);
                for (Allocation::AllocationList::const_iterator iter = aList.begin();
                        iter != aList.end(); ++iter)
                {
                    Allocation *alc = *iter;
                    uint64_t tmpTime = slsp.fill(generator, *alc, functionTable);
                    time = std::max(tmpTime, time);
                }
            }
            
            for (Allocation::AllocationList::iterator iter = aList.begin();
                    iter != aList.end(); ++iter)
            {
                delete (*iter);
            }
            
            return time;
        }
    };

}

#endif	/* MULTILATESYNCPATTERN_HPP */

