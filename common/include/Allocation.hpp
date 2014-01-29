/* 
 * File:   Allocation.hpp
 * Author: felix
 *
 * Created on May 13, 2013, 1:58 PM
 */

#ifndef ALLOCATION_HPP
#define	ALLOCATION_HPP

#include <vector>
#include <cstddef>
#include "Process.hpp"

namespace cdm
{

    class Split
    {
    public:

        Split(size_t numHost, size_t numDevice, bool nullStream) :
        numHost(numHost),
        numDevice(numDevice),
        nullStream(nullStream)
        {

        }

        size_t numHost;
        size_t numDevice;
        bool nullStream;
    };

    class Allocation
    {
    public:

        typedef std::vector<Process*> ProcessList;
        typedef std::vector<Allocation*> AllocationList;
        typedef std::vector<Split> Splittings;

        Allocation();
        Allocation(uint64_t start, const ProcessList& hostProcs,
                const ProcessList& deviceProcs, Process *nullStream);
        virtual ~Allocation();

        AllocationList split(const Splittings& splittings, bool *valid);

        void addHostProcess(Process *p);
        void addDeviceProcess(Process *p);
        ProcessList::iterator removeHostProcess(Process *p);
        void setNullStream(Process *p);

        void getAllProcesses(ProcessList &procs) const;
        void getAllProcesses(ProcessList &procs, Paradigm paradigm) const;
        const ProcessList& getHostProcesses() const;
        const ProcessList& getDeviceProcesses() const;
        void getAllDeviceProcesses(ProcessList& deviceProcs) const;
        Process *getNullStream() const;
        
        size_t getNumProcesses() const;
        size_t getNumHostProcesses() const;
        size_t getNumDeviceProcesses() const;

        uint64_t getStartTime() const;

    private:
        uint64_t startTime;
        ProcessList hostProcesses;
        ProcessList deviceProcesses;
        Process *nullStream;
    };

}

#endif	/* ALLOCATION_HPP */

