
#include <algorithm>

#include "Allocation.hpp"

using namespace cdm;

Allocation::Allocation() :
startTime(0),
nullStream(NULL)
{

}

Allocation::Allocation(uint64_t start, const ProcessList& hostProcs,
        const ProcessList& deviceProcs, Process *nullStream)
{
    startTime = start;
    hostProcesses.assign(hostProcs.begin(), hostProcs.end());
    deviceProcesses.assign(deviceProcs.begin(), deviceProcs.end());
    this->nullStream = nullStream;
}

Allocation::~Allocation()
{

}

void Allocation::addHostProcess(Process *p)
{
    hostProcesses.push_back(p);
}

void Allocation::addDeviceProcess(Process *p)
{
    deviceProcesses.push_back(p);
}

Allocation::ProcessList::iterator Allocation::removeHostProcess(Process *p)
{
    for (ProcessList::iterator iter = hostProcesses.begin();
            iter != hostProcesses.end(); ++iter)
    {
        if (*iter == p)
            return hostProcesses.erase(iter);
    }
    
    return hostProcesses.end();
}

void Allocation::setNullStream(Process* p)
{
    nullStream = p;
}

void Allocation::getAllProcesses(ProcessList &procs) const
{
    procs.clear();
    procs.assign(hostProcesses.begin(), hostProcesses.end());
    if (nullStream)
        procs.insert(procs.end(), nullStream);
    procs.insert(procs.end(), deviceProcesses.begin(), deviceProcesses.end());
}

void Allocation::getAllProcesses(ProcessList &procs, GraphNodeType g) const
{
    procs.clear();
    procs.assign(hostProcesses.begin(), hostProcesses.end());
    if (nullStream)
        procs.insert(procs.end(), nullStream);
    procs.insert(procs.end(), deviceProcesses.begin(), deviceProcesses.end());

    for (ProcessList::iterator iter = procs.begin(); iter != procs.end();)
    {
        ProcessList::iterator current = iter;
        iter++;

        Process *p = *current;
        GraphNode *lastGNode = p->getLastGraphNode(g);
        if (!lastGNode || (lastGNode->isProcess()))
            procs.erase(current);
    }
}

const Allocation::ProcessList& Allocation::getDeviceProcesses() const
{
    return deviceProcesses;
}

const Allocation::ProcessList& Allocation::getHostProcesses() const
{
    return hostProcesses;
}

void Allocation::getAllDeviceProcesses(Allocation::ProcessList& deviceProcs) const
{
    deviceProcs.clear();
    if (nullStream)
        deviceProcs.insert(deviceProcs.end(), nullStream);
    deviceProcs.insert(deviceProcs.end(), deviceProcesses.begin(), deviceProcesses.end());
}

Process *Allocation::getNullStream() const
{
    return nullStream;
}

size_t Allocation::getNumProcesses() const
{
    size_t numProcs = hostProcesses.size() + deviceProcesses.size();
    if (nullStream)
        numProcs++;

    return numProcs;
}

size_t Allocation::getNumHostProcesses() const
{
    return hostProcesses.size();
}

size_t Allocation::getNumDeviceProcesses() const
{
    return deviceProcesses.size();
}

uint64_t Allocation::getStartTime() const
{
    return startTime;
}

static bool allocPointLess(Process *p1, Process *p2)
{
    return p1->getLastEventTime() < p2->getLastEventTime();
}

Allocation::AllocationList Allocation::split(const Splittings& splittings, bool *valid)
{
    AllocationList allocList;
    std::sort(hostProcesses.begin(), hostProcesses.end(), allocPointLess);
    std::sort(deviceProcesses.begin(), deviceProcesses.end(), allocPointLess);
    ProcessList::const_iterator hIter = hostProcesses.begin();
    ProcessList::const_iterator dIter = deviceProcesses.begin();

    ProcessList hList;
    ProcessList dList;

    *valid = false;
    uint64_t allocPoint = 0;
    bool foundNullStream = false;

    for (Splittings::const_iterator iter = splittings.begin();
            iter != splittings.end(); ++iter)
    {
        // check if null stream is needed and assign, if possible
        if (iter->nullStream)
        {
            if (foundNullStream)
                return allocList;
            else
            {
                if (nullStream)
                {
                    foundNullStream = true;
                    if (nullStream->getLastEventTime() > allocPoint)
                        allocPoint = nullStream->getLastEventTime();
                } else
                    return allocList;
            }
        }

        // assign host streams
        for (size_t i = 0; i < iter->numHost; ++i)
        {
            if (hIter != hostProcesses.end())
            {
                Process *pHost = *hIter;

                hList.push_back(pHost);
                if (pHost->getLastEventTime() > allocPoint)
                    allocPoint = pHost->getLastEventTime();

                hIter++;
            } else
                return allocList;
        }

        for (size_t h = 0; h < iter->numDevice; ++h)
        {
            if (dIter != deviceProcesses.end())
            {
                Process *pDevice = *dIter;

                dList.push_back(pDevice);
                if (pDevice->getLastEventTime() > allocPoint)
                    allocPoint = pDevice->getLastEventTime();

                dIter++;
            } else
                return allocList;
        }

        Allocation *newAlloc = new Allocation(allocPoint + 1, hList, dList, NULL);
        if (foundNullStream)
            newAlloc->setNullStream(nullStream);
        allocList.push_back(newAlloc);

        hList.clear();
        dList.clear();
    }

    *valid = true;
    return allocList;
}
