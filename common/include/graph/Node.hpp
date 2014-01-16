/* 
 * File:   Node.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 12:10 PM
 */

#ifndef NODE_HPP
#define	NODE_HPP

#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

namespace cdm
{
    static uint32_t globalNodeId = 0;
    //static const uint32_t REF_ALL_PROCESS_ID = 0;

    typedef uint32_t LocalID;
    typedef uint64_t GlobalID;

    enum NodeType
    {
        // record types
        NT_RT_ENTER = 1 << 1,
        NT_RT_LEAVE = 1 << 2,
        NT_RT_ATOMIC = 1 << 3,
        // function types
        NT_FT_CPU = 1 << 4,
        // CUDA
        NT_FT_SYNC = 1 << 5,
        NT_FT_COLLSYNC = 1 << 6,
        NT_FT_EV_SYNC = 1 << 7,
        NT_FT_WAITSTATE_CUDA = 1 << 8,
        NT_FT_KERNEL = 1 << 9,
        NT_FT_PROCESS = 1 << 10,
        NT_FT_LAUNCH = 1 << 11,
        NT_FT_EV_LAUNCH = 1 << 12,
        NT_FT_MARKER = 1 << 13,
        NT_FT_QUERY = 1 << 14,
        NT_FT_EV_QUERY = 1 << 15,
        NT_FT_STREAMWAIT = 1 << 16,
        // MPI
        NT_FT_MPI_RECV = 1 << 17,
        NT_FT_MPI_SEND = 1 << 18,
        NT_FT_MPI_WAIT = 1 << 19,
        NT_FT_MPI_COLL = 1 << 20,
        NT_FT_MPI_SENDRECV = 1 << 21,
        NT_FT_MPI_MISC = 1 << 22,
        NT_RT_MPI_EXIT = 1 << 23,
        NT_FT_WAITSTATE_MPI = 1 << 24,

        NT_FT_MIXED = 1 << 31
    };

    typedef struct
    {
        NodeType type;
        const char * str;
    } TypeStrEntry;

    static const size_t numTypeStrEntries = 21;
    static const TypeStrEntry typeStrTable[numTypeStrEntries] = {
        {NT_FT_CPU, "cpu"},

        {NT_FT_COLLSYNC, "collsync"},
        {NT_FT_SYNC, "sync"},
        {NT_FT_EV_SYNC, "event_sync"},
        {NT_FT_KERNEL, "kernel"},
        {NT_FT_LAUNCH, "launch"},
        {NT_FT_EV_LAUNCH, "event_launch"},
        {NT_FT_PROCESS, "process"},
        {NT_FT_WAITSTATE_CUDA, "waitstate_cuda"},
        {NT_FT_MARKER, "marker"},
        {NT_FT_QUERY, "query"},
        {NT_FT_EV_QUERY, "event_query"},
        {NT_FT_STREAMWAIT, "streamwait"},

        {NT_FT_MPI_RECV, "mpi_recv"},
        {NT_FT_MPI_SEND, "mpi_send"},
        {NT_FT_MPI_WAIT, "mpi_wait"},
        {NT_FT_MPI_COLL, "mpi_coll"},
        {NT_FT_MPI_SENDRECV, "mpi_sendrecv"},
        {NT_FT_MPI_MISC, "mpi_misc"},
        {NT_RT_MPI_EXIT, "mpi_exit"},
        {NT_FT_WAITSTATE_MPI, "waitstate_mpi"},
    };

    static const char NAME_WAITSTATE[] = "WaitState";
    static const char NAME_MPI_INIT[] = "MPI_Init";
    static const char NAME_MPI_FINALIZE[] = "MPI_Finalize";

    class Node
    {
    public:

        typedef std::list<Node*> NodeList;

        bool isEnter() const
        {
            return nodeType & NT_RT_ENTER;
        }

        bool isLeave() const
        {
            return nodeType & NT_RT_LEAVE;
        }

        bool isAtomic() const
        {
            return nodeType & NT_RT_ATOMIC;
        }

        bool isCPU() const
        {
            return nodeType & NT_FT_CPU;
        }

        bool isSync() const
        {
            return nodeType & NT_FT_SYNC;
        }

        bool isCollSync() const
        {
            return nodeType & NT_FT_COLLSYNC;
        }

        bool isEventSync() const
        {
            return nodeType & NT_FT_EV_SYNC;
        }

        bool isWaitstate() const
        {
            return (nodeType & NT_FT_WAITSTATE_CUDA) ||
                    (nodeType & NT_FT_WAITSTATE_MPI);
        }

        bool isPureWaitstate() const
        {
            return isWaitstate() &&
                    (strcmp(name.c_str(), NAME_WAITSTATE) == 0);
        }

        bool isProcess() const
        {
            return nodeType & NT_FT_PROCESS;
        }

        bool isKernel() const
        {
            return nodeType & NT_FT_KERNEL;
        }

        bool isKernelLaunch() const
        {
            return nodeType & NT_FT_LAUNCH;
        }

        bool isEventLaunch() const
        {
            return nodeType & NT_FT_EV_LAUNCH;
        }

        bool isMarker() const
        {
            return nodeType & NT_FT_MARKER;
        }

        bool isQuery() const
        {
            return nodeType & NT_FT_QUERY;
        }

        bool isEventQuery() const
        {
            return nodeType & NT_FT_EV_QUERY;
        }

        bool isStreamWaitEvent() const
        {
            return nodeType & NT_FT_STREAMWAIT;
        }

        bool isCUDA() const
        {
            return isCUDAType(nodeType);
        }

        bool isMPI() const
        {
            return isMPIType(nodeType);
        }

        bool isMPIRecv() const
        {
            return nodeType & NT_FT_MPI_RECV;
        }

        bool isMPISend() const
        {
            return nodeType & NT_FT_MPI_SEND;
        }
        
        bool isMPIWait() const
        {
            return nodeType & NT_FT_MPI_WAIT;
        }
        
        bool isMPIInit() const
        {
            return (nodeType & NT_FT_MPI_COLL) &&
                    (strcmp(name.c_str(), NAME_MPI_INIT) == 0);
        }

        bool isMPICollective() const
        {
            return nodeType & NT_FT_MPI_COLL;
        }
        
        bool isMPISendRecv() const
        {
            return nodeType & NT_FT_MPI_SENDRECV;
        }

        bool isMPIFinalize() const
        {
            return (nodeType & NT_FT_MPI_COLL) &&
                    (strcmp(name.c_str(), NAME_MPI_FINALIZE) == 0);
        }

        static const std::string typeToStr(int type)
        {
            std::stringstream stream;

            for (size_t i = 0; i < numTypeStrEntries; ++i)
                if (type & typeStrTable[i].type)
                    stream << typeStrTable[i].str;

            return stream.str();
        }

        static bool isEventType(int type)
        {
            return ((type & NT_FT_EV_LAUNCH) ||
                    (type & NT_FT_EV_SYNC) ||
                    (type & NT_FT_EV_QUERY) ||
                    (type & NT_FT_STREAMWAIT));
        }

        static bool isCUDAType(int type)
        {
            return (type & NT_FT_SYNC) ||
                    (type & NT_FT_COLLSYNC) ||
                    (type & NT_FT_EV_SYNC) ||
                    (type & NT_FT_WAITSTATE_CUDA) ||
                    (type & NT_FT_KERNEL) ||
                    (type & NT_FT_PROCESS) ||
                    (type & NT_FT_LAUNCH) ||
                    (type & NT_FT_EV_LAUNCH) ||
                    (type & NT_FT_MARKER) ||
                    (type & NT_FT_QUERY) ||
                    (type & NT_FT_EV_QUERY) ||
                    (type & NT_FT_STREAMWAIT) ||
                    (type & NT_FT_MIXED);
        }

        static bool isMPIType(int type)
        {
            return (type & NT_FT_MPI_RECV) ||
                    (type & NT_FT_MPI_SEND) ||
                    (type & NT_FT_MPI_WAIT) ||
                    (type & NT_FT_MPI_COLL) ||
                    (type & NT_FT_MPI_SENDRECV) ||
                    (type & NT_FT_MPI_MISC) ||
                    (type & NT_RT_MPI_EXIT) ||
                    (type & NT_FT_MIXED) ||
                    (type & NT_FT_WAITSTATE_MPI);
        }

        static bool compareLess(const Node *n1, const Node *n2)
        {
            uint64_t time1 = n1->getTime();
            uint64_t time2 = n2->getTime();

            //printf("< (%s, %s)\n", n1->getUniqueName().c_str(), n2->getUniqueName().c_str());

            if (time1 != time2)
            {
                return time1 < time2;
            } else
            {
                int type1 = n1->getType();
                int type2 = n2->getType();

                if (n1->isProcess())
                    return true;

                if (n2->isProcess())
                    return false;

                // nodes from same or different processes
                if (n1->getProcessId() == n2->getProcessId())
                {
                    // nodes from same or different functions
                    if (((n1->getFunctionId() > 0) && (n2->getFunctionId() > 0) &&
                            (n1->getFunctionId() == n2->getFunctionId())) ||
                            ((n1->getFunctionId() == n2->getFunctionId()) &&
                            strcmp(n1->getName(), n2->getName()) == 0))
                    {
                        if ((type1 & NT_RT_LEAVE) && (type2 & NT_RT_ENTER))
                            return false;

                        if ((type1 & NT_RT_ENTER) && (type2 & NT_RT_LEAVE))
                            return true;
                    } else
                    {
                        if ((type1 & NT_RT_LEAVE) && (type2 & NT_RT_ENTER))
                            return true;

                        if ((type1 & NT_RT_ENTER) && (type2 & NT_RT_LEAVE))
                            return false;
                    }

                    if ((type1 & NT_RT_ENTER) && (type2 & NT_FT_MARKER))
                        return false;

                    if ((type1 & NT_FT_MARKER) && (type2 & NT_RT_ENTER))
                        return true;
                } else
                {
                    if ((type1 & NT_FT_KERNEL) && (type2 & NT_FT_SYNC))
                        return true;

                    if ((type2 & NT_FT_KERNEL) && (type1 & NT_FT_SYNC))
                        return false;
                }

                if (type1 & NT_FT_MARKER)
                    return false;

                if (type2 & NT_FT_MARKER)
                    return true;

                if (type1 & NT_RT_MPI_EXIT)
                    return false;

                if (type2 & NT_RT_MPI_EXIT)
                    return true;

                return n1->getProcessId() > n2->getProcessId();
            }
        }

        Node(uint64_t time, uint32_t processId, const std::string name, int nodeType) :
        time(time),
        processId(processId),
        functionId(0),
        name(name),
        nodeType(nodeType),
        link(NULL),
        referencedProcess(0)
        {
            id = ++globalNodeId;
        }

        virtual ~Node()
        {
        }

        uint32_t getId() const
        {
            return id;
        }

        uint64_t getTime() const
        {
            return time;
        }

        uint32_t getProcessId() const
        {
            return processId;
        }

        const char *getName() const
        {
            return name.c_str();
        }

        virtual void setName(const std::string newName)
        {
            name = newName;
        }

        virtual const std::string getUniqueName() const
        {
            std::stringstream sstream;
            sstream << name << ".";

            if (nodeType & NT_RT_ENTER)
                sstream << "enter.";

            if (nodeType & NT_RT_LEAVE)
                sstream << "leave.";

            sstream << id << "." << time << "." << processId;
            return sstream.str();
        }

        const char *toString() const
        {
            return getUniqueName().c_str();
        }

        uint32_t getFunctionId() const
        {
            return functionId;
        }

        void setFunctionId(uint32_t newId)
        {
            functionId = newId;
        }

        int getType() const
        {
            return nodeType;
        }

        void setReferencedProcessId(uint32_t processId)
        {
            referencedProcess = processId;
        }

        bool referencesProcess(uint32_t processId) const
        {
            if (processId == this->processId)
                return false;

            if (isCollSync() || (referencedProcess == processId))
                return true;

            return false;
        }

        uint32_t getReferencedProcessId() const
        {
            return referencedProcess;
        }

        virtual bool isGraphNode() const
        {
            return false;
        }

        virtual bool isEventNode() const
        {
            return false;
        }

        virtual bool isRemoteNode() const
        {
            return false;
        }

        void setLink(Node * link)
        {
            if (this->link)
                assert(0);
            this->link = link;
        }

        Node * getLink() const
        {
            return link;
        }

        void setCounter(uint32_t ctrId, uint64_t value)
        {
            counters[ctrId] = value;
        }

        void incCounter(uint32_t ctrId, uint64_t value)
        {
            if (counters.find(ctrId) != counters.end())
                counters[ctrId] += value;
            else
                counters[ctrId] = value;
        }

        uint64_t getCounter(uint32_t ctrId, bool* valid) const
        {
            std::map<uint32_t, uint64_t>::const_iterator iter =
                    counters.find(ctrId);
            if (iter == counters.end())
            {
                if (valid)
                    *valid = false;
                return 0;
            }

            if (valid)
                *valid = true;
            return iter->second;
        }

        void removeCounter(uint32_t ctrId)
        {
            counters.erase(ctrId);
        }

        void removeCounters()
        {
            counters.clear();
        }

    protected:
        uint32_t id;
        uint64_t time;
        uint32_t processId;
        uint32_t functionId;
        std::string name;
        int nodeType;
        Node *link;
        /**
         * Link mappings:
         * - KernelLaunch/enter > Kernel/enter
         * - Kernel/enter > KernelLaunch/enter
         * - EventLaunch/leave > KernelLaunch/leave
         * - EventQuery/leave > EventQuery/leave
         * - StreamWaitEvent/leave > EventLaunch/leave
         */
        uint32_t referencedProcess;
        std::map<uint32_t, uint64_t> counters;
    };

    typedef struct
    {

        bool operator()(const Node *n1, const Node *n2) const
        {
            return Node::compareLess(n1, n2);
        }

    } nodeCompareLess;
}

#endif	/* NODE_HPP */

