/* 
 * File:   FunctionTable.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 10:00 AM
 */

#ifndef FUNCTIONTABLE_HPP
#define	FUNCTIONTABLE_HPP

#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <stdio.h>

#include "graph/Node.hpp"
#include "common.hpp"

#define INVALID_ID 0

namespace cdm
{
    typedef const char** ConstCharPtr;

    typedef struct
    {
        int type;
        const size_t numEntries;
        ConstCharPtr table;
    } FTableEntry;
    
    typedef struct
    {
        Paradigm paradigm;
        int type;
    } FunctionDescriptor;

    static const char * FTABLE_CUDA_COLL_SYNC[] = {
        "cuCtxSynchronize",

        "cuMemcpyDtoD",
        "cuMemcpyDtoH",
        "cuMemcpyDtoH_v2",
        "cuMemcpyHtoD",
        "cuMemcpyHtoD_v2",

        "cuMemAlloc",
        "cuMemAlloc_v2",
        "cuMemAllocHost",
        "cuMemAllocPitch",
        "cuMemHostAlloc",

        "cuMemsetD8",
        "cuMemsetD16",
        "cuMemsetD32",
        "cuMemsetD2D8",
        "cuMemsetD2D16",
        "cuMemsetD2D32",

        "cudaSynchronize"
    };

    static const char * FTABLE_CUDA_SYNC[] = {
        "cuStreamSynchronize",
    };

    static const char * FTABLE_CUDA_QUERY[] = {
        "cuStreamQuery"
    };

    static const char * FTABLE_CUDA_LAUNCH[] = {
        "cuLaunch",
        "cuLaunchGrid",
        "cuLaunchGridAsync",
        "cuLaunchKernel",

        "cudaLaunch"
    };

    static const char * FTABLE_CUDA_EVENT_QUERY[] = {
        "cuEventQuery"
    };

    static const char * FTABLE_CUDA_EVENT_SYNC[] = {
        "cuEventSynchronize"
    };

    static const char * FTABLE_CUDA_EVENT_LAUNCH[] = {
        "cuEventRecord"
    };

    static const char * FTABLE_CUDA_STREAM_WAIT[] = {
        "cuStreamWaitEvent"
    };

    static const char * FTABLE_CUDA_WAITSTATE[] = {
        "__WaitState__"
    };

    static const char * FTABLE_MPI_RECV[] = {
        "MPI_Recv"
    };

    static const char * FTABLE_MPI_SEND[] = {
        "MPI_Send"
    };

    static const char * FTABLE_MPI_WAIT[] = {
        "MPI_Wait"
    };

    static const char * FTABLE_MPI_COLL[] = {
        "MPI_Barrier",
        "MPI_Allreduce",
        "MPI_Allgather",
        "MPI_Gather",
        "MPI_Reduce",
        "MPI_Bcast",
        "MPI_Finalize",
        "MPI_Init"
    };

    static const char * FTABLE_MPI_SENDRECV[] = {
        "MPI_Sendrecv"
    };

    static const char * FTABLE_MPI_MISC[] = {

    };
    
    static const char * FTABLE_VT_FLUSH[] = {
        "flushActivities",
        "sync time"
    };

    static const size_t fTableEntriesCUDA = 9;
    static const FTableEntry fTableCUDA[fTableEntriesCUDA] = {
        {CUDA_COLLSYNC, 7, FTABLE_CUDA_COLL_SYNC},
        {CUDA_SYNC, 1, FTABLE_CUDA_SYNC},
        {CUDA_QUERY, 1, FTABLE_CUDA_QUERY},
        {CUDA_KERNEL_LAUNCH, 5, FTABLE_CUDA_LAUNCH},
        {CUDA_EV_QUERY, 1, FTABLE_CUDA_EVENT_QUERY},
        {CUDA_EV_SYNC, 1, FTABLE_CUDA_EVENT_SYNC},
        {CUDA_EV_LAUNCH, 1, FTABLE_CUDA_EVENT_LAUNCH},
        {CUDA_STREAMWAIT, 1, FTABLE_CUDA_STREAM_WAIT},
        {CUDA_WAITSTATE, 1, FTABLE_CUDA_WAITSTATE}
    };

    static const size_t fTableEntriesMPI = 7;
    static const FTableEntry fTableMPI[fTableEntriesMPI] = {
        {MPI_RECV, 1, FTABLE_MPI_RECV},
        {MPI_SEND, 1, FTABLE_MPI_SEND},
        {MPI_WAIT, 1, FTABLE_MPI_WAIT},
        {MPI_COLL, 8, FTABLE_MPI_COLL},
        {MPI_SENDRECV, 1, FTABLE_MPI_SENDRECV},
        {MPI_MISC, 0, FTABLE_MPI_MISC},
        {MPI_WAITSTATE, 1, FTABLE_CUDA_WAITSTATE}
    };
    
    static const size_t fTableEntriesVT = 1;
    static const FTableEntry fTableVT[fTableEntriesVT] = {
        {VT_FLUSH, 2, FTABLE_VT_FLUSH}
    };

    class FunctionTable
    {
    public:
        typedef std::vector<uint32_t> FunctionIdList;

        FunctionTable() :
        lastFunctionId(INVALID_ID)
        {

        }

        const char * getName(uint32_t id)
        {
            std::map<uint32_t, std::string>::iterator iter = functionNameMap.find(id);
            if (iter != functionNameMap.end())
            {
                return iter->second.c_str();
            } else
                return "__unknown__";
        }

        static bool getAPIFunctionType(const char *name, FunctionDescriptor *descr)
        {
            descr->paradigm = PARADIGM_CPU;
            descr->type = 0;
            
            for (size_t i = 0; i < fTableEntriesCUDA; ++i)
            {
                FTableEntry entry = fTableCUDA[i];
                for (size_t j = 0; j < entry.numEntries; ++j)
                {
                    if (strcmp(entry.table[j], name) == 0)
                    {
                        descr->paradigm = PARADIGM_CUDA;
                        descr->type = entry.type;
                        return true;
                    }
                }
            }
            
            for (size_t i = 0; i < fTableEntriesMPI; ++i)
            {
                FTableEntry entry = fTableMPI[i];
                for (size_t j = 0; j < entry.numEntries; ++j)
                {
                    if (strcmp(entry.table[j], name) == 0)
                    {
                        descr->paradigm = PARADIGM_MPI;
                        descr->type = entry.type;
                        return true;
                    }
                }
            }
            
            for (size_t i = 0; i < fTableEntriesVT; ++i)
            {
                FTableEntry entry = fTableVT[i];
                for (size_t j = 0; j < entry.numEntries; ++j)
                {
                    if (strcmp(entry.table[j], name) == 0)
                    {
                        descr->paradigm = PARADIGM_VT;
                        descr->type = entry.type;
                        return true;
                    }
                }
            }

            return false;
        }

    private:
        std::map<uint32_t, std::string> functionNameMap;
        FunctionIdList hostFunctions;
        FunctionIdList kernels;
        uint32_t invalidId;
        uint32_t lastFunctionId;
    };

}

#endif	/* FUNCTIONTABLE_HPP */

