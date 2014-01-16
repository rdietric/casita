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
        NodeType type;
        const size_t numEntries;
        ConstCharPtr table;
    } FTableEntry;

    static const char * FTABLE_COLL_SYNC[] = {
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

    static const char * FTABLE_SYNC[] = {
        "cuStreamSynchronize",
    };

    static const char * FTABLE_QUERY[] = {
        "cuStreamQuery"
    };

    static const char * FTABLE_LAUNCH[] = {
        "cuLaunch",
        "cuLaunchGrid",
        "cuLaunchGridAsync",
        "cuLaunchKernel",

        "cudaLaunch"
    };

    static const char * FTABLE_EVENT_QUERY[] = {
        "cuEventQuery"
    };

    static const char * FTABLE_EVENT_SYNC[] = {
        "cuEventSynchronize"
    };

    static const char * FTABLE_EVENT_LAUNCH[] = {
        "cuEventRecord"
    };

    static const char * FTABLE_STREAM_WAIT[] = {
        "cuStreamWaitEvent"
    };

    static const char * FTABLE_WAITSTATE[] = {
        "__WaitState__"
    };

    static const char * FTABLE_MARKER[] = {
        "__Marker__"
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
    
    static const size_t fTableEntries = 17;
    
    static const FTableEntry fTable[fTableEntries] = {
        {NT_FT_COLLSYNC, 7, FTABLE_COLL_SYNC},
        {NT_FT_SYNC, 1, FTABLE_SYNC},
        {NT_FT_QUERY, 1, FTABLE_QUERY},
        {NT_FT_LAUNCH, 5, FTABLE_LAUNCH},
        {NT_FT_EV_QUERY, 1, FTABLE_EVENT_QUERY},
        {NT_FT_EV_SYNC, 1, FTABLE_EVENT_SYNC},
        {NT_FT_EV_LAUNCH, 1, FTABLE_EVENT_LAUNCH},
        {NT_FT_STREAMWAIT, 1, FTABLE_STREAM_WAIT},
        {NT_FT_WAITSTATE_CUDA, 1, FTABLE_WAITSTATE},
        {NT_FT_WAITSTATE_MPI, 1, FTABLE_WAITSTATE},
        {NT_FT_MARKER, 1, FTABLE_MARKER},
        
        {NT_FT_MPI_RECV, 1, FTABLE_MPI_RECV},
        {NT_FT_MPI_SEND, 1, FTABLE_MPI_SEND},
        {NT_FT_MPI_WAIT, 1, FTABLE_MPI_WAIT},
        {NT_FT_MPI_COLL, 8, FTABLE_MPI_COLL},
        {NT_FT_MPI_SENDRECV, 1, FTABLE_MPI_SENDRECV},
        {NT_FT_MPI_MISC, 0, FTABLE_MPI_MISC}
    };

    class FunctionTable
    {
    public:
        typedef std::vector<uint32_t> FunctionIdList;

        FunctionTable() :
        lastFunctionId(INVALID_ID)
        {

        }

        void generateFunctions(uint32_t numHostFunctions, uint32_t numKernels)
        {
            for (uint32_t i = 0; i < numHostFunctions; ++i)
                createFunction("foo", i, hostFunctions);

            for (uint32_t i = 0; i < numKernels; ++i)
                createFunction("kernel", i, kernels);
            
            for (size_t i = 0; i < fTableEntries; ++i)
            {
                FTableEntry entry = fTable[i];
                for (size_t j = 0; j < entry.numEntries; ++j)
                {
                    createAPIFunction(entry.type, entry.table[j]);
                }
            }
        }

        uint32_t getRandomFunction(int nodeType)
        {
            if (nodeType & NT_FT_CPU)
                return hostFunctions[rand() % hostFunctions.size()];

            if (nodeType & NT_FT_KERNEL)
                return kernels[rand() % kernels.size()];

            NodeType functionType = NT_FT_CPU;
            do
            {
                if (nodeType & NT_FT_COLLSYNC)
                {
                    functionType = NT_FT_COLLSYNC;
                    break;
                }

                if (nodeType & NT_FT_SYNC)
                {
                    functionType = NT_FT_SYNC;
                    break;
                }

                if (nodeType & NT_FT_EV_SYNC)
                {
                    functionType = NT_FT_EV_SYNC;
                    break;
                }

                if (nodeType & NT_FT_QUERY)
                {
                    functionType = NT_FT_QUERY;
                    break;
                }

                if (nodeType & NT_FT_EV_QUERY)
                {
                    functionType = NT_FT_EV_QUERY;
                    break;
                }

                if (nodeType & NT_FT_LAUNCH)
                {
                    functionType = NT_FT_LAUNCH;
                    break;
                }

                if (nodeType & NT_FT_EV_LAUNCH)
                {
                    functionType = NT_FT_EV_LAUNCH;
                    break;
                }

                if (nodeType & NT_FT_WAITSTATE_CUDA)
                {
                    functionType = NT_FT_WAITSTATE_CUDA;
                    break;
                }
                
                if (nodeType & NT_FT_WAITSTATE_MPI)
                {
                    functionType = NT_FT_WAITSTATE_MPI;
                    break;
                }

                if (nodeType & NT_FT_STREAMWAIT)
                {
                    functionType = NT_FT_STREAMWAIT;
                    break;
                }
            } while (0);

            if (functionType == NT_FT_CPU)
            {
                RTException("Cannot determine type of node");
            }

            std::map<NodeType, FunctionIdList >::iterator iter = apiFunctions.find(functionType);
            if (iter == apiFunctions.end())
                return INVALID_ID;

            FunctionIdList specialFuncList = iter->second;
            if (specialFuncList.size() > 0)
                return specialFuncList[rand() % specialFuncList.size()];
            else
                return INVALID_ID;
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

        static NodeType getAPIFunctionType(const char *name)
        {
            for (size_t i = 0; i < fTableEntries; ++i)
            {
                FTableEntry entry = fTable[i];
                for (size_t j = 0; j < entry.numEntries; ++j)
                {
                    if (strcmp(entry.table[j], name) == 0)
                        return entry.type;
                }
            }

            return NT_FT_CPU;
        }

    private:
        std::map<uint32_t, std::string> functionNameMap;
        FunctionIdList hostFunctions;
        FunctionIdList kernels;
        std::map<NodeType, FunctionIdList > apiFunctions;
        uint32_t invalidId;
        uint32_t lastFunctionId;

        void createFunction(const char *base, uint32_t offset, std::vector<uint32_t> &list)
        {
            lastFunctionId++;

            std::stringstream stream;
            stream << base << "_" << offset;
            functionNameMap[lastFunctionId] = stream.str();
            list.push_back(lastFunctionId);
        }

        void createAPIFunction(NodeType functionType, const char *name)
        {
            lastFunctionId++;
            functionNameMap[lastFunctionId] = name;
            apiFunctions[functionType].push_back(lastFunctionId);
        }
    };

}

#endif	/* FUNCTIONTABLE_HPP */

