/* 
 * File:   OTF2TraceWriter.cpp
 * Author: felix
 * 
 * Created on May 15, 2013, 7:28 PM
 */

#if defined OTF2_ENABLED

#include <sys/time.h>

#include "otf/OTF2TraceWriter.hpp"

using namespace cdm::io;

OTF2_FlushType pre_flush(void* userData,
        OTF2_FileType fileType,
        OTF2_LocationRef location,
        void* callerData,
        bool final)
{
    return OTF2_FLUSH;
}

OTF2_TimeStamp post_flush(void* userData,
        OTF2_FileType fileType,
        OTF2_LocationRef location)
{
    timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_usec + tv.tv_sec * 1000 * 1000;
}

OTF2_FlushCallbacks flush_callbacks = {
    .otf2_pre_flush = pre_flush,
    .otf2_post_flush = post_flush
};

OTF2TraceWriter::OTF2TraceWriter(const char *streamRefKeyName, const char *eventIdKeyName) :
ITraceWriter(streamRefKeyName, eventIdKeyName),
globalStringId(0),
archive(NULL),
eventWriter(NULL),
defWriter(NULL),
globDefWriter(NULL)
{
}

OTF2TraceWriter::~OTF2TraceWriter()
{
}

uint32_t OTF2TraceWriter::writeString(const char* str)
{
    uint32_t oldId = globalStringId;
    OTF2_GlobalDefWriter_WriteString(globDefWriter, globalStringId, str);
    globalStringId++;
    return oldId;
}

void OTF2TraceWriter::open(const std::string otfFilename, uint32_t maxFiles)
{
    archive = OTF2_Archive_Open("", otfFilename.c_str(), OTF2_FILEMODE_WRITE,
            OTF2_CHUNK_SIZE_EVENTS_DEFAULT, OTF2_CHUNK_SIZE_DEFINITIONS_DEFAULT,
            OTF2_SUBSTRATE_POSIX, OTF2_COMPRESSION_NONE);

    eventWriter = OTF2_Archive_GetEvtWriter(archive, 0);
    defWriter = OTF2_Archive_GetDefWriter(archive, 0);
    globDefWriter = OTF2_Archive_GetGlobalDefWriter(archive);

    OTF2_Archive_SetFlushCallbacks(archive, &flush_callbacks, NULL);
    OTF2_Archive_SetMasterSlaveMode(archive, OTF2_MASTER);

    writeString("");

    OTF2_GlobalDefWriter_WriteSystemTreeNode(globDefWriter, 0, writeString("Machine"),
            0, OTF2_UNDEFINED_SYSTEM_TREE_NODE);

    /*OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) FG_APPLICATION,
            writeString("Application"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);
    OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) FG_CUDA_API,
            writeString("CUDA API"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);
    OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) FG_KERNEL,
            writeString("CUDA Kernels"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);
    OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) FG_WAITSTATE,
            writeString("WaitStates"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);*/

    OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) PG_HOST,
            writeString("Host"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);
    OTF2_GlobalDefWriter_WriteLocationGroup(globDefWriter, (uint32_t) PG_DEVICE,
            writeString("Device"), OTF2_LOCATION_GROUP_TYPE_PROCESS, 0);
}

void OTF2TraceWriter::close()
{
    OTF2_Archive_Close(archive);
}

void OTF2TraceWriter::writeDefFunction(uint64_t id, const char* name, FunctionGroup fg)
{
    OTF2_Paradigm paradigm = OTF2_PARADIGM_UNKNOWN;
    if (fg == FG_CUDA_API)
        paradigm = OTF2_PARADIGM_CUDA;

    OTF2_GlobalDefWriter_WriteRegion(globDefWriter, id, writeString(name), 0, 0,
            OTF2_REGION_ROLE_FUNCTION, paradigm, OTF2_REGION_FLAG_NONE, 0, 0, 0);
}

void OTF2TraceWriter::writeDefProcess(uint64_t id, const char* name, ProcessGroup pg)
{
    /* uint32_t pValue = 0;
     if (pg == PG_DEVICE)
         pValue = 1;

     //OTF_KeyValueList_appendUint32(kvList, 1, pValue);

     OTF2_LocationType locType = OTF2_LOCATION_TYPE_CPU_THREAD;
     if (pg == PG_DEVICE)
         locType = OTF2_LOCATION_TYPE_GPU;

     OTF2_GlobalDefWriter_WriteLocation(globDefWriter, id, writeString(name), 
           locType, 0, )*/
}

void OTF2TraceWriter::writeNode(const Node *node)
{
    /*if (node->isEnter())
    {
        if (node->getReferencedProcesses().size() == 1)
            OTF_KeyValueList_appendUint32(kvList, 1, node->getReferencedProcesses()[0]);

        OTF_Writer_writeEnterKV(writer, node->getTime(), node->getFunctionId(),
                node->getProcessId(), 0, kvList);
    }

    if (node->isLeave())
    {
        OTF_Writer_writeLeave(writer, node->getTime(), node->getFunctionId(),
                node->getProcessId(), 0);
    }

    if (node->isMarker())
    {
        OTF_Writer_writeMarker(writer, node->getTime(), node->getProcessId(),
                MG_Marker, node->getName());
    }*/
}

#endif /* OTF2_ENABLED */
