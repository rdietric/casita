/* 
 * File:   OTF1ParallelTraceWriter.cpp
 * Author: felix
 * 
 * Created on 31. Juli 2013, 12:33
 */

#include <mpi.h>
#include <cmath>

#include "otf/OTF1ParallelTraceWriter.hpp"
#include "cuda.h"
#include "graph/EventNode.hpp"
#include "CounterTable.hpp"
#include "common.hpp"
#include "FunctionTable.hpp"
#include "otf/ITraceReader.hpp"
#include "include/Process.hpp"

using namespace cdm;
using namespace cdm::io;

#define OTF_CHECK(cmd) \
        { \
                int _status = cmd; \
                if (!_status) \
                        throw RTException("OTF command '%s' returned error %d", #cmd, _status); \
        }

#define MPI_CHECK(cmd) \
        { \
                int mpi_result = cmd; \
                if (mpi_result != MPI_SUCCESS) \
                        throw RTException("MPI error %d in call %s", mpi_result, #cmd); \
        }

OTF1ParallelTraceWriter::OTF1ParallelTraceWriter(const char *streamRefKeyName,
        const char *eventRefKeyName,
        const char *funcResultKeyName,
        uint32_t mpiRank,
        uint32_t mpiSize,
        const char *originalFilename) :
IParallelTraceWriter(streamRefKeyName, eventRefKeyName, funcResultKeyName,
mpiRank, mpiSize),
totalNumStreams(0),
fileMgr(NULL),
kvList(NULL),
globalWriter(NULL),
streamRefKey(1),
eventRefKey(2),
funcResultKey(3),
attrListCUDAToken(1),
attrListCUDAMasterToken(2)
{
    mpiNumProcesses = new int[mpiSize];
    outputFilename.assign("");
    this->originalFilename.assign(originalFilename);
    this->originalFilename.erase(this->originalFilename.size() - 4, 4);
}

OTF1ParallelTraceWriter::~OTF1ParallelTraceWriter()
{
    delete[] mpiNumProcesses;
}

static int handleDefProcess(void * userData, uint32_t stream, uint64_t processId,
        const char * name, uint64_t parent, OTF_KeyValueList_struct * list)
{
    uint32_t otf1_processId = (uint32_t)processId;
    uint32_t otf1_parent = (uint32_t)parent;
    
    WriterData *writerData = (WriterData*) userData;

    OTF_CHECK(OTF_Writer_writeDefProcess(writerData->writer, stream,
            otf1_processId, name, otf1_parent));

    return OTF_RETURN_OK;
}

static int handleDefProcessGroup(void *userData, uint32_t stream,
        uint32_t procGroup, const char *name, uint32_t numberOfProcs,
        const uint64_t *procs, OTF_KeyValueList *list)
{
    uint32_t otf1_procs[numberOfProcs];
    for(uint32_t i=0; i<numberOfProcs;++i)
    {
        otf1_procs[i] = (uint32_t) procs[i];
    }
    
    WriterData *writerData = (WriterData*) userData;

    OTF_CHECK(OTF_Writer_writeDefProcessGroupKV(writerData->writer,
            stream, procGroup, name, numberOfProcs, otf1_procs, list));

    return OTF_RETURN_OK;
}

static int handleDefFunction(void *userData, uint32_t stream,
        uint32_t func, const char *name, uint32_t funcGroup,
        uint32_t source, OTF_KeyValueList *list)
{
    WriterData *writerData = (WriterData*) userData;

    OTF_CHECK(OTF_Writer_writeDefFunctionKV(writerData->writer, stream,
            func, name, funcGroup, source, list));

    writerData->maxFunctionID = std::max(writerData->maxFunctionID, func);

    return OTF_RETURN_OK;
}

static int handleDefFunctionGroup(void * userData, uint32_t stream,
        uint32_t funcGroup, const char * name, OTF_KeyValueList * list)
{
    WriterData *writerData = (WriterData*) userData;

    OTF_CHECK(OTF_Writer_writeDefFunctionGroupKV(writerData->writer,
            stream, funcGroup, name, list));

    return OTF_RETURN_OK;
}

static int handleDefTimerResolution(void *userData, uint32_t stream,
        uint64_t ticksPerSecond, OTF_KeyValueList *list)
{
    WriterData *writerData = (WriterData*) userData;
    
    writerData->timerResolution = ticksPerSecond;

    OTF_CHECK(OTF_Writer_writeDefTimerResolution(writerData->writer,
            stream, ticksPerSecond));

    return OTF_RETURN_OK;
}

static int handleDefAttributeList(void *userData, uint32_t stream,
        uint32_t attr_token, uint32_t num, OTF_ATTR_TYPE *array, OTF_KeyValueList *list)
{
    WriterData *writerData = (WriterData*) userData;

    OTF_CHECK(OTF_Writer_writeDefAttributeListKV(writerData->writer,
            stream, attr_token, num, array, list));

    return OTF_RETURN_OK;
}

void OTF1ParallelTraceWriter::copyGlobalDefinitions(OTF_Writer *writer)
{
    OTF_Reader *reader = OTF_Reader_open(originalFilename.c_str(), fileMgr);

    WriterData writerData;
    memset(&writerData, 0, sizeof (WriterData));
    writerData.writer = writer;

    OTF_HandlerArray* handlers = OTF_HandlerArray_open();
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefTimerResolution,
            OTF_DEFTIMERRESOLUTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFTIMERRESOLUTION_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefProcess,
            OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFPROCESS_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefProcessGroup,
            OTF_DEFPROCESSGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFPROCESSGROUP_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefFunction,
            OTF_DEFFUNCTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFFUNCTION_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefFunctionGroup,
            OTF_DEFFUNCTIONGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFFUNCTIONGROUP_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) handleDefAttributeList,
            OTF_DEFATTRLIST_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, &writerData, OTF_DEFATTRLIST_RECORD);

    if (OTF_Reader_readDefinitions(reader, handlers) == OTF_READ_ERROR)
        throw RTException("Failed to read OTF definitions part");
    OTF_HandlerArray_close(handlers);

    OTF_CHECK(OTF_Reader_close(reader));

    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, streamRefKey, OTF_UINT32,
            streamRefKeyName, "Referenced CUDA stream"));
    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, eventRefKey, OTF_UINT32,
            eventRefKeyName, "Referenced CUDA event"));
    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, funcResultKey, OTF_UINT32,
            funcResultKeyName, "CUDA API function result"));
    
    timerResolution = writerData.timerResolution;
}

void OTF1ParallelTraceWriter::copyMasterControl()
{
    OTF_MasterControl* mc = OTF_MasterControl_new(fileMgr);

    OTF_CHECK(OTF_MasterControl_read(mc, originalFilename.c_str()));
    OTF_CHECK(OTF_MasterControl_write(mc, outputFilename.c_str()));

    OTF_MasterControl_close(mc);
}

void OTF1ParallelTraceWriter::open(const std::string otfFilename, uint32_t maxFiles,
        uint32_t numStreams, uint64_t timerResolution)
{
    fileMgr = OTF_FileManager_open(maxFiles);
    kvList = OTF_KeyValueList_new();

    MPI_CHECK(MPI_Allgather(&numStreams, 1, MPI_UNSIGNED,
            mpiNumProcesses, 1, MPI_INT, MPI_COMM_WORLD));
    for (uint32_t i = 0; i < mpiSize; ++i)
    {
        totalNumStreams += mpiNumProcesses[i];
    }

    outputFilename.append(otfFilename.c_str(), otfFilename.length() - 4);

    if (mpiRank == 0)
    {
        globalWriter = OTF_Writer_open(outputFilename.c_str(), totalNumStreams, fileMgr);
        copyGlobalDefinitions(globalWriter);
    }

    MPI_CHECK(MPI_Barrier(MPI_COMM_WORLD));
}

void OTF1ParallelTraceWriter::close()
{
    // close global writer
    if (mpiRank == 0)
    {
        OTF_CHECK(OTF_Writer_close(globalWriter));
        copyMasterControl();
    }

    // close all local writers
    for (std::map<uint32_t, OTF_WStream_ptr>::const_iterator iter = processWStreamMap.begin();
            iter != processWStreamMap.end(); ++iter)
    {
        OTF_CHECK(OTF_WStream_close(iter->second));
    }

    OTF_KeyValueList_close(kvList);
    OTF_FileManager_close(fileMgr);
}

void OTF1ParallelTraceWriter::writeDefProcess(uint64_t id, uint64_t parentId,
        const char* name, ProcessGroup pg)
{
    
    uint32_t otf1_id = (uint32_t) id;
    uint32_t otf1_parentId = (uint32_t) parentId;
    
    if (pg == PG_DEVICE)
        deviceProcesses.push_back(otf1_id);
    if (pg == PG_DEVICE_NULL)
    {
        deviceProcesses.push_back(otf1_id);
        deviceMasterProcesses.push_back(otf1_id);
    }

    // create local writer for process id
    OTF_WStream_ptr wstream = OTF_WStream_open(outputFilename.c_str(), otf1_id, fileMgr);
    processWStreamMap[otf1_id] = wstream;
    
    OTF_WStream_writeBeginProcess(wstream, 0, otf1_id);
}

void OTF1ParallelTraceWriter::writeDefCounter(uint32_t id, const char* name, int properties)
{
    if (mpiRank == 0)
    {
        OTF_CHECK(OTF_Writer_writeDefCounter(globalWriter, 0, id, name, properties, 0, 0));
    }
}

void OTF1ParallelTraceWriter::writeNode(const Node *node, CounterTable &ctrTable,
        bool lastProcessNode, const Node *futureNode)
{
    uint32_t processId = (uint32_t)node->getProcessId();
    OTF_WStream_ptr wstream = processWStreamMap[processId];
    uint64_t nodeTime = node->getTime();

    if (node->isEnter() || node->isLeave())
    { 
        if ((uint32_t)node->getReferencedProcessId() != 0)
        {
            OTF_KeyValueList_appendUint32(kvList, streamRefKey,
                    (uint32_t)node->getReferencedProcessId());
        }

        if (node->isEventNode())
        {
            EventNode *eNode = (EventNode*) node;
            OTF_KeyValueList_appendUint32(kvList, eventRefKey, eNode->getEventId());
            CUresult cuResult = CUDA_ERROR_NOT_READY;
            if (eNode->getFunctionResult() == EventNode::FR_SUCCESS)
                cuResult = CUDA_SUCCESS;
            OTF_KeyValueList_appendUint32(kvList, funcResultKey, cuResult);
        }

        if (node->isEnter())
        {
            OTF_CHECK(OTF_WStream_writeEnterKV(wstream, nodeTime,
                    (uint32_t)node->getFunctionId(), processId, 0, NULL));
        } else
        {
            OTF_CHECK(OTF_WStream_writeLeaveKV(wstream, nodeTime, 0,
                    processId, 0, NULL));
        }
    }

    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    for (CounterTable::CtrIdSet::const_iterator iter = ctrIdSet.begin();
            iter != ctrIdSet.end(); ++iter)
    {
        bool valid = false;
        uint32_t ctrId = *iter;
        CtrTableEntry* counter = ctrTable.getCounter(ctrId);
        if (counter->isInternal)
            continue;

        CounterType ctrType = counter->type;
        if (ctrType == CTR_WAITSTATE_LOG10 || ctrType == CTR_BLAME_LOG10)
            continue;
        
        uint64_t ctrVal = node->getCounter(ctrId, &valid);

        if (valid || counter->hasDefault)
        {
            if (!valid)
                ctrVal = counter->defaultValue;
                   
            if (ctrType == CTR_WAITSTATE)
            {
                uint64_t ctrValLog10 = 0;
                if (ctrVal > 0)
                    ctrValLog10 = std::log10((double) ctrVal);
                
                OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                    processId, ctrTable.getCtrId(CTR_WAITSTATE_LOG10), ctrValLog10));
            }
            
            if (ctrType == CTR_BLAME)
            {
                uint64_t ctrValLog10 = 0;
                if (ctrVal > 0)
                    ctrValLog10 = std::log10((double) ctrVal);
                
                OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                    processId, ctrTable.getCtrId(CTR_BLAME_LOG10), ctrValLog10));
            }
                
            OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                    processId, ctrId, ctrVal));

            if ((ctrType == CTR_CRITICALPATH) && (ctrVal == 1) && node->isGraphNode()) 
            {        
                if (lastProcessNode)
                {
                    OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                            processId, ctrId, 0));
                }

                // make critical path stop in current process if next cp node in different process
                if((node->isLeave()) && (futureNode != NULL) && 
                        ((uint32_t)futureNode->getProcessId() != processId))
                {
                    OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                            processId, ctrId, 0));
                }
            } 
        }
    }
}

void OTF1ParallelTraceWriter::writeRMANode(const Node *node,
        uint64_t prevProcessId, uint64_t nextProcessId)
{
    OTF_WStream_ptr wstream = processWStreamMap[(uint32_t)node->getProcessId()];
    uint64_t nodeTime = node->getTime();
    uint32_t processId = node->getProcessId();

    if (node->isEnter())
    {
        OTF_CHECK(OTF_WStream_writeRMAEnd(wstream, nodeTime, processId,
                0, 0, 0, 0));

        OTF_CHECK(OTF_WStream_writeRMAPutRemoteEnd(wstream, nodeTime, processId,
                processId, (uint32_t)nextProcessId, 0, 0, 0, 0));

    }

    if (node->isLeave())
    {
        if (processId != (uint32_t)prevProcessId)
        {
            OTF_CHECK(OTF_WStream_writeRMAEnd(wstream, nodeTime, processId,
                    0, 0, 0, 0));
        }

        OTF_CHECK(OTF_WStream_writeRMAPut(wstream, nodeTime, processId,
                0, processId, 0, 0, 0, 0));
    }
}

void* OTF1ParallelTraceWriter::getWriteObject(uint32_t id)
{
    return processWStreamMap[id];
}
