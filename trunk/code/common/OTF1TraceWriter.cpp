/* 
 * File:   OTF1TraceWriter.cpp
 * Author: felix
 * 
 * Created on May 15, 2013, 7:28 PM
 */

#include "otf/OTF1TraceWriter.hpp"
#include "cuda.h"
#include "graph/EventNode.hpp"
#include "CounterTable.hpp"
#include "common.hpp"

using namespace cdm::io;

#define OTF_CHECK(cmd) \
        { \
                int _status = cmd; \
                if (!_status) \
                        throw RTException("OTF command '%s' returned error", #cmd); \
        }

OTF1TraceWriter::OTF1TraceWriter(const char *streamRefKeyName, const char *eventRefKeyName,
        const char *funcResultKeyName) :
ITraceWriter(streamRefKeyName, eventRefKeyName, funcResultKeyName),
fileMgr(NULL),
streamRefKey(1),
eventRefKey(2),
funcResultKey(3),
attrListCUDAToken(1),
attrListCUDAMasterToken(2)
{
}

OTF1TraceWriter::~OTF1TraceWriter()
{
}

void OTF1TraceWriter::open(const std::string otfFilename, uint32_t maxFiles,
        uint32_t numStreams, uint64_t timerResolution)
{
    this->numStreams = numStreams;
    fileMgr = OTF_FileManager_open(maxFiles);

    std::string baseFilename;
    baseFilename.append(otfFilename.c_str(), otfFilename.length() - 4);

    writer = OTF_Writer_open(baseFilename.c_str(), numStreams, fileMgr);
    kvList = OTF_KeyValueList_new();

    OTF_CHECK(OTF_Writer_writeDefTimerResolution(writer, 0, timerResolution));

    OTF_CHECK(OTF_Writer_writeDefFunctionGroup(writer, 0,
            (uint32_t) FG_APPLICATION, "Application"));
    OTF_CHECK(OTF_Writer_writeDefFunctionGroup(writer, 0,
            (uint32_t) FG_CUDA_API, "CUDRV_API"));
    OTF_CHECK(OTF_Writer_writeDefFunctionGroup(writer, 0,
            (uint32_t) FG_KERNEL, "CUDA_KERNEL"));
    OTF_CHECK(OTF_Writer_writeDefFunctionGroup(writer, 0,
            (uint32_t) FG_WAITSTATE, "WaitStates"));

    OTF_CHECK(OTF_Writer_writeDefMarker(writer, 0, (uint32_t) MG_Marker, "Marker", OTF_MARKER_TYPE_HINT));

    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, streamRefKey, OTF_UINT32,
            streamRefKeyName, "Referenced CUDA stream"));
    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, eventRefKey, OTF_UINT32,
            eventRefKeyName, "Referenced CUDA event"));
    OTF_CHECK(OTF_Writer_writeDefKeyValue(writer, 0, funcResultKey, OTF_UINT32,
            funcResultKeyName, "CUDA API function result"));

    // attribute lists
    OTF_ATTR_TYPE attrList[2];
    attrList[0] = OTF_ATTR_IsCUDAThread;
    attrList[1] = OTF_ATTR_IsMasterThread;
    OTF_CHECK(OTF_Writer_writeDefAttributeList(writer, 0, attrListCUDAToken, 1, attrList));
    OTF_CHECK(OTF_Writer_writeDefAttributeList(writer, 0, attrListCUDAMasterToken, 2, attrList));
}

void OTF1TraceWriter::close()
{
    OTF_CHECK(OTF_Writer_writeDefProcessGroup(writer, 0, 1, "GPU_GROUP",
            deviceProcesses.size(), deviceProcesses.data()));
    OTF_CHECK(OTF_Writer_writeDefProcessOrGroupAttributes(
            writer, 0, 1, attrListCUDAToken));

    OTF_CHECK(OTF_Writer_writeDefProcessGroup(writer, 0, 2, "GPU_MASTER_GROUP",
            deviceMasterProcesses.size(), deviceMasterProcesses.data()));
    OTF_CHECK(OTF_Writer_writeDefProcessOrGroupAttributes(
            writer, 0, 2, attrListCUDAMasterToken));

    OTF_KeyValueList_close(kvList);
    OTF_CHECK(OTF_Writer_close(writer));
    OTF_FileManager_close(fileMgr);
}

void OTF1TraceWriter::writeDefFunction(uint32_t id,
        const char* name, FunctionGroup fg)
{
    OTF_CHECK(OTF_Writer_writeDefFunction(writer, 0, id, name, (uint32_t) fg, 0));
}

void OTF1TraceWriter::writeDefCounter(uint32_t id,
        const char* name, int properties)
{
    OTF_CHECK(OTF_Writer_writeDefCounter(writer, 0, id, name, properties, 0, 0));
}

void OTF1TraceWriter::writeDefProcess(uint32_t id, uint32_t parentId, const char* name, ProcessGroup pg)
{
    if (pg == PG_DEVICE)
        deviceProcesses.push_back(id);
    if (pg == PG_DEVICE_NULL)
    {
        deviceProcesses.push_back(id);
        deviceMasterProcesses.push_back(id);
    }
    
    OTF_CHECK(OTF_Writer_writeDefProcess(writer, 0, id, name, parentId));
    uint32_t streamId = OTF_Writer_mapProcess(writer, id);
    processStreamMap[id] = streamId;
}

void OTF1TraceWriter::writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode)
{
    OTF_WStream *wstream = OTF_Writer_getStream(
          writer, processStreamMap[node->getProcessId()]);

    if (node->isEnter() || node->isLeave())
    {
        if (node->getReferencedProcessId() != 0)
        {
            OTF_KeyValueList_appendUint32(kvList, streamRefKey,
                    node->getReferencedProcessId());
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
            OTF_CHECK(OTF_WStream_writeEnterKV(wstream, node->getTime(),
                    node->getFunctionId(), node->getProcessId(), 0, kvList));
        } else
        {
            OTF_CHECK(OTF_WStream_writeLeaveKV(wstream, node->getTime(), 0,
                    node->getProcessId(), 0, kvList));
        }
    }

    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    for (CounterTable::CtrIdSet::const_iterator iter = ctrIdSet.begin();
            iter != ctrIdSet.end(); ++iter)
    {
        bool valid = false;
        uint64_t ctrVal = node->getCounter(*iter, &valid);
        if (valid || ctrTable.getCounter(*iter)->hasDefault)
        {
            if (!valid)
                ctrVal = ctrTable.getCounter(*iter)->defaultValue;

            OTF_CHECK(OTF_WStream_writeCounter(wstream, node->getTime(),
                    node->getProcessId(), *iter, ctrVal));
        }
    }
}