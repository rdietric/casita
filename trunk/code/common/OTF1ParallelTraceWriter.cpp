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
processNodes(NULL),
enableWaitStates(false),
iter(NULL),
lastGraphNode(NULL),
cTable(NULL),
graph(NULL),
verbose(false)
{
    mpiNumProcesses = new int[mpiSize];
    outputFilename.assign("");
    this->originalFilename.assign(originalFilename);
    this->originalFilename.erase(this->originalFilename.size() - 4, 4);
}

OTF1ParallelTraceWriter::~OTF1ParallelTraceWriter()
{
}

void OTF1ParallelTraceWriter::open(const std::string otfFilename, uint32_t maxFiles,
        uint32_t numStreams)
{
    fileMgr = OTF_FileManager_open(maxFiles);
    
    std::string baseFilename;
    baseFilename.assign("");
    if(strstr(originalFilename.c_str(), ".otf") != NULL)
        baseFilename.append(originalFilename.c_str(), originalFilename.length() - 4);
    else
        baseFilename.append(originalFilename.c_str(), originalFilename.length());
    
    reader = OTF_Reader_open(baseFilename.c_str(), fileMgr);
    
    if (!reader)
        throw RTException("Failed to open OTF1 trace file %s", baseFilename.c_str());

    OTF_Reader_setRecordLimit(reader, OTF_READ_MAXRECORDS);
    
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
    
    MPI_CHECK(MPI_Bcast(&timerResolution, 1, MPI_LONG_INT, 0, MPI_COMM_WORLD));

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

/*
 * Copy definitions from original trace to new one
 */
void OTF1ParallelTraceWriter::copyGlobalDefinitions(OTF_Writer *writer)
{
    OTF_Reader *reader = OTF_Reader_open(originalFilename.c_str(), fileMgr);

    OTF_HandlerArray* handlers = OTF_HandlerArray_open();
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefTimerResolution,
            OTF_DEFTIMERRESOLUTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFTIMERRESOLUTION_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcess,
            OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESS_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcessGroup,
            OTF_DEFPROCESSGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESSGROUP_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefFunction,
            OTF_DEFFUNCTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFFUNCTION_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefFunctionGroup,
            OTF_DEFFUNCTIONGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFFUNCTIONGROUP_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefAttributeList,
            OTF_DEFATTRLIST_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFATTRLIST_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefKeyValue,
            OTF_DEFKEYVALUE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFKEYVALUE_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcessOrGroupAttributes,
            OTF_DEFPROCESSORGROUPATTR_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESSORGROUPATTR_RECORD);    

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
    
}

void OTF1ParallelTraceWriter::copyMasterControl()
{
    OTF_MasterControl* mc = OTF_MasterControl_new(fileMgr);

    OTF_CHECK(OTF_MasterControl_read(mc, originalFilename.c_str()));
    OTF_CHECK(OTF_MasterControl_write(mc, outputFilename.c_str()));

    OTF_MasterControl_close(mc);
}

/*
 * OTF2: just create event writer for this process
 * OTF1: create event stream writer and write "begin process" event
 */
void OTF1ParallelTraceWriter::writeDefProcess(uint64_t id, uint64_t parentId,
        const char* name, ProcessGroup pg)
{
    
    uint32_t otf1_id = (uint32_t) id;
    
    // create local writer for process id
    OTF_WStream_ptr wstream = OTF_WStream_open(outputFilename.c_str(), otf1_id, fileMgr);
    processWStreamMap[otf1_id] = wstream;
    
    OTF_WStream_writeBeginProcess(wstream, 0, otf1_id);
}

/*
 * Write self-defined metrics/counter to new trace file
 */
void OTF1ParallelTraceWriter::writeDefCounter(uint32_t id, const char* name, int properties)
{
    if (mpiRank == 0)
    {
        OTF_CHECK(OTF_Writer_writeDefCounter(globalWriter, 0, id, name, properties, 0, 0));
    }
}

/*
 * Read all events from original trace for this process and combine them with the
 * events and counter values from analysis
 */
void OTF1ParallelTraceWriter::writeProcess(uint64_t processId, Process::SortedNodeList *nodes,
                        bool enableWaitStates, GraphNode *pLastGraphNode, bool verbose,
                        CounterTable* ctrTable, Graph *graph)
{
    if(verbose)
        std::cout << "[" << mpiRank << "] Start writing for process " << processId << std::endl;
    
    // create writer for this process
    processNodes = nodes;
    this->enableWaitStates = enableWaitStates;
    iter = processNodes->begin();
    // skip first processNode for main processes, since it did not appear in original trace
    if((*iter)->getType() == MISC_PROCESS)
        iter++;
    lastGraphNode = pLastGraphNode;
    this->verbose = verbose;
    this->graph = graph;
    cTable = ctrTable;        
    
    uint32_t otf1_id = (uint32_t) processId;
    
    OTF_HandlerArray* handlers = OTF_HandlerArray_open();
    
    // Set event handlers
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleRecvMsg,
            OTF_RECEIVE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_RECEIVE_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleSendMsg,
            OTF_SEND_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_SEND_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleBeginCollectiveOperation,
            OTF_BEGINCOLLOP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_BEGINCOLLOP_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleEndCollectiveOperation,
            OTF_ENDCOLLOP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_ENDCOLLOP_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleRMAEnd,
            OTF_RMAEND_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_RMAEND_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleRMAGet,
            OTF_RMAGET_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_RMAGET_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleRMAPut,
            OTF_RMAPUT_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_RMAPUT_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleEnter,
            OTF_ENTER_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_ENTER_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleLeave,
            OTF_LEAVE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_LEAVE_RECORD);
    
    OTF_RStream* rStream = OTF_Reader_getStream(reader, otf1_id);
    OTF_RStream_setRecordLimit(rStream, OTF_READ_MAXRECORDS);
    OTF_RStream_readEvents(rStream, handlers);
    
    OTF_HandlerArray_close(handlers);
}

/*
 * Process next node in list from analysis
 */
bool OTF1ParallelTraceWriter::processNextNode(uint64_t time, uint32_t funcId)
{
    Node *node = *iter;

    if(time != node->getTime())
    {
        if(verbose)
            std::cout << "Node written from original trace, since there is an inconsistency: TIME: " 
                    << time << " - " << node->getTime() 
                    << "funcId " << funcId << " - " << node->getFunctionId() << std::endl;

        return false;
    }
    
    // find next connected node on critical path
    GraphNode *futureCPNode = NULL;
    Graph::EdgeList outEdges;
    if(graph->hasOutEdges((GraphNode*)node))
            outEdges = graph->getOutEdges((GraphNode*)node);
    
    Graph::EdgeList::const_iterator edgeIter = outEdges.begin();
    uint64_t timeNextCPNode = 0;
    uint32_t cpCtrId = cTable->getCtrId(CTR_CRITICALPATH);
    
    while(edgeIter != outEdges.end() && !outEdges.empty())
    {
        GraphNode *edgeEndNode = (*edgeIter)->getEndNode();

        if((edgeEndNode->getCounter(cpCtrId, NULL) == 1) && 
                ((timeNextCPNode > edgeEndNode->getTime()) || timeNextCPNode == 0))
        {
            futureCPNode = edgeEndNode;
            timeNextCPNode = futureCPNode->getTime();
        }
        ++edgeIter;
    }

    if (node->isEnter() || node->isLeave())
    {
        if ((!node->isPureWaitstate()) || enableWaitStates)
        {
            if (verbose)
            {
                printf("[%u] [%12lu:%12.8fs] %60s in %8lu (FID %lu)\n",
                        mpiRank,
                        node->getTime(),
                        (double) (node->getTime()) / (double) timerResolution,
                        node->getUniqueName().c_str(),
                        node->getProcessId(),
                        node->getFunctionId());
            }

            writeNode(node, *cTable,
                    node == lastGraphNode, futureCPNode);
        }
    }
    
    iter++;
    
    return true;
    
}

/*
 * Write the nodes that were read at program start, and processed during analysis
 * write corresponding counter values of computed metrics for each node
 */
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
            OTF_CHECK(OTF_WStream_writeEnter(wstream, nodeTime,
                    (uint32_t)node->getFunctionId(), processId, 0));
            
        } else
        {
            OTF_CHECK(OTF_WStream_writeLeave(wstream, nodeTime, 0,
                    processId, 0));
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
            
            if (ctrType == CTR_CRITICALPATH_TIME)
            {
                if (node->isEnter())
                    cpTimeCtrStack.push(ctrVal);
                else
                {
                    ctrVal = cpTimeCtrStack.top();
                    cpTimeCtrStack.pop();
                }
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

/* 
 * /////////////////////// Callbacks to re-write definition records of original trace file ///////////////////
 * Every callbacks has the writer object within @var{userData} and writes record immediately after reading
 */
int OTF1ParallelTraceWriter::otf1HandleDefProcess(void * userData, uint32_t stream, uint32_t processId,
        const char * name, uint32_t parent, OTF_KeyValueList_struct * list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;

    OTF_CHECK(OTF_Writer_writeDefProcess(writer->globalWriter, stream,
            processId, name, parent));

    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefProcessGroup(void *userData, uint32_t stream,
        uint32_t procGroup, const char *name, uint32_t numberOfProcs,
        const uint32_t *procs, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;

    OTF_CHECK(OTF_Writer_writeDefProcessGroupKV(writer->globalWriter,
            stream, procGroup, name, numberOfProcs, procs, list));

    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefProcessOrGroupAttributes(void *userData, uint32_t stream,
        uint32_t proc_token, uint32_t attr_token, OTF_KeyValueList *list)
{
    
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;
    
    OTF_CHECK(OTF_Writer_writeDefProcessOrGroupAttributesKV(writer->globalWriter,
            stream, proc_token, attr_token, list));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefAttributeList(void *userData, uint32_t stream,
        uint32_t attr_token, uint32_t num, OTF_ATTR_TYPE *array, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;

    OTF_CHECK(OTF_Writer_writeDefAttributeListKV(writer->globalWriter,
            stream, attr_token, num, array, list));

    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefFunction(void *userData, uint32_t stream,
        uint32_t func, const char *name, uint32_t funcGroup,
        uint32_t source, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;

    OTF_CHECK(OTF_Writer_writeDefFunctionKV(writer->globalWriter, stream,
            func, name, funcGroup, source, list));

    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefFunctionGroup(void * userData, uint32_t stream,
        uint32_t funcGroup, const char * name, OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;

    OTF_CHECK(OTF_Writer_writeDefFunctionGroupKV(writer->globalWriter,
            stream, funcGroup, name, list));

    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleDefKeyValue(void* userData, uint32_t stream, uint32_t key,
        OTF_Type type, const char *name, const char *description, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;
    
    OTF_CHECK(OTF_Writer_writeDefKeyValueKV(writer->globalWriter,
            stream, key, type, name, description, list));

    return OTF_RETURN_OK;    
}


int OTF1ParallelTraceWriter::otf1HandleDefTimerResolution(void *userData, uint32_t stream,
        uint64_t ticksPerSecond, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter * writer = (OTF1ParallelTraceWriter *) userData;
    
    writer->timerResolution = ticksPerSecond;

    OTF_CHECK(OTF_Writer_writeDefTimerResolution(writer->globalWriter,
            stream, ticksPerSecond));

    return OTF_RETURN_OK;
}

/* 
 * /////////////////////// Callbacks to re-write enter/leave and communication records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 * Enter and leave callbacks call "processNextNode()" to write node with metrics
 */
int OTF1ParallelTraceWriter::otf1HandleEnter(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    if(writer->processNextNode(time, functionId))
        return OTF_RETURN_OK;
    
    OTF_CHECK(OTF_WStream_writeEnterKV(writer->processWStreamMap[processId], time, functionId,
            processId, source, list));
    
    return OTF_RETURN_OK;
    
}

int OTF1ParallelTraceWriter::otf1HandleLeave(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    if(writer->processNextNode(time, functionId))
        return OTF_RETURN_OK;
    
    OTF_CHECK(OTF_WStream_writeLeaveKV(writer->processWStreamMap[processId], time, functionId,
            processId, source, list));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleSendMsg(void *userData, uint64_t time, uint32_t sender,
                    uint32_t receiver, uint32_t group, uint32_t type, uint32_t length,
                    uint32_t source, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeSendMsgKV(writer->processWStreamMap[sender], time, sender, receiver, 
            group, type, length, source, list));
    
    return OTF_RETURN_OK;
    
}

int OTF1ParallelTraceWriter::otf1HandleRecvMsg(void *userData, uint64_t time, uint32_t receiver,
                    uint32_t sender, uint32_t group, uint32_t type, uint32_t length,
                    uint32_t source, OTF_KeyValueList *list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeRecvMsgKV(writer->processWStreamMap[receiver], time, receiver, sender,
            group, type, length, source, list));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleBeginCollectiveOperation(void * userData, uint64_t time,
                    uint32_t process, uint32_t collOp, uint64_t matchingId, uint32_t procGroup,
                    uint32_t rootProc, uint64_t sent, uint64_t received, uint32_t scltoken,
                    OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    int OTF_WStream_writeBeginCollectiveOperationKV( OTF_WStream* wstream,
                uint64_t time, uint32_t process, uint32_t collOp,
                uint64_t matchingId, uint32_t procGroup, uint32_t rootProc,
                uint64_t sent, uint64_t received, uint32_t scltoken, OTF_KeyValueList* list );
    
    OTF_CHECK(OTF_WStream_writeBeginCollectiveOperation(writer->processWStreamMap[process], time, process, collOp,
            matchingId, procGroup, rootProc, sent, received, scltoken));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleEndCollectiveOperation	(void *	userData, uint64_t time,
                        uint32_t process, uint64_t matchingId, OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeEndCollectiveOperation(writer->processWStreamMap[process], time, process, 
           matchingId));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleRMAEnd	(void * userData, uint64_t time, uint32_t process,
                        uint32_t remote, uint32_t communicator, uint32_t tag, uint32_t source,
                        OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeRMAEndKV(writer->processWStreamMap[process], time, process, remote, 
            communicator, tag, source, list));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleRMAGet	(void * userData, uint64_t time, uint32_t process,
                        uint32_t origin, uint32_t target, uint32_t communicator,
                        uint32_t tag, uint64_t bytes, uint32_t source, OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeRMAGetKV(writer->processWStreamMap[process], time, process, origin, 
            target, communicator, tag, bytes, source, list));
    
    return OTF_RETURN_OK;
}

int OTF1ParallelTraceWriter::otf1HandleRMAPut	(void * userData, uint64_t time, uint32_t process,
                        uint32_t origin, uint32_t target, uint32_t communicator, uint32_t tag,
                        uint64_t bytes, uint32_t source, OTF_KeyValueList * list)
{
    OTF1ParallelTraceWriter *writer = (OTF1ParallelTraceWriter*) userData;
    
    OTF_CHECK(OTF_WStream_writeRMAPutKV(writer->processWStreamMap[process], time, process, origin,
            target, communicator, tag, bytes, source, list));
    
    return OTF_RETURN_OK;
}