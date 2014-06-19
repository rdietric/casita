/* 
 * File:   OTF2ParallelTraceWriter.cpp
 * Author: Jonas
 * 
 * Created on 17. March 2014, 12:00
 */

#include <mpi.h>
#include <cmath>
#include <iostream>
#include <stdlib.h>
#include <inttypes.h>

// following adjustments necessary to use MPI_Collectives
#define OTF2_MPI_UINT64_T MPI_UNSIGNED_LONG
#define OTF2_MPI_INT64_T  MPI_LONG

#include "otf2/OTF2_MPI_Collectives.h"
#include "otf/OTF2ParallelTraceWriter.hpp"
#include "cuda.h"
#include "graph/EventNode.hpp"
#include "CounterTable.hpp"
#include "common.hpp"
#include "FunctionTable.hpp"
#include "otf/ITraceReader.hpp"
#include "include/Process.hpp"
#include "include/otf/OTF2TraceReader.hpp"
#include "TraceData.hpp"

using namespace cdm;
using namespace cdm::io;

#define OTF2_CHECK(cmd) \
        { \
                int _status = cmd; \
                if (_status) \
                        throw RTException("OTF2 command '%s' returned error %d", #cmd, _status); \
        }

#define MPI_CHECK(cmd) \
        { \
                int mpi_result = cmd; \
                if (mpi_result != MPI_SUCCESS) \
                        throw RTException("MPI error %d in call %s", mpi_result, #cmd); \
        }

OTF2_FlushType preFlush( void* userData, OTF2_FileType fileType,
            OTF2_LocationRef location, void* callerData, bool final )
{
    return OTF2_FLUSH;
}

OTF2_TimeStamp postFlush( void* userData, OTF2_FileType fileType,
            OTF2_LocationRef location )
{
    return 0;
}

static inline size_t
otf2_mpi_type_to_size( OTF2_Type type )
{
    switch ( type )
    {
        case OTF2_TYPE_UINT8:
        case OTF2_TYPE_INT8:
            return 1;
        case OTF2_TYPE_UINT16:
        case OTF2_TYPE_INT16:
            return 2;
        case OTF2_TYPE_UINT32:
        case OTF2_TYPE_INT32:
        case OTF2_TYPE_FLOAT:
            return 4;
        case OTF2_TYPE_UINT64:
        case OTF2_TYPE_INT64:
        case OTF2_TYPE_DOUBLE:
            return 8;
        default:
            return 0;
    }
}

static inline MPI_Datatype otf2_to_mpi_type(OTF2_Type type)
{
    
    switch(type)
    {
        case OTF2_TYPE_UINT8:
        case OTF2_TYPE_INT8:
            return MPI_CHAR;
        case OTF2_TYPE_UINT16:
        case OTF2_TYPE_INT16:
            return MPI_SHORT;
        case OTF2_TYPE_UINT32:
        case OTF2_TYPE_INT32:
            return MPI_INTEGER;
        case OTF2_TYPE_FLOAT:
            return MPI_FLOAT;
        case OTF2_TYPE_UINT64:
            return MPI_UNSIGNED_LONG_LONG;
        case OTF2_TYPE_INT64:
            return MPI_LONG_LONG;
        case OTF2_TYPE_DOUBLE:
            return MPI_DOUBLE;
        default:
            return 0;
    }
    
}




OTF2ParallelTraceWriter::OTF2ParallelTraceWriter(const char *streamRefKeyName,
        const char *eventRefKeyName,
        const char *funcResultKeyName,
        uint32_t mpiRank,
        uint32_t mpiSize,
        MPI_Comm comm,
        const char *originalFilename,
        std::set<uint32_t> ctrIdSet) :
IParallelTraceWriter(streamRefKeyName, eventRefKeyName, funcResultKeyName,
mpiRank, mpiSize),
global_def_writer(NULL),
processNodes(NULL),
enableWaitStates(false),
iter(NULL),
lastGraphNode(NULL),
cTable(NULL),
graph(NULL),
verbose(false)
{
    outputFilename.assign("");
    pathToFile.assign("");
    this->originalFilename.assign(originalFilename);
    
    this->ctrIdSet = ctrIdSet;
    
    flush_callbacks.otf2_post_flush = postFlush;
    flush_callbacks.otf2_pre_flush = preFlush;
    
    commGroup = comm;
    
    timerOffset = 0;
    
    for (CounterTable::CtrIdSet::const_iterator ctrIter = ctrIdSet.begin();
                ctrIter != ctrIdSet.end(); ++ctrIter)
    {
        otf2CounterMapping[*ctrIter] = *ctrIter-1;
    }
    
}

OTF2ParallelTraceWriter::~OTF2ParallelTraceWriter()
{

}

void OTF2ParallelTraceWriter::open(const std::string otfFilename, uint32_t maxFiles,
        uint32_t numStreams)
{
   
    outputFilename = otfFilename;
    pathToFile = "./";

    printf("[%u] FILENAME: %s path: %s \n ", mpiRank, outputFilename.c_str(), pathToFile.c_str());
    
    //open new otf2 file
    archive = OTF2_Archive_Open( pathToFile.c_str(), outputFilename.c_str(), 
            OTF2_FILEMODE_WRITE, 1024 * 1024, 4 * 1024 * 1024, OTF2_SUBSTRATE_POSIX,
            OTF2_COMPRESSION_NONE );

    OTF2_Archive_SetFlushCallbacks( archive, &flush_callbacks, NULL );

    // set collective callbacks to write trace in parallel
    OTF2_MPI_Archive_SetCollectiveCallbacks(archive, commGroup, MPI_COMM_NULL);
    
    timerOffset = 0;
    timerResolution = 0;
    counterForStringDefinitions = 0;
    counterForMetricInstanceId = 0;    
    
    reader = OTF2_Reader_Open(originalFilename.c_str());

    OTF2_MPI_Reader_SetCollectiveCallbacks(reader, commGroup);

    if (!reader)
         throw RTException("Failed to open OTF2 trace file %s", originalFilename.c_str());        
    
    if(mpiRank == 0)
        copyGlobalDefinitions();
    
    MPI_CHECK(MPI_Bcast(&timerOffset, 1, MPI_LONG_INT, 0, MPI_COMM_WORLD));
    MPI_CHECK(MPI_Bcast(&timerResolution, 1, MPI_LONG_INT, 0, MPI_COMM_WORLD));
    
    MPI_CHECK(MPI_Barrier(MPI_COMM_WORLD));
    
    // open event to start creating event files for each location
    OTF2_Archive_OpenEvtFiles( archive );
    
    MPI_CHECK(MPI_Barrier(MPI_COMM_WORLD));
}

void OTF2ParallelTraceWriter::close()
{
    // close all opened event writer
    for(std::map<uint64_t,OTF2_EvtWriter*>::iterator iter = evt_writerMap.begin();
            iter != evt_writerMap.end(); iter++)
    {
        OTF2_Archive_CloseEvtWriter( archive, iter->second);
    }

    if(mpiRank == 0)
        OTF2_Archive_CloseDefFiles( archive );
    
    OTF2_Archive_CloseEvtFiles( archive );
    
    // close global writer
    OTF2_CHECK(OTF2_Archive_Close(archive));
}

/*
 * Copy definitions from original trace to new one
 */
void OTF2ParallelTraceWriter::copyGlobalDefinitions()
{

    global_def_writer = OTF2_Archive_GetGlobalDefWriter( archive );

    OTF2_GlobalDefReader* global_def_reader = OTF2_Reader_GetGlobalDefReader( reader );

    OTF2_GlobalDefReaderCallbacks* global_def_callbacks = OTF2_GlobalDefReaderCallbacks_New();

    OTF2_GlobalDefReaderCallbacks_SetAttributeCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_Attribute);
    OTF2_GlobalDefReaderCallbacks_SetStringCallback(global_def_callbacks, 
            &OTF2_GlobalDefReaderCallback_String);
    OTF2_GlobalDefReaderCallbacks_SetClockPropertiesCallback(global_def_callbacks, 
            &OTF2_GlobalDefReaderCallback_ClockProperties);    
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(global_def_callbacks, 
            &OTF2_GlobalDefReaderCallback_Location);    
    OTF2_GlobalDefReaderCallbacks_SetGroupCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_Group);
    OTF2_GlobalDefReaderCallbacks_SetLocationGroupCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_LocationGroup);
    OTF2_GlobalDefReaderCallbacks_SetCommCallback(global_def_callbacks,
           &OTF2_GlobalDefReaderCallback_Comm);
    OTF2_GlobalDefReaderCallbacks_SetRegionCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_Region);
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_SystemTreeNode);
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodePropertyCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty);
    OTF2_GlobalDefReaderCallbacks_SetSystemTreeNodeDomainCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain);
    OTF2_GlobalDefReaderCallbacks_SetRmaWinCallback(global_def_callbacks,
            &OTF2_GlobalDefReaderCallback_RmaWin);

    // register callbacks
    OTF2_Reader_RegisterGlobalDefCallbacks( reader, global_def_reader, global_def_callbacks, this );

    OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );    

    uint64_t definitions_read = 0;
    // read definitions
    OTF2_Reader_ReadAllGlobalDefinitions( reader, global_def_reader, &definitions_read );

    printf("Read and wrote %lu definitions\n ", definitions_read);
    
}

/*
 * OTF2: just create event writer for this process
 * OTF1: create event stream writer and write "begin process" event
 */
void OTF2ParallelTraceWriter::writeDefProcess(uint64_t id, uint64_t parentId,
        const char* name, ProcessGroup pg)
{
    // create writer for this process
    OTF2_EvtWriter *evt_writer = OTF2_Archive_GetEvtWriter( archive, OTF2_UNDEFINED_LOCATION );
    OTF2_CHECK(OTF2_EvtWriter_SetLocationID( evt_writer, id ));
    evt_writerMap[id] = evt_writer;
}

/*
 * Write self-defined metrics/counter to new trace file
 */
void OTF2ParallelTraceWriter::writeDefCounter(uint32_t otfId, const char* name, int properties)
{
    if (mpiRank == 0)
    {
        // map to otf2 (otf2 starts with 0 instead of 1)
        uint32_t id = otf2CounterMapping[otfId];    
        
        // 1) write String definition
        OTF2_CHECK(OTF2_GlobalDefWriter_WriteString( global_def_writer, counterForStringDefinitions, name ));
                
        // 2) Write metrics member and class definition
        OTF2_CHECK(OTF2_GlobalDefWriter_WriteMetricMember(global_def_writer, id, counterForStringDefinitions,
                counterForStringDefinitions, OTF2_METRIC_TYPE_USER, OTF2_METRIC_ABSOLUTE_POINT, OTF2_TYPE_UINT64, 
                OTF2_BASE_DECIMAL, 0, 0));	
        
        OTF2_CHECK(OTF2_GlobalDefWriter_WriteMetricClass(global_def_writer, id, 1, &id, OTF2_METRIC_ASYNCHRONOUS, OTF2_RECORDER_KIND_ABSTRACT));
        
        counterForStringDefinitions++;
    }
}

/*
 * Read all events from original trace for this process and combine them with the
 * events and counter values from analysis
 */
void OTF2ParallelTraceWriter::writeProcess(uint64_t processId, Process::SortedNodeList *nodes, 
        bool waitStates, GraphNode *pLastGraphNode, bool verbose, 
        CounterTable* ctrTable, Graph* graph)
{
    if(verbose)
        std::cout << "[" << mpiRank << "] Start writing for process " << processId << std::endl;
    
    processNodes = nodes;
    enableWaitStates = waitStates;
    iter = processNodes->begin();
    lastGraphNode = pLastGraphNode;
    this->verbose = verbose;
    this->graph = graph;
    cTable = ctrTable;        
    
    OTF2_Reader_SelectLocation(reader, processId);
    
    OTF2_Reader_OpenEvtFiles(reader);
    
    OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, processId );
    uint64_t def_reads = 0;
    OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
    OTF2_Reader_CloseDefReader( reader, def_reader );

    OTF2_Reader_GetEvtReader( reader, processId );
    
    OTF2_GlobalEvtReader* global_evt_reader = OTF2_Reader_GetGlobalEvtReader( reader );
    
    OTF2_GlobalEvtReaderCallbacks* event_callbacks = OTF2_GlobalEvtReaderCallbacks_New();
    OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks, &otf2CallbackEnter );
    OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks, &otf2CallbackLeave );
    OTF2_GlobalEvtReaderCallbacks_SetThreadForkCallback(event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadFork);
    OTF2_GlobalEvtReaderCallbacks_SetThreadJoinCallback(event_callbacks, &OTF2_GlobalEvtReaderCallback_ThreadJoin);
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveBeginCallback( event_callbacks, &otf2CallbackComm_MpiCollectiveBegin );
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback( event_callbacks, &otf2CallbackComm_MpiCollectiveEnd );
    OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback( event_callbacks, &otf2Callback_MpiRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback( event_callbacks, &otf2Callback_MpiSend );
    OTF2_GlobalEvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(event_callbacks, &otf2CallbackComm_RmaOpCompleteBlocking );
    OTF2_GlobalEvtReaderCallbacks_SetRmaWinCreateCallback(event_callbacks, &otf2CallbackComm_RmaWinCreate );
    OTF2_GlobalEvtReaderCallbacks_SetRmaWinDestroyCallback(event_callbacks, &otf2CallbackComm_RmaWinDestroy );
    OTF2_GlobalEvtReaderCallbacks_SetRmaGetCallback(event_callbacks, &otf2CallbackComm_RmaGet );
    OTF2_GlobalEvtReaderCallbacks_SetRmaPutCallback(event_callbacks, &otf2CallbackComm_RmaPut );
    OTF2_GlobalEvtReaderCallbacks_SetThreadTeamBeginCallback(event_callbacks, &otf2CallbackComm_ThreadTeamBegin);
    OTF2_GlobalEvtReaderCallbacks_SetThreadTeamEndCallback(event_callbacks, &otf2CallbackComm_ThreadTeamEnd);
    OTF2_Reader_RegisterGlobalEvtCallbacks( reader, global_evt_reader, event_callbacks, this );
    OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );
    
    uint64_t events_read = 0;

    // returns 0 if successfull, >0 otherwise
    if (OTF2_Reader_ReadAllGlobalEvents(reader, global_evt_reader, &events_read))
        throw RTException("Failed to read OTF2 events");
    
    OTF2_Reader_CloseGlobalEvtReader(reader, global_evt_reader);
    
    OTF2_Reader_CloseEvtFiles(reader);
}

/*
 * Process next node in list from analysis
 */
bool OTF2ParallelTraceWriter::processNextNode(uint64_t time, uint32_t funcId)
{
    // Skip threadFork/Join (also skips first inserted processNode that is not in original trace)
    while((*iter)->isOMPParallelRegion())
    {
        if(verbose)
            std::cout << "Skipping " << (*iter)->getUniqueName() << std::endl;
        iter++;
    }
    
    Node *node = *iter;

    if(time != (node->getTime()+timerOffset))
    {
        if(verbose)
            std::cout << "Node written from original trace, since there is an inconsistency: TIME: " 
                    << time << " - " << node->getTime()+timerOffset 
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
    
    // set iterator to next node in sorted list
    iter++;
 
    return true;
    
}

/*
 * Write the nodes that were read at program start, and processed during analysis
 * write corresponding counter values of computed metrics for each node
 */
void OTF2ParallelTraceWriter::writeNode(const Node *node, CounterTable &ctrTable,
        bool lastProcessNode, const Node *futureNode)
{
    
    uint64_t processId = node->getProcessId();
    uint64_t nodeTime = node->getTime() + timerOffset;
    
    OTF2_EvtWriter *evt_writer = evt_writerMap[processId];
    
    if (node->isEnter() || node->isLeave())
    { 
        if (node->isEnter())
        {
            OTF2_CHECK(OTF2_EvtWriter_Enter(evt_writer, NULL, nodeTime,
                node->getFunctionId()));
        } else
        {
            OTF2_CHECK(OTF2_EvtWriter_Leave(evt_writer, NULL, nodeTime,
                node->getFunctionId()));
        }
    }
    
    CounterTable::CtrIdSet ctrIdSet = ctrTable.getAllCounterIDs();
    for (CounterTable::CtrIdSet::const_iterator iter = ctrIdSet.begin();
            iter != ctrIdSet.end(); ++iter)
    {
        bool valid = false;
        uint32_t otf2CtrId = otf2CounterMapping[*iter];
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

                OTF2_Type types[1] = {OTF2_TYPE_UINT64};
                OTF2_MetricValue values[1];
                values[0].unsigned_int = ctrValLog10;

                OTF2_CHECK(OTF2_EvtWriter_Metric(evt_writer,NULL, nodeTime,
                        otf2CounterMapping[ctrTable.getCtrId(CTR_WAITSTATE_LOG10)],
                        1,types,values));
            }

            if (ctrType == CTR_BLAME)
            {
                uint64_t ctrValLog10 = 0;
                if (ctrVal > 0)
                    ctrValLog10 = std::log10((double) ctrVal);

                OTF2_Type types[1] = {OTF2_TYPE_UINT64};
                OTF2_MetricValue values[1];
                values[0].unsigned_int = ctrValLog10;

                 OTF2_CHECK(OTF2_EvtWriter_Metric(evt_writer,NULL, nodeTime,
                        otf2CounterMapping[ctrTable.getCtrId(CTR_BLAME_LOG10)],
                        1,types,values));
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

            OTF2_Type types[1] = {OTF2_TYPE_UINT64};
            OTF2_MetricValue values[1];
            values[0].unsigned_int = ctrVal;

            OTF2_CHECK(OTF2_EvtWriter_Metric(evt_writer, NULL, nodeTime,
                    otf2CtrId, 1, types, values));

            if ((ctrType == CTR_CRITICALPATH) && (ctrVal == 1) && node->isGraphNode()) 
            {        
                if (lastProcessNode)
                {
                    OTF2_Type types[1] = {OTF2_TYPE_UINT64};
                    OTF2_MetricValue values[1];
                    values[0].unsigned_int = 0;

                    OTF2_CHECK(OTF2_EvtWriter_Metric(evt_writer, NULL, nodeTime,
                            otf2CtrId, 1, types, values));
                }

                // make critical path stop in current process if next cp node in different process
                if((node->isLeave()) && (futureNode != NULL) && 
                        (futureNode->getProcessId() != processId))
                {
                    OTF2_Type types[1] = {OTF2_TYPE_UINT64};
                    OTF2_MetricValue values[1];
                    values[0].unsigned_int = 0;

                    OTF2_CHECK(OTF2_EvtWriter_Metric(evt_writer, NULL, nodeTime,
                            otf2CtrId, 1, types, values));
                }
            } 
        }
    }
}

/* 
 * /////////////////////// Callbacks to re-write definition records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 */
OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_ClockProperties(void *userData, 
        uint64_t timerResolution, uint64_t globalOffset, uint64_t traceLength)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    tw->timerOffset = globalOffset;
    tw->timerResolution = timerResolution;
           
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteClockProperties( tw->global_def_writer,
            timerResolution,globalOffset,traceLength));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode  OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_LocationGroup(void *userData, 
        OTF2_LocationGroupRef self, OTF2_StringRef name, OTF2_LocationGroupType locationGroupType, 
        OTF2_SystemTreeNodeRef systemTreeParent)
{
    
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteLocationGroup(tw->global_def_writer, 
            self, name, locationGroupType, systemTreeParent));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Location(void *userData, 
        OTF2_LocationRef self, OTF2_StringRef name, OTF2_LocationType locationType, 
        uint64_t numberOfEvents, OTF2_LocationGroupRef locationGroup)
{

    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteLocation(tw->global_def_writer, self,
            name, locationType, numberOfEvents, locationGroup));
        
    return OTF2_CALLBACK_SUCCESS;
}
    
OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Group(void *userData,
                    OTF2_GroupRef self, OTF2_StringRef name, OTF2_GroupType groupType, 
                    OTF2_Paradigm paradigm, OTF2_GroupFlag groupFlags, uint32_t numberOfMembers, 
                    const uint64_t *members)
{
       
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;

    OTF2_CHECK(OTF2_GlobalDefWriter_WriteGroup(tw->global_def_writer, self, name,
            groupType, paradigm, groupFlags, numberOfMembers, members));
    
    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Comm(void *userData, 
                    OTF2_CommRef self, OTF2_StringRef name, OTF2_GroupRef group, OTF2_CommRef parent)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
 
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteComm(tw->global_def_writer, self, name,
            group, parent));

    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_String(void *userData, 
                    OTF2_StringRef self, const char *string)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    tw->counterForStringDefinitions++;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteString(tw->global_def_writer, self, string));
    
    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNode(void *userData, 
                    OTF2_SystemTreeNodeRef self, OTF2_StringRef name, OTF2_StringRef className, 
                    OTF2_SystemTreeNodeRef parent)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNode(tw->global_def_writer, 
            self, name, className, parent));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNodeProperty(void *userData, 
                    OTF2_SystemTreeNodeRef systemTreeNode, OTF2_StringRef name, OTF2_StringRef value)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty(tw->global_def_writer, 
            systemTreeNode, name, value));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_SystemTreeNodeDomain(void *userData, 
                    OTF2_SystemTreeNodeRef systemTreeNode, OTF2_SystemTreeDomain systemTreeDomain)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;

    OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNodeDomain(tw->global_def_writer, 
            systemTreeNode, systemTreeDomain));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Region(void *userData,
                    OTF2_RegionRef self, OTF2_StringRef name, OTF2_StringRef cannonicalName,
                    OTF2_StringRef description, OTF2_RegionRole regionRole, OTF2_Paradigm paradigm,
                    OTF2_RegionFlag regionFlags, OTF2_StringRef sourceFile, uint32_t beginLineNumber,
                    uint32_t endLineNumber)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
   
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteRegion(tw->global_def_writer,self,name,
            cannonicalName, description, regionRole, paradigm,
            regionFlags, sourceFile, beginLineNumber, endLineNumber));

    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_Attribute(void *userData, 
                    OTF2_AttributeRef self, OTF2_StringRef name, OTF2_StringRef description, OTF2_Type type)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteAttribute(tw->global_def_writer, self, name,
            description, type));
    
    return OTF2_CALLBACK_SUCCESS;
}       

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalDefReaderCallback_RmaWin(void *userData, 
                    OTF2_RmaWinRef self, OTF2_StringRef name, OTF2_CommRef comm)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_GlobalDefWriter_WriteRmaWin(tw->global_def_writer, self, name,
            comm));
    
    return OTF2_CALLBACK_SUCCESS;
    
}


/* 
 * /////////////////////// Callbacks to re-write enter/leave and communication records of original trace file ///////////////////
 * Every callback has the writer object within @var{userData} and writes record immediately after reading
 * Enter and leave callbacks call "processNextNode()" to write node with metrics
 */
OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_MpiCollectiveEnd(OTF2_LocationRef locationID,
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,
                    OTF2_CollectiveOp collectiveOp, OTF2_CommRef communicator, uint32_t root,
                    uint64_t sizeSent, uint64_t sizeReceived)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_MpiCollectiveEnd(tw->evt_writerMap[locationID], attributeList, time, 
                            collectiveOp, communicator, root, sizeSent, sizeReceived));
    
    return OTF2_CALLBACK_SUCCESS;
}
    
OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_MpiCollectiveBegin(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList)
{

    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_MpiCollectiveBegin(tw->evt_writerMap[locationID], attributeList, time));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_RmaWinCreate(OTF2_LocationRef location, OTF2_TimeStamp time, 
                    void *userData, OTF2_AttributeList *attributeList, OTF2_RmaWinRef win)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
        
    OTF2_CHECK(OTF2_EvtWriter_RmaWinCreate(tw->evt_writerMap[location], attributeList, time, 
                                win));
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_RmaWinDestroy(OTF2_LocationRef location, OTF2_TimeStamp time, 
        void *userData, OTF2_AttributeList *attributeList, OTF2_RmaWinRef win)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_RmaWinDestroy(tw->evt_writerMap[location], attributeList, time, 
                            win));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_RmaPut(OTF2_LocationRef location, OTF2_TimeStamp time, 
        void *userData, OTF2_AttributeList *attributeList, OTF2_RmaWinRef win, 
        uint32_t remote, uint64_t bytes, uint64_t matchingId)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
   
    OTF2_CHECK(OTF2_EvtWriter_RmaPut(tw->evt_writerMap[location], attributeList, time, 
                            win, remote, bytes, matchingId));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_RmaOpCompleteBlocking(OTF2_LocationRef location, OTF2_TimeStamp time, 
        void *userData, OTF2_AttributeList *attributeList, OTF2_RmaWinRef win, 
        uint64_t matchingId)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_RmaOpCompleteBlocking(tw->evt_writerMap[location], attributeList, time, 
                            win, matchingId));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_RmaGet(OTF2_LocationRef location, OTF2_TimeStamp time,
        void *userData, OTF2_AttributeList *attributeList, 
        OTF2_RmaWinRef win, uint32_t remote, uint64_t bytes, uint64_t matchingId)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_RmaGet(tw->evt_writerMap[location], attributeList, time, 
                            win, remote, bytes, matchingId));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_ThreadTeamBegin(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, OTF2_CommRef threadTeam)
{
    
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_ThreadTeamBegin(tw->evt_writerMap[locationID], attributeList, time, 
                            threadTeam));
    
    return OTF2_CALLBACK_SUCCESS;   
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackComm_ThreadTeamEnd(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, OTF2_CommRef threadTeam) 
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    OTF2_CHECK(OTF2_EvtWriter_ThreadTeamEnd(tw->evt_writerMap[locationID], attributeList, time, 
                            threadTeam));
    
    return OTF2_CALLBACK_SUCCESS;   
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackEnter(OTF2_LocationRef location, OTF2_TimeStamp time, 
                    void *userData, OTF2_AttributeList *attributes, OTF2_RegionRef region)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;

    // write next node in List    
    if(!tw->processNextNode(time, region))
            OTF2_CHECK(OTF2_EvtWriter_Enter(tw->evt_writerMap[location], attributes, time,
                    region));
    
    return OTF2_CALLBACK_SUCCESS;
    
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2CallbackLeave(OTF2_LocationRef location, OTF2_TimeStamp time, 
                    void *userData, OTF2_AttributeList *attributes,OTF2_RegionRef region)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;
    
    // write next node in List
    if(!tw->processNextNode(time, region))
            OTF2_CHECK(OTF2_EvtWriter_Leave(tw->evt_writerMap[location], attributes, time,
                    region));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalEvtReaderCallback_ThreadFork(OTF2_LocationRef locationID,    
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,      
        OTF2_Paradigm paradigm, uint32_t numberOfRequestedThreads)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;    
    
    OTF2_CHECK(OTF2_EvtWriter_ThreadFork(tw->evt_writerMap[locationID], attributeList, time, paradigm, numberOfRequestedThreads));
    
    return OTF2_CALLBACK_SUCCESS;
}
    
OTF2_CallbackCode OTF2ParallelTraceWriter::OTF2_GlobalEvtReaderCallback_ThreadJoin(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,
        OTF2_Paradigm paradigm)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;    
    
    OTF2_CHECK(OTF2_EvtWriter_ThreadJoin(tw->evt_writerMap[locationID], attributeList, time, paradigm));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2Callback_MpiRecv(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
        uint32_t sender, OTF2_CommRef communicator, uint32_t msgTag, 
        uint64_t msgLength)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;    
    
    OTF2_CHECK(OTF2_EvtWriter_MpiRecv(tw->evt_writerMap[locationID], attributeList, time, sender, 
            communicator, msgTag, msgLength));
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2ParallelTraceWriter::otf2Callback_MpiSend(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
        uint32_t receiver, OTF2_CommRef communicator, uint32_t msgTag, 
        uint64_t msgLength)
{
    OTF2ParallelTraceWriter* tw = (OTF2ParallelTraceWriter* ) userData;    
    
    OTF2_CHECK(OTF2_EvtWriter_MpiSend(tw->evt_writerMap[locationID], attributeList, time, receiver, 
            communicator, msgTag, msgLength));
    
    return OTF2_CALLBACK_SUCCESS;
}