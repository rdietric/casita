/* 
 * File:   OTF2ParallelTraceWriter.cpp
 * Author: Jonas
 * 
 * Created on 17. March 2014, 12:00
 */

#include <mpi.h>
#include <cmath>
#include <stdlib.h>
#include <stdio.h>
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
totalNumStreams(0),
global_def_writer(NULL),
tr(NULL),
streamRefKey(1),
eventRefKey(2),
funcResultKey(3),
attrListCUDAToken(1),
attrListCUDAMasterToken(2)
{
    mpiNumProcesses = new int[mpiSize];
    outputFilename.assign("");
    pathToFile.assign("");
    this->originalFilename.assign(originalFilename);
    
    this->ctrIdSet = ctrIdSet;
    
    flush_callbacks.otf2_post_flush = postFlush;
    flush_callbacks.otf2_pre_flush = preFlush;
    
    
    /*
    coll_callbacks.otf2_release = otf2_mpi_collectives_release;
    coll_callbacks.otf2_get_size          = otf2_mpi_collectives_get_size;
    coll_callbacks.otf2_get_rank          = otf2_mpi_collectives_get_rank;
    coll_callbacks.otf2_create_local_comm = otf2_mpi_collectives_create_local_comm;
    coll_callbacks.otf2_free_local_comm   = otf2_mpi_collectives_free_local_comm;
    coll_callbacks.otf2_barrier           = otf2_mpi_collectives_barrier;
    coll_callbacks.otf2_bcast             = otf2_mpi_collectives_bcast;
    coll_callbacks.otf2_gather            = otf2_mpi_collectives_gather;
    coll_callbacks.otf2_gatherv           = otf2_mpi_collectives_gatherv;
    coll_callbacks.otf2_scatter           = otf2_mpi_collectives_scatter;
    coll_callbacks.otf2_scatterv          = otf2_mpi_collectives_scatterv;
    */
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
    delete[] mpiNumProcesses;
}

void OTF2ParallelTraceWriter::copyGlobalDefinitions()
{
    
}

void OTF2ParallelTraceWriter::copyMasterControl()
{

}

void OTF2ParallelTraceWriter::open(const std::string otfFilename, uint32_t maxFiles,
        uint32_t numStreams, uint64_t timerResolution)
{
   
    /*
    OTF2_MPI_UserData *userData = (OTF2_MPI_UserData *) malloc( sizeof( *userData ) );
    userData->callbacks = coll_callbacks;
    userData->global.mpi_comm = commGroup;
    userData->local.mpi_comm = NULL;
    userData->local_created = false;
    */
    
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
    //OTF2_Archive_SetCollectiveCallbacks( archive, &userData->callbacks, userData, &userData->global,&userData->local);
    
    // read old OTF2-definitions
    tr = new OTF2TraceReader(this, mpiRank);
    tr->open(originalFilename,100);
    tr->readDefinitions();
    
    timerOffset = tr->getTimerOffset();
    
    // only master mpi process writes global definitions
    if(mpiRank==0) 
    {

        global_def_writer = OTF2_Archive_GetGlobalDefWriter( archive );

        // write old global definitions
        //Write Clockproperties:
        OTF2_CHECK(OTF2_GlobalDefWriter_WriteClockProperties( global_def_writer,
                    tr->getTimerResolution(),tr->getTimerOffset(),tr->getTraceLength()));

        //write Stringdefinitions
        OTF2TraceReader::TokenNameMap stringMap = tr->getDefinitionTokenStringMap();
        counterForStringDefinitions = stringMap.size();
        for(OTF2TraceReader::TokenNameMap::const_iterator iter = stringMap.begin();
                iter != stringMap.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteString(global_def_writer, iter->first, iter->second.c_str()));
        }
        
        // write SystemTree definitions
        std::list<OTF2TraceReader::OTF2SystemTreeNode> stnList = tr->getSystemTreeNodeList();
        for(std::list<OTF2TraceReader::OTF2SystemTreeNode>::const_iterator iter = stnList.begin();
                iter != stnList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNode(global_def_writer, 
                    iter->self, iter->name, iter->className, iter->parent));
        }
        
        std::list<OTF2TraceReader::OTF2SystemTreeNodeProperty> stnpList = tr->getSystemTreeNodePropertyList();
        for(std::list<OTF2TraceReader::OTF2SystemTreeNodeProperty>::const_iterator iter = stnpList.begin();
                iter != stnpList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNodeProperty(global_def_writer, 
                    iter->systemTreeNode, iter->name, iter->value));
        }
        
        std::list<OTF2TraceReader::OTF2SystemTreeNodeDomain> stndList = tr->getSystemTreeNodeDomainList();
        for(std::list<OTF2TraceReader::OTF2SystemTreeNodeDomain>::const_iterator iter = stndList.begin();
                iter != stndList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteSystemTreeNodeDomain(global_def_writer, 
                    iter->systemTreeNode, iter->systemTreeDomain));
        }
        
        //write LocationGroups
        std::list<OTF2TraceReader::OTF2LocationGroup> lgList = tr->getLocationGroupList();
        for(std::list<OTF2TraceReader::OTF2LocationGroup>::const_iterator iter = lgList.begin();
                iter != lgList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteLocationGroup(global_def_writer, 
                    iter->self, iter->name, iter->locationGroupType, iter->systemTreeParent));
            
        }
        
        OTF2_Archive_OpenDefFiles( archive );
        
        // write Locations and create local definition files for each location
        std::list<OTF2TraceReader::OTF2Location> lList = tr->getLocationList();
        for(std::list<OTF2TraceReader::OTF2Location>::const_iterator iter = lList.begin();
                iter != lList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteLocation(global_def_writer, iter->self,
                    iter->name, iter->locationType, iter->numberOfEvents, iter->locationGroup));
            
            // Write local definitionFiles for each process
            OTF2_DefWriter* def_writer = OTF2_Archive_GetDefWriter( archive, iter->self );
           
            OTF2_Archive_CloseDefWriter( archive, def_writer );
        }
        
        // write regions
        std::list<OTF2TraceReader::OTF2Region> rList = tr->getRegionList();
        for(std::list<OTF2TraceReader::OTF2Region>::const_iterator iter = rList.begin();
                iter != rList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteRegion(global_def_writer,iter->self,iter->name,
                    iter->cannonicalName,iter->description, iter->regionRole, iter->paradigm,
                    iter->regionFlags, iter->sourceFile, iter->beginLineNumber, iter->endLineNumber));
        }
        
        // write Attributes
        std::list<OTF2TraceReader::OTF2Attribute> aList = tr->getAttributeList();
        for(std::list<OTF2TraceReader::OTF2Attribute>::const_iterator iter = aList.begin();
                iter != aList.end(); iter++)
        {
            OTF2_CHECK(OTF2_GlobalDefWriter_WriteAttribute(global_def_writer,iter->self,iter->name,
                    iter->description, iter->type));
        }
        
    } 
    
    // open event to start creating event files for each location
    OTF2_Archive_OpenEvtFiles( archive );
    
    // create evtWriter for original locations
    std::list<OTF2TraceReader::OTF2Location> lList = tr->getLocationList();
    for(std::list<OTF2TraceReader::OTF2Location>::const_iterator iter = lList.begin();
            iter != lList.end(); iter++)
    {
        // only for locations of my rank
        if(!tr->isChildOf(iter->self, mpiRank))
            continue;
        
        OTF2_EvtWriter *evt_writer = OTF2_Archive_GetEvtWriter( archive, OTF2_UNDEFINED_LOCATION );
        OTF2_CHECK(OTF2_EvtWriter_SetLocationID( evt_writer, iter->self ));
        evt_writerMap[iter->self] = evt_writer;
    }
    
    MPI_CHECK(MPI_Allgather(&numStreams, 1, MPI_UNSIGNED,
            mpiNumProcesses, 1, MPI_INT, MPI_COMM_WORLD));
    for (uint32_t i = 0; i < mpiSize; ++i)
    {
        totalNumStreams += mpiNumProcesses[i];
    }

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

void OTF2ParallelTraceWriter::writeDefProcess(uint64_t id, uint64_t parentId,
        const char* name, ProcessGroup pg)
{
    
}

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
            if(node->isOMPParallelRegion())
            {
                OTF2_CHECK(OTF2_EvtWriter_ThreadFork(evt_writer, NULL, nodeTime,OTF2_PARADIGM_OPENMP, 1))
            } else
            {
                OTF2_CHECK(OTF2_EvtWriter_Enter(evt_writer, NULL, nodeTime,
                    node->getFunctionId()));
            }
        } else
        {
            if(node->isOMPParallelRegion())
            {
                OTF2_CHECK(OTF2_EvtWriter_ThreadJoin(evt_writer, NULL, nodeTime, OTF2_PARADIGM_OPENMP))
            } else
            {
                OTF2_CHECK(OTF2_EvtWriter_Leave(evt_writer, NULL, nodeTime,
                    node->getFunctionId()));
            }
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

void OTF2ParallelTraceWriter::writeRMANode(const Node *node,
        uint64_t prevProcessId, uint64_t nextProcessId)
{
    
}

void* OTF2ParallelTraceWriter::getWriteObject(uint64_t id)
{
    return evt_writerMap[id];
}





