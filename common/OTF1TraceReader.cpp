/* 
 * File:   OTF1TraceReader.cpp
 * Author: felix
 * 
 * Created on May 7, 2013, 1:04 PM
 */

#include <stdexcept>
#include <stack>

#include "otf/OTF1TraceReader.hpp"
#include "common.hpp"

#define OTF_CHECK(cmd) \
        { \
                int _status = cmd; \
                if (!_status) \
                        throw RTException("OTF command '%s' returned error", #cmd); \
        }

using namespace cdm::io;

OTF1TraceReader::OTF1TraceReader(void *userData, uint32_t mpiRank) :
ITraceReader(userData),
mpiRank(mpiRank),
mpiProcessId(1),
fileMgr(NULL),
reader(NULL),
ticksPerSecond(1),
processingPhase(0)
{

}

OTF1TraceReader::~OTF1TraceReader()
{
    for (ProcessGroupMap::iterator iter = processGroupMap.begin();
            iter != processGroupMap.end(); ++iter)
    {
        ProcessGroup *pg = iter->second;
        if (pg->name != NULL)
            delete[] pg->name;
        if (pg->procs != NULL)
            delete[] pg->procs;
        delete pg;
    }
}

int OTF1TraceReader::getProcessingPhase()
{
    return processingPhase;
}

uint32_t OTF1TraceReader::getMPIRank()
{
    return mpiRank;
}

uint32_t OTF1TraceReader::getMPIProcessId()
{
    return mpiProcessId;
}

void OTF1TraceReader::setMPIProcessId(uint32_t processId)
{
    mpiProcessId = processId;
}

OTF1TraceReader::TokenTokenMap& OTF1TraceReader::getProcessRankMap()
{
    return processRankMap;
}

OTF1TraceReader::TokenTokenMap& OTF1TraceReader::getProcessFamilyMap()
{
    return processFamilyMap;
}

void OTF1TraceReader::open(const std::string otfFilename, uint32_t maxFiles)
{
    fileMgr = OTF_FileManager_open(maxFiles);

    baseFilename.assign("");
    baseFilename.append(otfFilename.c_str(), otfFilename.length() - 4);
    reader = OTF_Reader_open(baseFilename.c_str(), fileMgr);
    
    if (!reader)
        throw RTException("Failed to open OTF1 trace file %s", baseFilename.c_str());

    OTF_Reader_setRecordLimit(reader, OTF_READ_MAXRECORDS);
}

void OTF1TraceReader::close()
{
    OTF_CHECK(OTF_Reader_close(reader));
    OTF_FileManager_close(fileMgr);
}

void OTF1TraceReader::setEventHandlers(OTF_HandlerArray* handlers)
{
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleEnter,
            OTF_ENTER_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_ENTER_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleLeave,
            OTF_LEAVE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_LEAVE_RECORD);

    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleRecvMsg,
            OTF_RECEIVE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_RECEIVE_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleSendMsg,
            OTF_SEND_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_SEND_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleBeginCollectiveOperation,
            OTF_BEGINCOLLOP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_BEGINCOLLOP_RECORD);
}

void OTF1TraceReader::readEvents()
{
    OTF_HandlerArray* handlers = OTF_HandlerArray_open();
    setEventHandlers(handlers);

    if (OTF_Reader_readEvents(reader, handlers) == OTF_READ_ERROR)
        throw RTException("Failed to read OTF events");
    OTF_HandlerArray_close(handlers);
}

void OTF1TraceReader::readEventsForProcess(uint64_t id)
{
    uint32_t otf1_id = (uint32_t)id;
    
    OTF_HandlerArray* handlers = OTF_HandlerArray_open();
    setEventHandlers(handlers);
    
    OTF_RStream* rStream = OTF_Reader_getStream(reader, otf1_id);
    OTF_RStream_setRecordLimit(rStream, OTF_READ_MAXRECORDS);
    OTF_RStream_readEvents(rStream, handlers);
    
    OTF_HandlerArray_close(handlers);
}

void OTF1TraceReader::readDefinitions()
{
    OTF_HandlerArray* handlers = OTF_HandlerArray_open();

    // find (MPI) master process
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcessGroupMPI,
            OTF_DEFPROCESSGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESSGROUP_RECORD);

    if (OTF_Reader_readDefinitions(reader, handlers) == OTF_READ_ERROR)
        throw RTException("Failed to read OTF definitions for MPI mapping");
    OTF_HandlerArray_close(handlers);
    OTF_Reader_close(reader);

    processingPhase = 1;

    // read definitions (part 1)
    reader = OTF_Reader_open(baseFilename.c_str(), fileMgr);
    handlers = OTF_HandlerArray_open();
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefAttributeList,
            OTF_DEFATTRLIST_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFATTRLIST_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcess,
            OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcessGroup,
            OTF_DEFPROCESSGROUP_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESSGROUP_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcessOrGroupAttributes,
            OTF_DEFPROCESSORGROUPATTR_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESSORGROUPATTR_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefTimerResolution,
            OTF_DEFTIMERRESOLUTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFTIMERRESOLUTION_RECORD);

    if (OTF_Reader_readDefinitions(reader, handlers) == OTF_READ_ERROR)
        throw RTException("Failed to read OTF definitions part 2");
    OTF_HandlerArray_close(handlers);
    OTF_Reader_close(reader);

    processingPhase = 2;

    // read definitions (part 2)
    reader = OTF_Reader_open(baseFilename.c_str(), fileMgr);
    handlers = OTF_HandlerArray_open();
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefProcess,
            OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFPROCESS_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefFunction,
            OTF_DEFFUNCTION_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFFUNCTION_RECORD);
    OTF_HandlerArray_setHandler(handlers, (OTF_FunctionPointer*) otf1HandleDefKeyValue,
            OTF_DEFKEYVALUE_RECORD);
    OTF_HandlerArray_setFirstHandlerArg(handlers, this, OTF_DEFKEYVALUE_RECORD);

    if (OTF_Reader_readDefinitions(reader, handlers) == OTF_READ_ERROR)
        throw RTException("Failed to read OTF definitions part 2");
    OTF_HandlerArray_close(handlers);

    // disable reading for all processes which are not in the process map
    OTF_CHECK(OTF_Reader_setProcessStatusAll(reader, 0));
    for (TokenNameMap::const_iterator iter = pNameMap.begin();
            iter != pNameMap.end(); ++iter)
    {
        OTF_CHECK(OTF_Reader_setProcessStatus(reader, iter->first, 1));
    }
}

OTF1KeyValueList& OTF1TraceReader::getKVList()
{
    return kvList;
}

OTF1TraceReader::NameTokenMap& OTF1TraceReader::getNameKeysMap()
{
    return nameKeysMap;
}

OTF1TraceReader::TokenNameMap& OTF1TraceReader::getKeyNameMap()
{
    return kNameMap;
}

OTF1TraceReader::TokenNameMap& OTF1TraceReader::getFuncNameMap()
{
    return fNameMap;
}

std::string OTF1TraceReader::getKeyName(uint32_t id)
{
    TokenNameMap &nm = getKeyNameMap();
    TokenNameMap::iterator iter = nm.find(id);
    if (iter != nm.end())
        return iter->second;
    else
        return "(unknown)";
}

std::string OTF1TraceReader::getFunctionName(uint64_t id)
{
    uint32_t otf1_id = (uint32_t)id;
    TokenNameMap::iterator iter = fNameMap.find(otf1_id);
    if (iter != fNameMap.end())
        return iter->second;
    else
        return "(unknown)";
}

OTF1TraceReader::TokenNameMap& OTF1TraceReader::getProcNameMap()
{
    return pNameMap;
}

OTF1TraceReader::ProcessGroupMap& OTF1TraceReader::getProcGoupMap()
{
    return processGroupMap;
}

OTF1TraceReader::AttrListMap& OTF1TraceReader::getAttrListMap()
{
    return attrListMap;
}

OTF1TraceReader::ProcessFuncStack& OTF1TraceReader::getFuncStack()
{
    return funcStack;
}

std::string OTF1TraceReader::getProcessName(uint64_t id)
{
    uint32_t otf1_id = (uint32_t) id;
    TokenNameMap &nm = getProcNameMap();
    TokenNameMap::iterator iter = nm.find(otf1_id);
    if (iter != nm.end())
        return iter->second;
    else
        return "unknown";
}

std::vector<uint32_t> OTF1TraceReader::getKeys(const std::string keyName)
{
    std::vector<uint32_t> keys;

    std::pair <NameTokenMap::iterator, NameTokenMap::iterator> range;
    range = nameKeysMap.equal_range(keyName);
    for (NameTokenMap::iterator iter = range.first; iter != range.second; ++iter)
    {
        keys.push_back(iter->second);
    }

    return keys;
}

int32_t OTF1TraceReader::getFirstKey(const std::string keyName)
{
    std::pair <NameTokenMap::iterator, NameTokenMap::iterator> range;
    range = nameKeysMap.equal_range(keyName);

    if (range.first != range.second)
        return range.first->second;
    else
        return -1;
}

uint64_t OTF1TraceReader::getTimerResolution()
{
    return ticksPerSecond;
}

void OTF1TraceReader::setTimerResolution(uint64_t ticksPerSecond)
{
    this->ticksPerSecond = ticksPerSecond;
}

bool OTF1TraceReader::isChildOf(uint32_t child, uint32_t parent)
{
    TokenTokenMap::const_iterator iter = processFamilyMap.find(child);
    if (iter == processFamilyMap.end())
        throw RTException("Requesting parent of unknown child process");

    uint32_t directParent = child;

    while (directParent)
    {
        uint32_t directParent = iter->second;

        if (directParent == parent)
            return true;

        iter = processFamilyMap.find(directParent);
        if (iter == processFamilyMap.end())
            return false;
    }

    return false;
}

int OTF1TraceReader::otf1HandleEnter(void *userData, uint64_t time, uint32_t functionId,
        uint32_t processId, uint32_t source, OTF_KeyValueList_struct *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    if (tr->handleEnter)
    {
        tr->getFuncStack()[processId].push(functionId);

        OTF1KeyValueList& kvList = tr->getKVList();
        kvList.setList(list);
        tr->handleEnter(tr, time, functionId, processId, (IKeyValueList*) & kvList);
    }
    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleLeave(void *userData, uint64_t time, uint32_t functionId,
        uint32_t processId, uint32_t source, OTF_KeyValueList_struct * list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    if (tr->handleLeave)
    {
        ProcessFuncStack &funcStack = tr->getFuncStack();
        functionId = funcStack[processId].top();
        funcStack[processId].pop();

        OTF1KeyValueList& kvList = tr->getKVList();
        kvList.setList(list);
        tr->handleLeave(tr, time, functionId, processId, (IKeyValueList*) & kvList);
    }
    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefProcess(void * userData, uint32_t stream, uint32_t processId,
        const char * name, uint32_t parent, OTF_KeyValueList_struct * list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    int phase = tr->getProcessingPhase();

    if (phase == 1)
    {
        tr->getProcessFamilyMap()[processId] = parent;
    }

    if (phase == 2)
    {
        if (tr->handleProcessMPIMapping)
        {
            if (parent == 0)
                tr->handleProcessMPIMapping(tr, processId, tr->getProcessRankMap()[processId]);
            else
                tr->handleProcessMPIMapping(tr, processId, tr->getProcessRankMap()[parent]);
        }

        // skip all processes but the mapping MPI master process and its children
        if (processId != tr->getMPIProcessId() && (!tr->isChildOf(processId, tr->getMPIProcessId())))
            return OTF_RETURN_OK;

        tr->getProcNameMap()[processId] = name;

        if (tr->handleDefProcess)
        {
            bool isCUDA = false;
            bool isCUDAMaster = false;
            for (ProcessGroupMap::iterator iter = tr->getProcGoupMap().begin();
                    iter != tr->getProcGoupMap().end(); ++iter)
            {
                for (uint32_t p = 0; p < iter->second->numProcs; ++p)
                    if (iter->second->procs[p] == processId)
                    {
                        if (iter->second->isCUDA)
                            isCUDA = true;
                        if (iter->second->isCUDAMaster)
                            isCUDAMaster = true;
                    }
            }

            OTF1KeyValueList kvList(list);
            tr->handleDefProcess(tr, stream, processId, parent, name,
                    (IKeyValueList*) & kvList, isCUDA, isCUDAMaster);
        }
    }
    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefProcessGroupMPI(void *userData, uint32_t stream,
        uint32_t procGroup, const char *name, uint32_t numberOfProcs,
        const uint32_t *procs, OTF_KeyValueList *list)
{
    
    uint64_t otf2_procs[numberOfProcs];
    for(uint32_t i = 0; i < numberOfProcs; ++i){
        otf2_procs[i]=procs[i];
    }
    
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    if (strstr(name, "MPI") != NULL)
    {
        if (tr->handleMPICommGroup)
        {
            tr->handleMPICommGroup(tr, procGroup, numberOfProcs, otf2_procs);
        }

        // get mpiRank'th process from MPI_COMM_WORLD group
        if (strcmp(name, "MPI_COMM_WORLD") == 0)
        {
            uint32_t mpiRank = tr->getMPIRank();

            if (numberOfProcs <= tr->getMPIRank())
                throw RTException("Process group MPI_COMM_WORLD has no process for this mpi rank (%u)",
                    mpiRank);

            TokenTokenMap &processRankMap = tr->getProcessRankMap();
            tr->setMPIProcessId(procs[mpiRank]);
            for (uint32_t i = 0; i < numberOfProcs; ++i)
            {
                processRankMap[procs[i]] = i;
            }

            printf("Rank %u maps to process %u\n", mpiRank, procs[mpiRank]);

            if (tr->handleMPICommGroup)
                tr->handleMPICommGroup(tr, 0, numberOfProcs, otf2_procs);
        }
    }

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefProcessGroup(void *userData, uint32_t stream,
        uint32_t procGroup, const char *name, uint32_t numberOfProcs,
        const uint32_t *procs, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    ProcessGroup *pg = NULL;
    ProcessGroupMap::iterator pgIter = tr->getProcGoupMap().find(procGroup);
    if (pgIter == tr->getProcGoupMap().end())
    {
        pg = new ProcessGroup();
        pg->isCUDA = false;
        pg->isCUDAMaster = false;
        tr->getProcGoupMap()[procGroup] = pg;
    } else
        pg = pgIter->second;

    pg->name = new char[strlen(name) + 1];
    strcpy(pg->name, name);

    pg->numProcs = numberOfProcs;
    if (numberOfProcs > 0)
        pg->procs = new uint32_t[numberOfProcs];
    else
        pg->procs = NULL;

    //printf("process group: %u %s: (", procGroup, name);

    for (uint32_t i = 0; i < numberOfProcs; ++i)
    {
        pg->procs[i] = procs[i];
        //printf("%u, ", procs[i]);
    }

    //printf(")\n");

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefProcessOrGroupAttributes(void *userData,
        uint32_t stream, uint32_t proc_token, uint32_t attr_token, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    AttrListMap::iterator attrIter = tr->getAttrListMap().find(attr_token);
    if (attrIter == tr->getAttrListMap().end())
        throw std::runtime_error("Attribute list not found");

    std::vector<OTF_ATTR_TYPE>& attrList = attrIter->second;
    ProcessGroup *pg = NULL;
    ProcessGroupMap::iterator pgIter = tr->getProcGoupMap().find(proc_token);
    if (pgIter == tr->getProcGoupMap().end())
    {
        pg = new ProcessGroup();
        pg->name = NULL;
        pg->numProcs = 0;
        tr->getProcGoupMap()[proc_token] = pg;
    } else
        pg = pgIter->second;

    bool isCUDA = false;
    bool isCUDAMaster = false;
    for (std::vector<OTF_ATTR_TYPE>::iterator iter = attrList.begin();
            iter != attrList.end(); ++iter)
    {
        if (*iter == OTF_ATTR_IsCUDAThread)
            isCUDA = true;
        if (*iter == OTF_ATTR_IsMasterThread)
            isCUDAMaster = true;
    }

    if (!isCUDA && isCUDAMaster)
        throw std::runtime_error("Wrong attribute combination");

    pg->isCUDA = isCUDA;
    pg->isCUDAMaster = isCUDAMaster;

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefAttributeList(void *userData, uint32_t stream,
        uint32_t attr_token, uint32_t num, OTF_ATTR_TYPE *array, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    for (uint32_t i = 0; i < num; ++i)
    {
        tr->getAttrListMap()[attr_token].push_back(array[i]);
    }

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefFunction(void *userData, uint32_t stream,
        uint32_t func, const char *name, uint32_t funcGroup, uint32_t source)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    tr->getFuncNameMap()[func] = name;

    if (tr->handleDefFunction)
        tr->handleDefFunction(tr, stream, func, name, funcGroup);

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefKeyValue(void* userData, uint32_t stream, uint32_t key,
        OTF_Type type, const char *name, const char *description, OTF_KeyValueList_struct *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;
    tr->getNameKeysMap().insert(std::make_pair(name, key));
    tr->getKeyNameMap().insert(std::make_pair(key, name));

    if (tr->handleDefKeyValue)
    {
        tr->handleDefKeyValue(tr, stream, key, name, description);
    }
    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleDefTimerResolution(void *userData, uint32_t stream,
        uint64_t ticksPerSecond, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    if (tr->getMPIRank() == 0)
        printf("[0] timer resolution = %llu\n", (long long unsigned)ticksPerSecond);
    
    tr->setTimerResolution(ticksPerSecond);

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleSendMsg(void *userData, uint64_t time, uint32_t sender,
        uint32_t receiver, uint32_t group, uint32_t type, uint32_t length,
        uint32_t source, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    if (tr->handleMPIComm)
    {
        tr->handleMPIComm(tr, MPI_SEND, sender, receiver, 0, 0);
    }

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleRecvMsg(void *userData, uint64_t time, uint32_t receiver,
        uint32_t sender, uint32_t group, uint32_t type, uint32_t length,
        uint32_t source, OTF_KeyValueList *list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    if (tr->handleMPIComm)
    {
        tr->handleMPIComm(tr, MPI_RECV, receiver, sender, 0, 0);
    }

    return OTF_RETURN_OK;
}

int OTF1TraceReader::otf1HandleBeginCollectiveOperation(void * userData, uint64_t time,
        uint32_t process, uint32_t collOp, uint64_t matchingId, uint32_t procGroup,
        uint32_t rootProc, uint64_t sent, uint64_t received, uint32_t scltoken,
        OTF_KeyValueList * list)
{
    OTF1TraceReader *tr = (OTF1TraceReader*) userData;

    if (tr->handleMPIComm)
    {
        io::MPIType mpiType = io::MPI_COLLECTIVE;
        if (rootProc)
            mpiType = io::MPI_ONEANDALL;

        tr->handleMPIComm(tr, mpiType, process, procGroup, rootProc, 0);
    }

    return OTF_RETURN_OK;
}