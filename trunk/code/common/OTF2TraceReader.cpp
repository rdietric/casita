/* 
 * File:   OTF2TraceReader.cpp
 * Author: Jonas
 * 
 * Created on Feb 17, 2014, 1:04 PM
 */

#include <stdexcept>
#include <stack>
#include <cstring>
#include <list>
#include "common.hpp"
#include "otf/OTF2TraceReader.hpp"

#define OTF2_CHECK(cmd) \
        { \
                int _status = cmd; \
                if (_status) \
                        throw RTException("OTF2 command '%s' returned error", #cmd); \
        }

using namespace cdm::io;

OTF2TraceReader::OTF2TraceReader(void *userData, uint32_t mpiRank) :
ITraceReader(userData),
mpiRank(mpiRank),
mpiProcessId(1),
reader(NULL),
ticksPerSecond(1),
timerOffset(0),
processingPhase(0)
{

}

OTF2TraceReader::~OTF2TraceReader()
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

OTF2TraceReader::IdNameTokenMap& OTF2TraceReader::getProcessNameTokenMap()
{
    return processNameTokenMap;
}
    
OTF2TraceReader::IdNameTokenMap& OTF2TraceReader::getFunctionNameTokenMap()
{
    return functionNameTokenMap;
}

OTF2TraceReader::NameTokenMap& OTF2TraceReader::getNameKeysMap()
{
    return nameKeysMap;
}

OTF2TraceReader::TokenNameMap& OTF2TraceReader::getKeyNameMap()
{
    return kNameMap;
}

OTF2TraceReader::TokenTokenMap64& OTF2TraceReader::getProcessFamilyMap()
{
    return processFamilyMap;
}

OTF2TraceReader::IdTokenMap& OTF2TraceReader::getProcessRankMap()
{
    return processRankMap;
}

OTF2TraceReader::GroupIdGroupMap& OTF2TraceReader::getGroupMap()
{
    return groupMap;
}

int OTF2TraceReader::getProcessingPhase()
{
    return processingPhase;
}

OTF2KeyValueList& OTF2TraceReader::getKVList()
{
    return kvList;
}

uint64_t OTF2TraceReader::getMPIProcessId()
{
    return mpiProcessId;
}

void OTF2TraceReader::setMPIProcessId(uint64_t processId)
{
    mpiProcessId = processId;
}

void OTF2TraceReader::open(const std::string otfFilename, uint32_t maxFiles)
{
   baseFilename.assign("");
   baseFilename.append(otfFilename.c_str(), otfFilename.length());
   reader = OTF2_Reader_Open(baseFilename.c_str());
   
   // \todo remove this again -> just for reading serially -> one process after the other
   //OTF2_Reader_SetSerialCollectiveCallbacks(reader);
   
   if (!reader)
        throw RTException("Failed to open OTF2 trace file %s", baseFilename.c_str());
}

void OTF2TraceReader::close()
{
    OTF2_CHECK(OTF2_Reader_Close(reader));
}

void OTF2TraceReader::readEvents()
{
    for(IdNameTokenMap::const_iterator iter = processNameTokenMap.begin();
            iter != processNameTokenMap.end(); ++iter)
    {
        printf("[%u] Read for process %lu \n", mpiRank, iter->first);
        OTF2_Reader_SelectLocation(reader, iter->first);
    }
    
    OTF2_Reader_OpenEvtFiles(reader);
    OTF2_Reader_OpenDefFiles(reader);
    
    for(IdNameTokenMap::const_iterator iter = processNameTokenMap.begin();
            iter != processNameTokenMap.end(); ++iter)
    {
        OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, iter->first );
        uint64_t def_reads = 0;
        OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
        OTF2_Reader_CloseDefReader( reader, def_reader );
        OTF2_Reader_GetEvtReader( reader, iter->first );
    }
    
    OTF2_Reader_CloseDefFiles(reader);
    
    OTF2_GlobalEvtReader* global_evt_reader = OTF2_Reader_GetGlobalEvtReader( reader );
    
    OTF2_GlobalEvtReaderCallbacks* event_callbacks = OTF2_GlobalEvtReaderCallbacks_New();
    OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks, &otf2CallbackEnter );
    OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks, &otf2CallbackLeave );
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback( event_callbacks, &otf2Callback_MpiCollectiveEnd );
    OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback(event_callbacks, &otf2Callback_MpiRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback(event_callbacks, &otf2Callback_MpiSend );
    OTF2_Reader_RegisterGlobalEvtCallbacks( reader, global_evt_reader, event_callbacks, this );
    OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );
    
    uint64_t events_read = 0;

    // returns 0 if successfull, >0 otherwise
    if (OTF2_Reader_ReadAllGlobalEvents(reader, global_evt_reader, &events_read))
        throw RTException("Failed to read OTF2 events");
    
    printf("[%u] Read %lu events. \n", mpiRank, events_read);
    
    OTF2_Reader_CloseGlobalEvtReader(reader, global_evt_reader);
    
    OTF2_Reader_CloseEvtFiles(reader);
    
}

void OTF2TraceReader::readEventsForProcess(uint64_t id)
{
    OTF2_Reader_SelectLocation(reader, id);
    
    OTF2_Reader_OpenEvtFiles(reader);
    
    OTF2_DefReader* def_reader = OTF2_Reader_GetDefReader( reader, id );
    uint64_t def_reads = 0;
    OTF2_Reader_ReadAllLocalDefinitions( reader, def_reader, &def_reads );
    OTF2_Reader_CloseDefReader( reader, def_reader );

    OTF2_Reader_GetEvtReader( reader, id );
    
    OTF2_GlobalEvtReader* global_evt_reader = OTF2_Reader_GetGlobalEvtReader( reader );
    
    OTF2_GlobalEvtReaderCallbacks* event_callbacks = OTF2_GlobalEvtReaderCallbacks_New();
    OTF2_GlobalEvtReaderCallbacks_SetEnterCallback( event_callbacks, &otf2CallbackEnter );
    OTF2_GlobalEvtReaderCallbacks_SetLeaveCallback( event_callbacks, &otf2CallbackLeave );
    OTF2_GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback( event_callbacks, &otf2Callback_MpiCollectiveEnd );
    OTF2_GlobalEvtReaderCallbacks_SetMpiRecvCallback(event_callbacks, &otf2Callback_MpiRecv );
    OTF2_GlobalEvtReaderCallbacks_SetMpiSendCallback(event_callbacks, &otf2Callback_MpiSend );
    OTF2_Reader_RegisterGlobalEvtCallbacks( reader, global_evt_reader, event_callbacks, this );
    OTF2_GlobalEvtReaderCallbacks_Delete( event_callbacks );
    
    uint64_t events_read = 0;

    // returns 0 if successfull, >0 otherwise
    if (OTF2_Reader_ReadAllGlobalEvents(reader, global_evt_reader, &events_read))
        throw RTException("Failed to read OTF2 events");
    
    OTF2_Reader_CloseGlobalEvtReader(reader, global_evt_reader);
    
    OTF2_Reader_CloseEvtFiles(reader);
    
}

void OTF2TraceReader::readDefinitions()
{
    OTF2_GlobalDefReader* global_def_reader = OTF2_Reader_GetGlobalDefReader( reader );
    
    OTF2_GlobalDefReaderCallbacks* global_def_callbacks = OTF2_GlobalDefReaderCallbacks_New();
    
    processingPhase = 1;
    
    // Phase 1 -> read string definitions and Groups
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
    
    // register callbacks
    OTF2_Reader_RegisterGlobalDefCallbacks( reader, global_def_reader, global_def_callbacks, this );
    
    OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );    
    
    uint64_t definitions_read = 0;
    // read definitions
    OTF2_Reader_ReadAllGlobalDefinitions( reader, global_def_reader, &definitions_read );
    
    printf("Read %lu definitions in Phase 1 \n ", definitions_read);
    
    close();
    
    open(baseFilename.c_str(),10);
    
    processingPhase = 2;
    // read definitions (part 2)
    global_def_reader = OTF2_Reader_GetGlobalDefReader( reader );
    
    global_def_callbacks = OTF2_GlobalDefReaderCallbacks_New();

    // Phase 2 -> read string definitions and Groups
    OTF2_GlobalDefReaderCallbacks_SetLocationCallback(global_def_callbacks, 
            &OTF2_GlobalDefReaderCallback_Location);    
    
    // register callbacks
    OTF2_Reader_RegisterGlobalDefCallbacks( reader, global_def_reader, global_def_callbacks, this );
    
    OTF2_GlobalDefReaderCallbacks_Delete( global_def_callbacks );    
    
    definitions_read = 0;

    // read definitions
    OTF2_Reader_ReadAllGlobalDefinitions( reader, global_def_reader, &definitions_read );
    
    printf("Read %lu definitions in Phase 2 \n ", definitions_read);
}


OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_ClockProperties(void *userData, 
        uint64_t timerResolution, uint64_t globalOffset, uint64_t traceLength)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;

    tr->setTimerOffset(globalOffset);
    tr->setTimerResolution(timerResolution);

    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode  OTF2TraceReader::OTF2_GlobalDefReaderCallback_LocationGroup(void *userData, 
        OTF2_LocationGroupRef self, OTF2_StringRef name, OTF2_LocationGroupType locationGroupType, 
        OTF2_SystemTreeNodeRef systemTreeParent)
{
    //\TODO save locationGroups somewhere
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_Location(void *userData, 
        OTF2_LocationRef self, OTF2_StringRef name, OTF2_LocationType locationType, 
        uint64_t numberOfEvents, OTF2_LocationGroupRef locationGroup)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    int phase = tr->getProcessingPhase();
    
    if(phase == 1)
    {
        tr->getProcessFamilyMap()[self] = locationGroup;
    }
    
    if(phase == 2)
    {
    
        if (tr->handleProcessMPIMapping)
        {
            tr->handleProcessMPIMapping(tr, self, locationGroup);
        }

        // skip all processes but the mapping MPI master process and its children
        if (self != tr->getMPIProcessId() && (!tr->isChildOf(self, tr->getMPIProcessId())))
            return OTF2_CALLBACK_SUCCESS;

        //Locations are processes
        tr->getProcessNameTokenMap()[self] = name;

        if (tr->handleDefProcess)
        {
            tr->handleDefProcess(tr, 0, self, locationGroup, tr->getStringRef(name).c_str(),
                    NULL, locationType == OTF2_LOCATION_TYPE_GPU ? true : false , false);
        }
    }
    
    return OTF2_CALLBACK_SUCCESS;
}
    
OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_Group(void *userData,
                    OTF2_GroupRef self, OTF2_StringRef name, OTF2_GroupType groupType, 
                    OTF2_Paradigm paradigm, OTF2_GroupFlag groupFlags, uint32_t numberOfMembers, 
                    const uint64_t *members)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;

    OTF2Group myGroup;
    myGroup.groupId = self;
    myGroup.members = members;
    myGroup.numberOfMembers = numberOfMembers;
    myGroup.paradigm = paradigm;
    myGroup.stringRef = name;
    
    tr->getGroupMap()[self] = myGroup;
    
    if((groupType == OTF2_GROUP_TYPE_COMM_LOCATIONS) && (paradigm == OTF2_PARADIGM_MPI))
    {
        uint32_t mpiRank = tr->getMPIRank();

        if (numberOfMembers <= tr->getMPIRank())
            throw RTException("Process group MPI_COMM_WORLD has no process for this mpi rank (%u)",
                mpiRank);

        IdTokenMap &processRankMap = tr->getProcessRankMap();
        tr->setMPIProcessId(members[mpiRank]);
        for (uint32_t i = 0; i < numberOfMembers; ++i)
        {
            processRankMap[members[i]] = i;
        }

        printf("Rank %u maps to process %lu\n", mpiRank, members[mpiRank]);
        
        if(tr->handleMPICommGroup)
        {
            tr->handleMPICommGroup(tr,0,myGroup.numberOfMembers,myGroup.members);
        } 
     
    }
    
    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_Comm(void *userData, 
                    OTF2_CommRef self, OTF2_StringRef name, OTF2_GroupRef group, OTF2_CommRef parent)
{
    
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    
    OTF2Group myGroup = tr->getGroupMap()[group];
    
    if(myGroup.paradigm == OTF2_PARADIGM_MPI)
    {
        if(tr->handleMPICommGroup)
        {
            tr->handleMPICommGroup(tr,self,myGroup.numberOfMembers,myGroup.members);
        } 
    }
    
    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_String(void *userData, 
                    OTF2_StringRef self, const char *string)
{
    
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    uint32_t max_length = 1000;
    std::string str(string, strnlen(string, max_length));
    tr->getDefinitionTokenStringMap()[self] = str;
    
    return OTF2_CALLBACK_SUCCESS;    
}

OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_Region(void *userData,
                    OTF2_RegionRef self, OTF2_StringRef name, OTF2_StringRef cannonicalName,
                    OTF2_StringRef description, OTF2_RegionRole regionRole, OTF2_Paradigm paradigm,
                    OTF2_RegionFlag regionFlags, OTF2_StringRef sourceFile, uint32_t beginLineNumber,
                    uint32_t endLineNumber)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    //Locations are processes
    tr->getFunctionNameTokenMap()[self] = name;
    
    if (tr->handleDefFunction)
        tr->handleDefFunction(tr, 0, self, tr->getFunctionName(self).c_str(), paradigm);
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::OTF2_GlobalDefReaderCallback_Attribute(void *userData, 
                    OTF2_AttributeRef self, OTF2_StringRef name, OTF2_Type type)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    std::string s = tr->getKeyName(name);
    tr->getNameKeysMap().insert(std::make_pair(s, self));
    tr->getKeyNameMap().insert(std::make_pair(self, s));

    if (tr->handleDefKeyValue)
    {
        tr->handleDefKeyValue(tr, 0, self, s.c_str(), "");
    }
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2CallbackEnter(OTF2_LocationRef location, OTF2_TimeStamp time, 
                    void *userData, OTF2_AttributeList *attributes, OTF2_RegionRef region)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    
    
    
    if (tr->handleEnter)
    {
        OTF2KeyValueList& kvList = tr->getKVList();
        kvList.setList(attributes);
        
        tr->handleEnter(tr, time - tr->getTimerOffset(), region, location, (IKeyValueList*) & kvList);
    }
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2CallbackLeave(OTF2_LocationRef location, OTF2_TimeStamp time, 
                    void *userData, OTF2_AttributeList *attributes,OTF2_RegionRef region)
{
    
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;

    if (tr->handleLeave)
    {
        OTF2KeyValueList& kvList = tr->getKVList();
        kvList.setList(attributes);
        
        tr->handleLeave(tr, time - tr->getTimerOffset(), region, location, (IKeyValueList*) & kvList);
    }
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2Callback_MpiCollectiveEnd(OTF2_LocationRef locationID,
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,
                    OTF2_CollectiveOp collectiveOp, OTF2_CommRef communicator, uint32_t root,
                    uint64_t sizeSent, uint64_t sizeReceived)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    
    if (tr->handleMPIComm)
    {
        io::MPIType mpiType = io::MPI_COLLECTIVE;
        switch (collectiveOp)
        {
            case OTF2_COLLECTIVE_OP_BCAST:
            case OTF2_COLLECTIVE_OP_GATHER:
            case OTF2_COLLECTIVE_OP_GATHERV:
            case OTF2_COLLECTIVE_OP_REDUCE:
            case OTF2_COLLECTIVE_OP_SCATTER:
            case OTF2_COLLECTIVE_OP_SCATTERV:
                mpiType = io::MPI_ONEANDALL;
                break;
        }
        
        tr->handleMPIComm(tr, mpiType, locationID, communicator, root, 0);
    }
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2Callback_MpiRecv(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
        uint32_t sender, OTF2_CommRef communicator, uint32_t msgTag, 
        uint64_t msgLength)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;

    if (tr->handleMPIComm)
    {
        tr->handleMPIComm(tr, MPI_RECV, locationID, sender, 0, msgTag);
    }
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2Callback_MpiSend(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
        uint32_t receiver, OTF2_CommRef communicator, uint32_t msgTag, 
        uint64_t msgLength)
{
    OTF2TraceReader *tr = (OTF2TraceReader*) userData;
    
    if (tr->handleMPIComm)
    {
        tr->handleMPIComm(tr, MPI_SEND, locationID, receiver, 0, msgTag);
    }
    
    return OTF2_CALLBACK_SUCCESS;
}

OTF2_CallbackCode OTF2TraceReader::otf2Callback_OmpFork(OTF2_LocationRef locationID,    
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,      
        uint32_t numberOfRequestedThreads)
{
    //\TODO implement
    return OTF2_CALLBACK_SUCCESS;
}
    
OTF2_CallbackCode OTF2TraceReader::otf2Callback_OmpJoin(OTF2_LocationRef locationID, 
        OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList){
    
    //\TODO implement
    return OTF2_CALLBACK_SUCCESS;
}


std::string OTF2TraceReader::getStringRef(Token t)
{
    return getKeyName(t);
}


OTF2TraceReader::TokenNameMap& OTF2TraceReader::getDefinitionTokenStringMap(){
    return definitionTokenStringMap;
}


uint64_t OTF2TraceReader::getTimerResolution()
{
    return ticksPerSecond;
}

void OTF2TraceReader::setTimerResolution(uint64_t ticksPerSecond)
{
    this->ticksPerSecond = ticksPerSecond;
}

uint64_t OTF2TraceReader::getTimerOffset()
{
    return timerOffset;
}

void OTF2TraceReader::setTimerOffset(uint64_t offset)
{
    this->timerOffset = offset;
}

uint32_t OTF2TraceReader::getMPIRank()
{
    return mpiRank;
}

std::string OTF2TraceReader::getKeyName(uint32_t id)
{
    TokenNameMap &nm = getDefinitionTokenStringMap();
    TokenNameMap::iterator iter = nm.find(id);
    if (iter != nm.end())
        return iter->second;
    else
        return "(unknown)";
}

std::string OTF2TraceReader::getFunctionName(uint64_t id)
{
    return getKeyName(getFunctionNameTokenMap()[id]);
}

OTF2TraceReader::ProcessGroupMap& OTF2TraceReader::getProcGoupMap()
{
    return processGroupMap;
}

std::string OTF2TraceReader::getProcessName(uint64_t id)
{
    return getKeyName(id);
}

std::vector<uint32_t> OTF2TraceReader::getKeys(const std::string keyName)
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

uint32_t OTF2TraceReader::getFirstKey(const std::string keyName)
{
    std::pair <NameTokenMap::iterator, NameTokenMap::iterator> range;
    range = nameKeysMap.equal_range(keyName);

    if (range.first != range.second)
        return range.first->second;
    else
        return 0;
}

bool OTF2TraceReader::isChildOf(uint64_t child, uint64_t parent)
{
    TokenTokenMap64::const_iterator iter = processFamilyMap.find(child);
    if (iter == processFamilyMap.end())
        throw RTException("Requesting parent of unknown child process");

    bool myOwnParent = false;
    
    iter = processFamilyMap.find(child);
    
    while (!myOwnParent)
    {
        uint64_t directParent = iter->second;

        if (directParent == parent)
        {
            return true;
        }

        iter = processFamilyMap.find(directParent);
        if (iter == processFamilyMap.end())
        {
            return false;
        }
        
        if(directParent == iter->second)
            myOwnParent = true;
    }
    
    return false;
}