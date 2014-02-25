/* 
 * File:   OTF2TraceReader.hpp
 * Author: stolle
 *
 * Created on February 17, 2014, 1:32 PM
 */

#ifndef OTF2TRACEREADER_HPP
#define	OTF2TRACEREADER_HPP

#include <otf2/otf2.h>
#include <map>
#include <vector>
#include <stack>
#include <set>
#include "ITraceReader.hpp"

namespace cdm
{
    namespace io
    {

        class OTF2TraceReader : public ITraceReader
        {
        public:

            typedef struct
            {
                char *name;
                bool isCUDA;
                bool isCUDAMaster;
                uint32_t numProcs;
                uint64_t *procs;
            } ProcessGroup;

            typedef uint32_t Token;
            typedef std::multimap<std::string, Token> NameTokenMap;
            typedef std::map<Token, std::string> TokenNameMap;
            typedef std::map<uint64_t,Token> IdNameTokenMap;
            typedef std::map<Token, Token> TokenTokenMap;
            typedef std::set<Token> TokenSet;
            typedef std::map<Token, TokenSet> TokenSetMap;

            typedef std::map<Token, ProcessGroup*> ProcessGroupMap;
            typedef std::map<Token, std::stack<Token> > ProcessFuncStack;

            OTF2TraceReader(void *userData, uint32_t mpiRank);
            ~OTF2TraceReader();

            uint32_t getMPIRank();
            uint64_t getMPIProcessId();
            void setMPIProcessId(uint64_t processId);

            TokenTokenMap &getProcessRankMap();
            TokenTokenMap &getProcessFamilyMap();

            void open(const std::string otfFilename, uint32_t maxFiles);
            void close();
            void readEvents();
            void readEventsForProcess(uint64_t id);
            void readDefinitions();

            TokenNameMap& getDefinitionTokenStringMap();
            ProcessGroupMap& getProcGoupMap();
            ProcessFuncStack& getFuncStack();

            std::string getStringRef(Token t);
            std::string getKeyName(uint32_t id);
            std::string getFunctionName(uint64_t id);
            std::string getProcessName(uint64_t id);
            IdNameTokenMap& getProcessNameTokenMap();
            IdNameTokenMap& getFunctionNameTokenMap();
            std::vector<uint32_t> getKeys(const std::string keyName);
            uint32_t getFirstKey(const std::string keyName);
            uint64_t getTimerResolution();
            void setTimerResolution(uint64_t ticksPerSecond);
            bool isChildOf(uint64_t child, uint64_t parent);
            int getProcessingPhase();

        private:
            static OTF2_CallbackCode otf2CallbackEnter(OTF2_LocationRef location, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributes,
                    OTF2_RegionRef region);
            static OTF2_CallbackCode otf2CallbackLeave(OTF2_LocationRef location, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributes,
                    OTF2_RegionRef region);
            static OTF2_CallbackCode otf2Callback_MpiCollectiveBegin(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList);
            static OTF2_CallbackCode otf2Callback_MpiCollectiveEnd(OTF2_LocationRef locationID,
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList,
                    OTF2_CollectiveOp collectiveOp, OTF2_CommRef communicator, uint32_t root,
                    uint64_t sizeSent, uint64_t sizeReceived);
            static OTF2_CallbackCode otf2Callback_MpiRecv(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
                    uint32_t sender, OTF2_CommRef communicator, uint32_t msgTag, 
                    uint64_t msgLength);
            static OTF2_CallbackCode otf2Callback_MpiSend(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
                    uint32_t receiver, OTF2_CommRef communicator, uint32_t msgTag, 
                    uint64_t msgLength);
            static OTF2_CallbackCode otf2Callback_OmpFork(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList, 
                    uint32_t numberOfRequestedThreads);
            static OTF2_CallbackCode otf2Callback_OmpJoin(OTF2_LocationRef locationID, 
                    OTF2_TimeStamp time, void *userData, OTF2_AttributeList *attributeList);


            // Definition callbacks
            static OTF2_CallbackCode GlobDefLocation_Register(void* userData, 
                    OTF2_LocationRef location, OTF2_StringRef name, 
                    OTF2_LocationType locationType, uint64_t numberOfEvents,
                    OTF2_LocationGroupRef locationGroup);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_ClockProperties(void *userData, 
                    uint64_t timerResolution, uint64_t globalOffset, uint64_t traceLength);
            static OTF2_CallbackCode  OTF2_GlobalDefReaderCallback_LocationGroup(void *userData,
                    OTF2_LocationGroupRef self, OTF2_StringRef name, OTF2_LocationGroupType locationGroupType, 
                    OTF2_SystemTreeNodeRef systemTreeParent);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_Location(void *userData, OTF2_LocationRef self, 
                    OTF2_StringRef name, OTF2_LocationType locationType, uint64_t numberOfEvents, 
                    OTF2_LocationGroupRef locationGroup);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_Group(void *userData,
                    OTF2_GroupRef self, OTF2_StringRef name, OTF2_GroupType groupType, 
                    OTF2_Paradigm paradigm, OTF2_GroupFlag groupFlags, uint32_t numberOfMembers, 
                    const uint64_t *members);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_Comm(void *userData, OTF2_CommRef self, 
                    OTF2_StringRef name, OTF2_GroupRef group, OTF2_CommRef parent);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_String(void *userData, 
                    OTF2_StringRef self, const char *string);
            static OTF2_CallbackCode OTF2_GlobalDefReaderCallback_Region(void *userData,
                    OTF2_RegionRef self, OTF2_StringRef name, OTF2_StringRef cannonicalName,
                    OTF2_StringRef description, OTF2_RegionRole regionRole, OTF2_Paradigm paradigm,
                    OTF2_RegionFlag regionFlags, OTF2_StringRef sourceFile, uint32_t beginLineNumber,
                    uint32_t endLineNumber);

            

            void setEventCallbacks(OTF2_GlobalEvtReaderCallbacks* evtReaderCallbacks);

            uint32_t mpiRank;
            uint64_t mpiProcessId;
            TokenTokenMap processRankMap; // maps (parent) process ID to MPI rank
            TokenTokenMap processFamilyMap; // tracks for each process its direct parent

            OTF2_Reader *reader;
            OTF2_GlobalDefReader* gobal_def_reader;

            std::string baseFilename;
            NameTokenMap nameKeysMap;
            IdNameTokenMap processNameTokenMap;
            IdNameTokenMap functionNameTokenMap;
            TokenNameMap definitionTokenStringMap;
            ProcessGroupMap processGroupMap;
            ProcessFuncStack funcStack;
            uint64_t ticksPerSecond;
            
            int processingPhase;
        };
    }
}


#endif	/* OTF2TRACEREADER_HPP */

