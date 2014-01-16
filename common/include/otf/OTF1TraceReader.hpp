/* 
 * File:   OTF1TraceReader.hpp
 * Author: felix
 *
 * Created on May 7, 2013, 1:04 PM
 */

#ifndef OTF1TRACEREADER_HPP
#define	OTF1TRACEREADER_HPP

#include <otf.h>
#include <map>
#include <vector>
#include <stack>
#include <set>
#include "ITraceReader.hpp"
#include "OTF1KeyValueList.hpp"

namespace cdm
{
    namespace io
    {

        class OTF1TraceReader : public ITraceReader
        {
        public:

            typedef struct
            {
                char *name;
                bool isCUDA;
                bool isCUDAMaster;
                uint32_t numProcs;
                uint32_t *procs;
            } ProcessGroup;

            typedef uint32_t Token;
            typedef std::multimap<std::string, Token> NameTokenMap;
            typedef std::map<Token, std::string> TokenNameMap;
            typedef std::map<Token, Token> TokenTokenMap;
            typedef std::set<Token> TokenSet;
            typedef std::map<Token, TokenSet> TokenSetMap;

            typedef std::map<Token, ProcessGroup*> ProcessGroupMap;
            typedef std::map<Token, std::vector<OTF_ATTR_TYPE> > AttrListMap;
            typedef std::map<Token, std::stack<Token> > ProcessFuncStack;


            OTF1TraceReader(void *userData, uint32_t mpiRank);
            ~OTF1TraceReader();

            uint32_t getMPIRank();
            uint32_t getMPIProcessId();
            void setMPIProcessId(uint32_t processId);

            TokenTokenMap &getProcessRankMap();
            TokenTokenMap &getProcessFamilyMap();

            void open(const std::string otfFilename, uint32_t maxFiles);
            void close();
            void readEvents();
            void readDefinitions();

            OTF1KeyValueList& getKVList();

            NameTokenMap& getNameKeysMap();
            TokenNameMap& getKeyNameMap();
            TokenNameMap& getFuncNameMap();
            TokenNameMap& getProcNameMap();
            ProcessGroupMap& getProcGoupMap();
            AttrListMap& getAttrListMap();
            ProcessFuncStack& getFuncStack();

            std::string getKeyName(uint32_t id);
            std::string getFunctionName(uint32_t id);
            std::string getProcessName(uint32_t id);
            std::vector<uint32_t> getKeys(const std::string keyName);
            uint32_t getFirstKey(const std::string keyName);
            uint64_t getTimerResolution();
            void setTimerResolution(uint64_t ticksPerSecond);
            bool isChildOf(uint32_t child, uint32_t parent);
            int getProcessingPhase();

        private:
            static int otf1HandleDefProcessGroupMPI(void *userData, uint32_t stream, uint32_t procGroup,
                    const char *name, uint32_t numberOfProcs, const uint32_t *procs, OTF_KeyValueList *list);

            static int otf1HandleEnter(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList *list);
            static int otf1HandleLeave(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList * list);
            static int otf1HandleDefProcess(void * userData, uint32_t stream, uint32_t processId,
                    const char * name, uint32_t parent, OTF_KeyValueList * list);
            static int otf1HandleDefProcessGroup(void *userData, uint32_t stream, uint32_t procGroup,
                    const char *name, uint32_t numberOfProcs, const uint32_t *procs, OTF_KeyValueList *list);
            static int otf1HandleDefProcessOrGroupAttributes(void *userData, uint32_t stream,
                    uint32_t proc_token, uint32_t attr_token, OTF_KeyValueList *list);
            static int otf1HandleDefAttributeList(void *userData, uint32_t stream, uint32_t attr_token,
                    uint32_t num, OTF_ATTR_TYPE *array, OTF_KeyValueList *list);
            static int otf1HandleDefFunction(void *userData, uint32_t stream, uint32_t func,
                    const char *name, uint32_t funcGroup, uint32_t source);
            static int otf1HandleDefKeyValue(void* userData, uint32_t stream, uint32_t key,
                    OTF_Type type, const char *name, const char *description, OTF_KeyValueList *list);
            static int otf1HandleDefTimerResolution(void *userData, uint32_t stream,
                    uint64_t ticksPerSecond, OTF_KeyValueList *list);
            static int otf1HandleSendMsg(void *userData, uint64_t time, uint32_t sender,
                    uint32_t receiver, uint32_t group, uint32_t type, uint32_t length,
                    uint32_t source, OTF_KeyValueList *list);
            static int otf1HandleRecvMsg(void *userData, uint64_t time, uint32_t receiver,
                    uint32_t sender, uint32_t group, uint32_t type, uint32_t length,
                    uint32_t source, OTF_KeyValueList *list);
            static int otf1HandleBeginCollectiveOperation(void * userData, uint64_t time,
                    uint32_t process, uint32_t collOp, uint64_t matchingId, uint32_t procGroup,
                    uint32_t rootProc, uint64_t sent, uint64_t received, uint32_t scltoken,
                    OTF_KeyValueList * list);

            uint32_t mpiRank;
            uint32_t mpiProcessId;
            TokenTokenMap processRankMap; // maps (parent) process ID to MPI rank
            TokenTokenMap processFamilyMap; // tracks for each process its direct parent

            OTF_FileManager *fileMgr;
            OTF_Reader *reader;
            OTF1KeyValueList kvList;

            std::string baseFilename;
            NameTokenMap nameKeysMap;
            TokenNameMap kNameMap;
            TokenNameMap fNameMap;
            TokenNameMap pNameMap;
            ProcessGroupMap processGroupMap;
            AttrListMap attrListMap;
            ProcessFuncStack funcStack;
            uint64_t ticksPerSecond;
            
            int processingPhase;
        };
    }
}

#endif	/* OTF1TRACEREADER_HPP */

