/* 
 * File:   OTF1ParallelTraceWriter.hpp
 * Author: felix
 *
 * Created on 31. Juli 2013, 12:33
 */

#ifndef OTF1PARALLELTRACEWRITER_HPP
#define	OTF1PARALLELTRACEWRITER_HPP

#include <open-trace-format/otf.h>
#include <vector>
#include <map>
#include <stack>
#include "otf/IParallelTraceWriter.hpp"
#include "OTF1TraceReader.hpp"

namespace cdm
{
    namespace io
    {

        typedef struct
        {
            uint32_t maxFunctionID;
            uint64_t timerResolution;
            OTF_Writer *writer;
        } WriterData;

        class OTF1ParallelTraceWriter : public IParallelTraceWriter
        {
        public:
            OTF1ParallelTraceWriter(const char *streamRefKeyName,
                    const char *eventRefKeyName,
                    const char *funcResultKeyName,
                    uint32_t mpiRank,
                    uint32_t mpiSize,
                    const char *originalFilename);
            virtual ~OTF1ParallelTraceWriter();

            void open(const std::string otfFilename, uint32_t maxFiles,
                    uint32_t numStreams);
            void close();

            void writeDefProcess(uint64_t id, uint64_t parentId,
                    const char *name, ProcessGroup pg);
            void writeDefCounter(uint32_t id, const char *name, int properties);
            void writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode, const Node *futureNode);
            
            void writeProcess(uint64_t processId, Process::SortedNodeList *nodes,
                        bool enableWaitStates, GraphNode *pLastGraphNode, bool verbose,
                        CounterTable* ctrTable, Graph *graph);
            
        private:
            uint32_t totalNumStreams;
            uint64_t timerResolution;
            int *mpiNumProcesses;

            typedef OTF_WStream* OTF_WStream_ptr;

            std::string outputFilename, originalFilename;

            OTF_FileManager* fileMgr;
            OTF_KeyValueList *kvList;
            OTF_Writer *globalWriter;
            OTF_Reader *reader;
            
            std::map<uint32_t, OTF_WStream_ptr> processWStreamMap;
            std::stack<uint64_t> cpTimeCtrStack;

            uint32_t streamRefKey, eventRefKey, funcResultKey;
            
            void copyGlobalDefinitions(OTF_Writer *writer);
            void copyMasterControl();
            
            bool processNextNode(uint64_t time, uint32_t funcId);
            
            Process::SortedNodeList *processNodes;
            bool enableWaitStates;
            Process::SortedNodeList::iterator iter;
            GraphNode *lastGraphNode;
            CounterTable* cTable;
            Graph* graph;
            bool verbose;
            
            static int otf1HandleDefProcess(void * userData, uint32_t stream, uint32_t processId,
                    const char * name, uint32_t parent, OTF_KeyValueList * list);
            static int otf1HandleDefProcessGroup(void *userData, uint32_t stream, uint32_t procGroup,
                    const char *name, uint32_t numberOfProcs, const uint32_t *procs, OTF_KeyValueList *list);
            static int otf1HandleDefProcessOrGroupAttributes(void *userData, uint32_t stream,
                    uint32_t proc_token, uint32_t attr_token, OTF_KeyValueList *list);
            static int otf1HandleDefAttributeList(void *userData, uint32_t stream, uint32_t attr_token,
                    uint32_t num, OTF_ATTR_TYPE *array, OTF_KeyValueList *list);
            static int otf1HandleDefFunction(void *userData, uint32_t stream, uint32_t func,
                    const char *name, uint32_t funcGroup, uint32_t source, OTF_KeyValueList *list);
            static int otf1HandleDefFunctionGroup(void * userData, uint32_t stream,
                    uint32_t funcGroup, const char * name, OTF_KeyValueList * list);
            static int otf1HandleDefKeyValue(void* userData, uint32_t stream, uint32_t key,
                    OTF_Type type, const char *name, const char *description, OTF_KeyValueList *list);
            static int otf1HandleDefTimerResolution(void *userData, uint32_t stream,
                    uint64_t ticksPerSecond, OTF_KeyValueList *list);

            static int otf1HandleEnter(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList *list);
            static int otf1HandleLeave(void *userData, uint64_t time, uint32_t functionId,
                    uint32_t processId, uint32_t source, OTF_KeyValueList * list);
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
            static int otf1HandleEndCollectiveOperation	(void *	userData, uint64_t time,
                        uint32_t process, uint64_t matchingId, OTF_KeyValueList * list);
            static int otf1HandleRMAEnd	(void * userData, uint64_t time, uint32_t process,
                        uint32_t remote, uint32_t communicator, uint32_t tag, uint32_t source,
                        OTF_KeyValueList * list);
            static int otf1HandleRMAGet	(void * userData, uint64_t time, uint32_t process,
                        uint32_t origin, uint32_t target, uint32_t communicator,
                        uint32_t tag, uint64_t bytes, uint32_t source, OTF_KeyValueList * list);
            static int otf1HandleRMAPut	(void * userData, uint64_t time, uint32_t process,
                        uint32_t origin, uint32_t target, uint32_t communicator, uint32_t tag,
                        uint64_t bytes, uint32_t source, OTF_KeyValueList * list);
            
        };

    }

}

#endif	/* OTF1PARALLELTRACEWRITER_HPP */

