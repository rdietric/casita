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
                    uint32_t numStreams, uint64_t timerResolution);
            void close();

            void writeDefProcess(uint64_t id, uint64_t parentId,
                    const char *name, ProcessGroup pg);
            void writeDefCounter(uint32_t id, const char *name, int properties);
            void writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode, const Node *futureNode);
            void writeRMANode(const Node *node, uint64_t prevProcessId,
                    uint64_t nextProcessId);
            
            void writeRemainingCommEvents();
            
            void *getWriteObject(uint64_t id);
        private:
            uint32_t totalNumStreams;
            uint64_t timerResolution;
            int *mpiNumProcesses;

            typedef OTF_WStream* OTF_WStream_ptr;

            std::string outputFilename, originalFilename;

            OTF_FileManager* fileMgr;
            OTF_KeyValueList *kvList;
            OTF_Writer *globalWriter;
            
            OTF1TraceReader *tr;

            std::map<uint32_t, OTF_WStream_ptr> processWStreamMap;
            std::vector<uint32_t> deviceProcesses;
            std::vector<uint32_t> deviceMasterProcesses;
            std::map<uint32_t, std::list<uint64_t> > processTimeEndMap;
            std::stack<uint64_t> cpTimeCtrStack;

            uint32_t streamRefKey, eventRefKey, funcResultKey;
            uint32_t attrListCUDAToken;
            uint32_t attrListCUDAMasterToken;
            
            void copyGlobalDefinitions(OTF_Writer *writer);
            void copyMasterControl();
        };

    }

}

#endif	/* OTF1PARALLELTRACEWRITER_HPP */

