/* 
 * File:   OTF1ParallelTraceWriter.hpp
 * Author: felix
 *
 * Created on 31. Juli 2013, 12:33
 */

#ifndef OTF1PARALLELTRACEWRITER_HPP
#define	OTF1PARALLELTRACEWRITER_HPP

#include <otf.h>
#include <vector>
#include <map>
#include "otf/IParallelTraceWriter.hpp"

namespace cdm
{
    namespace io
    {

        typedef struct
        {
            uint32_t maxFunctionID;
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

            void writeDefProcess(uint32_t id, uint32_t parentId,
                    const char *name, ProcessGroup pg);
            void writeDefCounter(uint32_t id, const char *name, int properties);
            void writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode);
            void writeRMANode(const Node *node, uint32_t prevProcessId,
                    uint32_t nextProcessId);
            
            void *getWriteObject(uint32_t id);
        private:
            uint32_t totalNumStreams;
            int *mpiNumProcesses;

            typedef OTF_WStream* OTF_WStream_ptr;

            std::string outputFilename, originalFilename;

            OTF_FileManager* fileMgr;
            OTF_KeyValueList *kvList;
            OTF_Writer *globalWriter;

            std::map<uint32_t, OTF_WStream_ptr> processWStreamMap;
            std::map<uint32_t, uint64_t> lastNodeMap;
            std::vector<uint32_t> deviceProcesses;
            std::vector<uint32_t> deviceMasterProcesses;

            uint32_t streamRefKey, eventRefKey, funcResultKey;
            uint32_t attrListCUDAToken;
            uint32_t attrListCUDAMasterToken;

            void copyGlobalDefinitions(OTF_Writer *writer);
            void copyMasterControl();
        };

    }

}

#endif	/* OTF1PARALLELTRACEWRITER_HPP */

