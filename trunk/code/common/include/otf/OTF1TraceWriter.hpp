/* 
 * File:   OTF1TraceWriter.hpp
 * Author: felix
 *
 * Created on May 15, 2013, 7:28 PM
 */

#ifndef OTF1TRACEWRITER_HPP
#define	OTF1TRACEWRITER_HPP

#include "open-trace-format/otf.h"
#include <vector>
#include <map>
#include "ITraceWriter.hpp"

namespace cdm
{
    namespace io
    {

        class OTF1TraceWriter : public ITraceWriter
        {
        public:
            OTF1TraceWriter(const char *streamRefKeyName, const char *eventRefKeyName,
                    const char *funcResultKeyName);
            virtual ~OTF1TraceWriter();

            void open(const std::string otfFilename, uint32_t maxFiles,
                    uint32_t numStreams, uint64_t timerResolution);
            void close();

            void writeDefProcess(uint64_t id, uint64_t parentId, const char *name, ProcessGroup pg);
            void writeDefFunction(uint64_t id, const char *name, FunctionGroup fg);
            void writeDefCounter(uint32_t id, const char *name, int properties);

            void writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode, const Node *futureNode);

        private:
            uint32_t numStreams;
            OTF_FileManager *fileMgr;
            OTF_Writer *writer;
            std::map<uint32_t, uint32_t> processStreamMap;
            OTF_KeyValueList *kvList;

            std::vector<uint32_t> deviceProcesses;
            std::vector<uint32_t> deviceMasterProcesses;

            uint32_t streamRefKey, eventRefKey, funcResultKey;
            uint32_t attrListCUDAToken;
            uint32_t attrListCUDAMasterToken;
        };
    }
}

#endif	/* OTF1TRACEWRITER_HPP */

