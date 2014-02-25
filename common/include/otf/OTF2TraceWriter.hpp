/* 
 * File:   OTF2TraceWriter.hpp
 * Author: felix
 *
 * Created on May 15, 2013, 7:28 PM
 */

#ifndef OTF2TRACEWRITER_HPP
#define	OTF2TRACEWRITER_HPP

#if defined OTF2_ENABLED

#include <otf2/otf2.h>
#include <vector>
#include "ITraceWriter.hpp"
#include "IKeyValueList.hpp"

namespace cdm
{
    namespace io
    {

        class OTF2TraceWriter : public ITraceWriter
        {
        public:
            OTF2TraceWriter(const char *streamRefKeyName, const char *eventIdKeyName);
            virtual ~OTF2TraceWriter();

            void open(const std::string otfFilename, uint32_t maxFiles);
            void close();

            void writeDefProcess(uint64_t id, const char *name, ProcessGroup pg);
            void writeDefFunction(uint64_t id, const char *name, FunctionGroup fg);

            void writeNode(const Node *node);

        private:
            uint32_t globalStringId;

            OTF2_Archive* archive;
            OTF2_EvtWriter *eventWriter;
            OTF2_DefWriter *defWriter;
            OTF2_GlobalDefWriter *globDefWriter;

            std::vector<uint64_t> hostProcesses;
            std::vector<uint64_t> deviceProcesses;

            uint32_t writeString(const char *str);
        };
    }
}

#endif /* OTF2_ENABLED */

#endif	/* OTF2TRACEWRITER_HPP */

