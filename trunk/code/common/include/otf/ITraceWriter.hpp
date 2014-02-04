/* 
 * File:   ITraceWriter.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 3:45 PM
 */

#ifndef ITRACEWRITER_HPP
#define	ITRACEWRITER_HPP

#include <string>
#include <stdint.h>
#include "graph/Node.hpp"
#include "CounterTable.hpp"

namespace cdm
{
    namespace io
    {

        class ITraceWriter
        {
        public:

            enum FunctionGroup
            {
                FG_APPLICATION = 1, FG_CUDA_API, FG_KERNEL, FG_WAITSTATE, FG_MPI
            };

            enum ProcessGroup
            {
                PG_HOST = 1, PG_DEVICE, PG_DEVICE_NULL
            };

            enum MarkerGroup
            {
                MG_Marker = 1
            };

            ITraceWriter(const char *streamRefKeyName, const char *eventRefKeyName,
                    const char *funcResultKeyName) :
            streamRefKeyName(streamRefKeyName),
            eventRefKeyName(eventRefKeyName),
            funcResultKeyName(funcResultKeyName)
            {

            }

            virtual ~ITraceWriter()
            {
            }

            virtual void open(const std::string otfFilename, uint32_t maxFiles,
                    uint32_t numStreams, uint64_t timerResolution) = 0;
            virtual void close() = 0;

            virtual void writeDefProcess(uint32_t id, uint32_t parentId, const char *name, ProcessGroup pg) = 0;
            virtual void writeDefFunction(uint32_t id, const char *name, FunctionGroup fg) = 0;
            virtual void writeDefCounter(uint32_t id, const char *name, int properties) = 0;

            virtual void writeNode(const Node *node, CounterTable &ctrTable, bool lastProcessNode, const Node *futureNode) = 0;

        protected:
            const char *streamRefKeyName;
            const char *eventRefKeyName;
            const char *funcResultKeyName;
        };
    }
}

#endif	/* ITRACEWRITER_HPP */

