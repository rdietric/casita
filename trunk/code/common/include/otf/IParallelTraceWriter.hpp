/* 
 * File:   IParallelTraceWriter.hpp
 * Author: felix
 *
 * Created on May 14, 2013, 3:45 PM
 */

#ifndef IPARALLELTRACEWRITER_HPP
#define	IPARALLELTRACEWRITER_HPP

#include <string>
#include <stdint.h>
#include "graph/Node.hpp"
#include "CounterTable.hpp"
#include "ITraceWriter.hpp"

namespace cdm
{
    namespace io
    {

        class IParallelTraceWriter : public ITraceWriter
        {
        public:

            IParallelTraceWriter(const char *streamRefKeyName,
                    const char *eventRefKeyName,
                    const char *funcResultKeyName,
                    uint32_t mpiRank,
                    uint32_t mpiSize) :
            ITraceWriter(streamRefKeyName, eventRefKeyName, funcResultKeyName),
            mpiRank(mpiRank),
            mpiSize(mpiSize)
            {

            }

            virtual ~IParallelTraceWriter()
            {
            }

            virtual void writeRMANode(const Node *node, uint64_t prevRank,
                    uint64_t nextRank) = 0;
            
            virtual void *getWriteObject(uint32_t id)
            {
                return NULL;
            }

        protected:
            uint32_t mpiRank, mpiSize;

        private:

            void writeDefFunction(uint64_t id, const char *name, FunctionGroup fg)
            {
            };
        };
    }
}

#endif	/* IPARALLELTRACEWRITER_HPP */

