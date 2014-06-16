/* 
 * File:   OTF2ParallelTraceWriter.hpp
 * Author: stolle
 *
 * Created on March 17, 2014, 11:53 AM
 */

#ifndef OTF2PARALLELTRACEWRITER_HPP
#define	OTF2PARALLELTRACEWRITER_HPP

#include <otf2/otf2.h>
#include <vector>
#include <map>
#include <stack>
#include "CounterTable.hpp"
#include "otf/IParallelTraceWriter.hpp"
#include "OTF2TraceReader.hpp"

namespace cdm
{
    namespace io
    {

        typedef std::map<uint32_t,uint32_t> CtrInstanceMap;
        
        class OTF2ParallelTraceWriter : public IParallelTraceWriter
        {
        public:
            OTF2ParallelTraceWriter(const char *streamRefKeyName,
                    const char *eventRefKeyName,
                    const char *funcResultKeyName,
                    uint32_t mpiRank,
                    uint32_t mpiSize,
                    MPI_Comm comm,
                    const char *originalFilename,
                    std::set<uint32_t> ctrIdSet);
            virtual ~OTF2ParallelTraceWriter();

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
            uint64_t timerOffset;
            int *mpiNumProcesses;
            uint64_t counterForStringDefinitions;           // counter to assign ids to stringdefinitions
            uint64_t counterForMetricInstanceId;            // counter to assign ids to MetricInstances

            std::string outputFilename, originalFilename, pathToFile;

            OTF2_Archive *archive;
            std::map<uint64_t,OTF2_EvtWriter*> evt_writerMap;   // maps each process to corresponding evtWriter
            OTF2_GlobalDefWriter* global_def_writer;
            OTF2TraceReader *tr;                                // TraceReader to re-read definitions
            
            std::vector<uint32_t> deviceProcesses;
            std::vector<uint32_t> deviceMasterProcesses;
            std::map<uint32_t, std::list<uint64_t> > processTimeEndMap;
            std::stack<uint64_t> cpTimeCtrStack;
            std::map<uint32_t,uint32_t> ctrStrIdMap; // maps a counter to its corresponding String-Id
            uint32_t streamRefKey, eventRefKey, funcResultKey;
            uint32_t attrListCUDAToken;
            uint32_t attrListCUDAMasterToken;
            
            MPI_Comm commGroup;
            
            std::set<uint32_t> ctrIdSet;    // set of all counterIds
            std::map<uint32_t,uint32_t> otf2CounterMapping; // map counters to start with 0... 1=>0, 2=>1,... (in otf it starts with 1, otf2 with 0...)
            
            void copyGlobalDefinitions();
            void copyMasterControl();
            
            OTF2_FlushCallbacks flush_callbacks;            // tell OTF2 what to do after bufferFlush
            OTF2_CollectiveCallbacks coll_callbacks;        // callbacks to support parallel writing
        };

    }

}


#endif	/* OTF2PARALLELTRACEWRITER_HPP */

