/* 
 * File:   CDMRunner.cpp
 * Author: felix
 * 
 * Created on 13. August 2013, 10:40
 */

#include <sys/types.h>
#include <sys/time.h>

#include <omp.h>

#include "CDMRunner.hpp"

#include "otf/IKeyValueList.hpp"
#include "otf/OTF1TraceReader.hpp"
#include "otf/OTF2TraceReader.hpp"

#include "cuda/BlameKernelRule.hpp"
#include "cuda/BlameSyncRule.hpp"
#include "cuda/LateSyncRule.hpp"
#include "cuda/KernelLaunchRule.hpp"
#include "cuda/EventLaunchRule.hpp"
#include "cuda/EventSyncRule.hpp"
#include "cuda/EventQueryRule.hpp"
#include "cuda/StreamWaitRule.hpp"

#include "mpi/RecvRule.hpp"
#include "mpi/SendRule.hpp"
#include "mpi/CollectiveRule.hpp"
#include "mpi/SendRecvRule.hpp"
#include "mpi/OneToAllRule.hpp"
#include "mpi/AllToOneRule.hpp"

#include "omp/OMPParallelRegionRule.hpp"
#include "omp/OMPComputeRule.hpp"
#include "omp/OMPBarrierRule.hpp"

using namespace cdm;
using namespace cdm::io;
using namespace cdm::mpi;
using namespace cdm::omp;

/**
 * http://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
 */

static int parseLine(char* line)
{
    int i = strlen(line);
    while (*line < '0' || *line > '9') line++;
    line[i - 3] = '\0';
    i = atoi(line);
    return i;
}

int CDMRunner::getCurrentResources()
{
    //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL)
    {
        if (strncmp(line, "VmRSS:", 6) == 0)
        {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

static void dumpAllocationTail(AnalysisEngine &analysis)
{
    Allocation::ProcessList allProcs;
    analysis.getProcesses(allProcs);

    for (Allocation::ProcessList::const_iterator iter = allProcs.begin();
            iter != allProcs.end(); ++iter)
    {
        Process *p = *iter;
        printf("# %s (%lu)\n", p->getName(), p->getId());
        Process::SortedNodeList &nodes = p->getNodes();
        int n = 0;
        for (Process::SortedNodeList::const_reverse_iterator rIter = nodes.rbegin();
                rIter != nodes.rend(); ++rIter)
        {
            if (n > 10)
                break;

            printf("  %s\n", (*rIter)->getUniqueName().c_str());
            n++;
        }
    }
}

CDMRunner::CDMRunner(int mpiRank, int mpiSize) :
mpiRank(mpiRank),
mpiSize(mpiSize),
analysis(mpiRank, mpiSize),
options(Parser::getInstance().getProgramOptions())
{
    if (options.noErrors)
        ErrorUtils::getInstance().setNoExceptions();

    if (options.verbose)
        ErrorUtils::getInstance().setVerbose();
}

CDMRunner::~CDMRunner()
{
}

ProgramOptions& CDMRunner::getOptions()
{
    return options;
}

AnalysisEngine& CDMRunner::getAnalysis()
{
    return analysis;
}

void CDMRunner::printNode(GraphNode *node, Process *process)
{
    if ((options.verbose >= VERBOSE_ALL) || ((options.verbose >= VERBOSE_BASIC) &&
            (!node->isEventNode() ||
            (((EventNode*) node)->getFunctionResult() == EventNode::FR_SUCCESS))))
    {
        printf(" [%u]", mpiRank);
        if (node->isEnter())
            printf(" E ");
        else
            printf(" L ");

        printf("[%12lu:%12.8fs:%10u,%5lu] [%20.20s] proc [%15s], pid [%10lu], [%s]",
                node->getTime(),
                (double) (node->getTime()) / (double) analysis.getTimerResolution(),
                node->getId(),
                node->getFunctionId(),
                node->getName(),
                process->getName(),
                process->getId(),
                Node::typeToStr(node->getParadigm(), node->getType()).c_str());

        uint64_t refProcess = node->getReferencedProcessId();
        if (refProcess)
            printf(", ref = %lu", refProcess);

        if (node->isLeave() && node->isEventNode())
        {
            printf(", event = %u, result = %u",
                    ((EventNode*) node)->getEventId(),
                    ((EventNode*) node)->getFunctionResult());
        }

        printf("\n");
    }
}

void CDMRunner::applyStreamRefsEnter(ITraceReader *reader, Node *node, IKeyValueList *list)
{
    uint32_t refValue = 0;
    uint32_t streamRefKey = reader->getFirstKey(VT_CUPTI_CUDA_STREAMREF_KEY);
    if (streamRefKey != 0 && list && list->getUInt32(streamRefKey, &refValue) == 0)
    {
        node->setReferencedProcessId(refValue);
    }
}

void CDMRunner::applyStreamRefsLeave(ITraceReader *reader, Node *node, Node *oldNode, IKeyValueList *list)
{
    uint32_t refValue = 0;
    uint32_t streamRefKey = reader->getFirstKey(VT_CUPTI_CUDA_STREAMREF_KEY);
    if (streamRefKey != 0 && list && list->getUInt32(streamRefKey, &refValue) == 0)
    {
        node->setReferencedProcessId(refValue);
        oldNode->setReferencedProcessId(refValue);
    }
}

uint32_t CDMRunner::readKeyVal(ITraceReader *reader, const char * keyName, IKeyValueList *list)
{
    uint32_t keyVal = 0;
    uint32_t key = reader->getFirstKey(keyName);
    if (key != 0 && list)
    {
        list->getUInt32(key, &keyVal);
    }

    return keyVal;
}

void CDMRunner::handleProcessMPIMapping(ITraceReader *reader, uint64_t processId, uint32_t mpiRank)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    runner->getAnalysis().getMPIAnalysis().setMPIRank(processId, mpiRank);
}

void CDMRunner::handleDefProcess(ITraceReader *reader, uint32_t stream, uint64_t processId,
        uint64_t parentId, const char *name, IKeyValueList * list, bool isCUDA, bool isCUDANull)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();

    Process::ProcessType processType = Process::PT_HOST;

    if (isCUDANull)
        processType = Process::PT_DEVICE_NULL;
    else
    {
        if (isCUDA)
            processType = Process::PT_DEVICE;
    }

    if (runner->getOptions().verbose >= VERBOSE_BASIC)
        printf("  [%u] Found process %s (%lu) with type %u, stream %u\n",
            analysis.getMPIRank(), name, processId, processType, stream);

    analysis.newProcess(processId, parentId, name, processType, PARADIGM_CUDA);
}

void CDMRunner::handleDefFunction(ITraceReader *reader, uint32_t streamId,
        uint64_t functionId, const char *name, uint32_t functionGroupId)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    runner->getAnalysis().addFunction(functionId, name);
}

void CDMRunner::handleEnter(ITraceReader *reader, uint64_t time, uint64_t functionId,
        uint64_t processId, IKeyValueList *list)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();
    ProgramOptions &options = runner->getOptions();

    Process *process = analysis.getProcess(processId);
    if (!process)
        throw RTException("Process %lu not found.", processId);

    const char *funcName = reader->getFunctionName(functionId).c_str();

    FunctionDescriptor functionType;
    TraceData::getFunctionType(functionId, funcName, process, &functionType);

    if (functionType.paradigm == PARADIGM_VT)
        return;

    GraphNode *enterNode = NULL;
    if (Node::isCUDAEventType(functionType.paradigm, functionType.type))
    {
        enterNode = analysis.addNewEventNode(time, 0, EventNode::FR_UNKNOWN, process,
                funcName, functionType.paradigm, RECORD_ENTER, functionType.type);
    } else
    {
        enterNode = analysis.addNewGraphNode(time, process, funcName,
                functionType.paradigm, RECORD_ENTER, functionType.type);
    }

    enterNode->setFunctionId(functionId);

    if (enterNode->isCUDAKernelLaunch())
    {
        CDMRunner::applyStreamRefsEnter(reader, enterNode, list);
        analysis.addPendingKernelLaunch(enterNode);
    }

    runner->printNode(enterNode, process);
    options.eventsProcessed++;

    if (options.eventsProcessed % 1000 == 0)
    {
        int memUsage = getCurrentResources();
        if (memUsage > options.memLimit)
        {
            throw RTException("Memory limit exceeded (%d KByte / %d KByte)",
                    memUsage, options.memLimit);
        }
    }
}

void CDMRunner::handleLeave(ITraceReader *reader, uint64_t time,
        uint64_t functionId, uint64_t processId, IKeyValueList *list)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();
    ProgramOptions &options = runner->getOptions();

    Process *process = runner->getAnalysis().getProcess(processId);
    if (!process)
        throw RTException("Process %lu not found", processId);

    const char *funcName = reader->getFunctionName(functionId).c_str();

    FunctionDescriptor functionType;
    TraceData::getFunctionType(functionId, funcName, process, &functionType);

    if (functionType.paradigm == PARADIGM_VT)
        return;

    GraphNode *leaveNode = NULL;
    if (Node::isCUDAEventType(functionType.paradigm, functionType.type))
    {
        uint32_t eventId = readKeyVal(reader, VT_CUPTI_CUDA_EVENTREF_KEY, list);
        CUresult cuResult = (CUresult) readKeyVal(reader, VT_CUPTI_CUDA_CURESULT_KEY, list);
        EventNode::FunctionResultType fResult = EventNode::FR_UNKNOWN;
        if (cuResult == CUDA_SUCCESS)
            fResult = EventNode::FR_SUCCESS;

        leaveNode = runner->getAnalysis().addNewEventNode(time, eventId, fResult, process,
                funcName, functionType.paradigm, RECORD_LEAVE, functionType.type);

        if (eventId == 0)
            throw RTException("No eventId for event node %s found",
                leaveNode->getUniqueName().c_str());
    } else
    {
        leaveNode = analysis.addNewGraphNode(time, process, funcName,
                functionType.paradigm, RECORD_LEAVE, functionType.type);
    }

    leaveNode->setFunctionId(functionId);

    CDMRunner::applyStreamRefsLeave(reader, leaveNode, leaveNode->getGraphPair().first, list);
    if (leaveNode->isMPI())
    {
        Process::MPICommRecordList mpiCommRecords = process->getPendingMPIRecords();
        for (Process::MPICommRecordList::const_iterator iter = mpiCommRecords.begin();
                iter != mpiCommRecords.end(); ++iter)
        {
            uint64_t *tmpId = NULL;

            switch (iter->mpiType)
            {
                case Process::MPI_RECV:
                    leaveNode->setReferencedProcessId(iter->partnerId);
                    break;

                case Process::MPI_COLLECTIVE:
                    leaveNode->setReferencedProcessId(iter->partnerId);
                    if (iter->rootId)
                    {
                        tmpId = new uint64_t;
                        *tmpId = iter->rootId;
                        leaveNode->setData(tmpId);
                    }
                    break;

                case Process::MPI_SEND:
                    tmpId = new uint64_t;
                    *tmpId = iter->partnerId;
                    leaveNode->setData(tmpId);
                    break;

                default:
                    throw RTException("Not a valid MPICommRecord type here");
            }
        }
    }

    if (leaveNode->isCUDAKernelLaunch())
    {
        analysis.addPendingKernelLaunch(leaveNode);
    }

    runner->printNode(leaveNode, process);
    options.eventsProcessed++;
}

void CDMRunner::handleMPIComm(ITraceReader *reader, MPIType mpiType, uint64_t processId,
        uint64_t partnerId, uint32_t root, uint32_t tag)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();

    Process *process = analysis.getProcess(processId);

    Process::MPIType pMPIType;

    switch (mpiType)
    {
        case io::MPI_COLLECTIVE:
            pMPIType = Process::MPI_COLLECTIVE;
            break;
        case io::MPI_RECV:
            pMPIType = Process::MPI_RECV;
            break;
        case io::MPI_SEND:
            pMPIType = Process::MPI_SEND;
            break;
        default:
            throw RTException("Unknown cdm::io::MPIType %u", mpiType);
    }

    if (runner->getOptions().verbose >= VERBOSE_ALL)
    {
        printf(" [%u] mpi record, [%lu > %lu], type %u, tag %u\n",
                analysis.getMPIRank(),
                processId, partnerId,
                pMPIType, tag);
    }

    process->setPendingMPIRecord(pMPIType, partnerId, root);
}

void CDMRunner::handleMPICommGroup(ITraceReader *reader, uint32_t group,
        uint32_t numProcs, const uint64_t *procs)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());

    runner->getAnalysis().getMPIAnalysis().setMPICommGroupMap(group, numProcs, procs);
}

void CDMRunner::readOTF()
{
    uint32_t mpiRank = analysis.getMPIRank();
    ITraceReader *traceReader;

    if (mpiRank == 0)
        printf("[%u] Reading OTF %s\n", mpiRank, options.filename.c_str());
    //OTF1MODE
    if (strstr(options.filename.c_str(), ".otf2") == NULL)
    {
        if (mpiRank == 0)
            printf("Operating in OTF1-Mode.\n");
        traceReader = new OTF1TraceReader(this, mpiRank);
    } else
    {
        if (mpiRank == 0)
            printf("Operating in OTF2-Mode.\n");
        traceReader = new OTF2TraceReader(this, mpiRank);
    }

    traceReader->handleDefProcess = handleDefProcess;
    traceReader->handleDefFunction = handleDefFunction;
    traceReader->handleEnter = handleEnter;
    traceReader->handleLeave = handleLeave;
    traceReader->handleProcessMPIMapping = handleProcessMPIMapping;
    traceReader->handleMPIComm = handleMPIComm;
    traceReader->handleMPICommGroup = handleMPICommGroup;

    traceReader->open(options.filename, 10);
    if (options.verbose >= VERBOSE_BASIC)
        printf(" [%u] Reading definitions\n", mpiRank);
    traceReader->readDefinitions();

    analysis.getMPIAnalysis().createMPICommunicatorsFromMap();
    analysis.setWaitStateFunctionId(analysis.getNewFunctionId());

    uint64_t timerResolution = traceReader->getTimerResolution();
    analysis.setTimerResolution(timerResolution);
    if (options.verbose >= VERBOSE_BASIC)
        printf(" [%u] Timer resolution = %llu\n",
            mpiRank, (long long unsigned) (timerResolution));
    if (timerResolution < 1000000000) // 1GHz
        printf(" [%u] Warning: your timer resolution is very low!\n", mpiRank);

    try
    {
        if (options.verbose >= VERBOSE_BASIC)
            printf(" [%u] Reading events\n", mpiRank);
        traceReader->readEvents();
    } catch (RTException e)
    {
        // dump
        dumpAllocationTail(analysis);
        throw e;
    }

    traceReader->close();
    delete traceReader;
}

void CDMRunner::getOptFactors(char *optKernels, std::map<uint64_t, double>& optFactors)
{
    const char *delims[] = {"%", ","};
    size_t i = 0;
    char *token = NULL;

    std::vector<std::string> kernels;
    std::vector<std::string> factors;

    if (optKernels == NULL)
        return;

    token = strtok(optKernels, delims[i]);

    while (token)
    {
        if (i == 0)
            kernels.push_back(token);
        else
            factors.push_back(token);

        i = (i + 1) % 2;
        token = strtok(NULL, delims[i]);
    }

    if (kernels.size() != factors.size())
        throw RTException("Wrong format");

    for (i = 0; i < kernels.size(); ++i)
    {
        int factor = atoi(factors[i].c_str());
        if (factor < 0 || factor > 100)
            throw RTException("Wrong format");

        TraceData::ActivityList& kernelsList = analysis.getActivities();
        for (TraceData::ActivityList::const_iterator iter = kernelsList.begin();
                iter != kernelsList.end(); ++iter)
        {
            if (strcmp((*iter)->getName(), kernels[i].c_str()) == 0)
            {
                double realFactor = (double) (100 - factor) / 100.0;
                optFactors[(*iter)->getFunctionId()] = realFactor;
                printf("Optimizing kernel '%s' by %d%%\n",
                        kernels[i].c_str(), factor);
                break;
            }
        }
    }
}

void CDMRunner::mergeActivityGroups(ActivityGroupMap& activityGroupMap, bool cpKernelsOnly)
{
    /* phase 1: each MPI process*/

    uint32_t blameStatCtrId = analysis.getCtrTable().getCtrId(CTR_BLAME_STATISTICS);
    uint32_t blameLocalCtrId = analysis.getCtrTable().getCtrId(CTR_BLAME);
    uint32_t cpCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH);
    uint32_t cpTimeCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH_TIME);
    uint64_t lengthCritPath = analysis.getLastGraphNode(PARADIGM_COMPUTE_LOCAL)->getTime() -
            analysis.getSourceNode()->getTime();

    uint64_t globalBlame = 0;

    // group all activity instances
    TraceData::ActivityList &activities = analysis.getActivities();
    for (TraceData::ActivityList::const_iterator iter = activities.begin();
            iter != activities.end(); ++iter)
    {
        Activity *activity = *iter;
        bool onCriticalPath = activity->getStart()->getCounter(cpCtrId, NULL);

        if (cpKernelsOnly && (!onCriticalPath || !activity->getStart()->isCUDAKernel()))
            continue;

        bool valid = false;
        uint64_t blameStatCtr = activity->getStart()->getCounter(blameStatCtrId, &valid);
        uint64_t blameLocalCtr = activity->getStart()->getCounter(blameLocalCtrId, &valid);
        uint64_t cpTimeCtr = activity->getStart()->getCounter(cpTimeCtrId, &valid);

        uint64_t fId = activity->getFunctionId();
        std::map<uint64_t, ActivityGroup>::iterator groupIter = activityGroupMap.find(fId);
        if (groupIter != activityGroupMap.end())
        {
            // update
            groupIter->second.numInstances++;
            groupIter->second.totalBlame += blameStatCtr + blameLocalCtr;
            groupIter->second.totalDuration += activity->getDuration();
            groupIter->second.totalDurationOnCP += cpTimeCtr;
        } else
        {
            activityGroupMap[fId].functionId = fId;
            activityGroupMap[fId].numInstances = 1;
            activityGroupMap[fId].numUnifyProcesses = 1;
            activityGroupMap[fId].totalBlame = blameStatCtr + blameLocalCtr;
            activityGroupMap[fId].totalDuration = activity->getDuration();
            activityGroupMap[fId].totalDurationOnCP = cpTimeCtr;
        }
    }

    // compute some final metrics
    for (std::map<uint64_t, ActivityGroup>::iterator groupIter =
            activityGroupMap.begin(); groupIter != activityGroupMap.end();)
    {
        std::map<uint64_t, ActivityGroup>::iterator iter = groupIter;
        groupIter->second.fractionCP =
                (double) (groupIter->second.totalDurationOnCP) / (double) lengthCritPath;
        globalBlame += groupIter->second.totalBlame;

        groupIter = ++iter;
    }

    /* phase 2: MPI all-reduce */

    // send/receive groups to master/from other MPI processes
    if (analysis.getMPIRank() == 0)
    {
        if (options.verbose >= VERBOSE_BASIC)
            printf("Combining results from all analysis processes\n");

        // receive from all other MPI processes
        uint32_t mpiSize = analysis.getMPIAnalysis().getMPISize();
        for (uint32_t rank = 1; rank < mpiSize; ++rank)
        {
            // receive number of entries
            uint32_t numEntries = 0;
            MPI_Status status;

            ///\todo: receive from any
            MPI_Recv(&numEntries, 1, MPI_INTEGER4, rank, 1, MPI_COMM_WORLD, &status);

            // receive entries
            if (numEntries > 0)
            {
                ActivityGroup *buf = new ActivityGroup[numEntries];
                MPI_Recv(buf, numEntries * sizeof (ActivityGroup), MPI_BYTE,
                        rank, 2, MPI_COMM_WORLD, &status);

                // combine with own activity groups
                std::map<uint64_t, ActivityGroup>::iterator groupIter;
                for (uint32_t i = 0; i < numEntries; ++i)
                {
                    ActivityGroup *group = &(buf[i]);
                    uint64_t fId = group->functionId;
                    groupIter = activityGroupMap.find(fId);

                    if (groupIter != activityGroupMap.end())
                    {
                        groupIter->second.numInstances += group->numInstances;
                        groupIter->second.totalBlame += group->totalBlame;
                        groupIter->second.totalDuration += group->totalDuration;
                        groupIter->second.totalDurationOnCP += group->totalDurationOnCP;
                        groupIter->second.fractionCP += group->fractionCP;
                        groupIter->second.numUnifyProcesses++;
                    } else
                    {
                        activityGroupMap[fId].functionId = fId;
                        activityGroupMap[fId].numInstances = group->numInstances;
                        activityGroupMap[fId].fractionCP = group->fractionCP;
                        activityGroupMap[fId].totalBlame = group->totalBlame;
                        activityGroupMap[fId].totalDuration = group->totalDuration;
                        activityGroupMap[fId].totalDurationOnCP = group->totalDurationOnCP;
                        activityGroupMap[fId].numUnifyProcesses = group->numUnifyProcesses;
                    }

                    globalBlame += group->totalBlame;
                }

                delete[] buf;
            }
        }

        for (std::map<uint64_t, ActivityGroup>::iterator groupIter = activityGroupMap.begin();
                groupIter != activityGroupMap.end(); ++groupIter)
        {
            std::map<uint64_t, ActivityGroup>::iterator iter = groupIter;
            groupIter->second.fractionCP /= (double) (iter->second.numUnifyProcesses);
            if (globalBlame > 0)
            {
                groupIter->second.fractionBlame =
                        (double) (groupIter->second.totalBlame) / (double) globalBlame;
            } else
                groupIter->second.fractionBlame = 0.0;
        }
    } else
    {
        // send to MPI master process (rank 0)
        uint32_t numEntries = activityGroupMap.size();
        MPI_Send(&numEntries, 1, MPI_INTEGER4, 0, 1, MPI_COMM_WORLD);

        ActivityGroup *buf = new ActivityGroup[numEntries];

        uint32_t i = 0;
        for (std::map<uint64_t, ActivityGroup>::iterator groupIter = activityGroupMap.begin();
                groupIter != activityGroupMap.end(); ++groupIter)
        {
            memcpy(&(buf[i]), &(groupIter->second), sizeof (ActivityGroup));
            ++i;
        }

        MPI_Send(buf, numEntries * sizeof (ActivityGroup), MPI_CHAR, 0, 2, MPI_COMM_WORLD);

        delete[] buf;
    }
}

void CDMRunner::getCriticalPath(Process::SortedGraphNodeList &criticalNodes)
{
    VT_TRACER("getCriticalPath");
    MPIAnalysis::CriticalSectionsMap sectionsMap;
    criticalNodes.clear();

    if (mpiSize > 1)
    {
        // better reverse replay strategy
        MPIAnalysis::CriticalSectionsList sectionsList;
        reverseReplayMPICriticalPath(sectionsList);
        getCriticalLocalSections(sectionsList.data(), sectionsList.size(), criticalNodes, sectionsMap);
    } else
    {
        /* compute local critical path on root, only */
        if (options.verbose >= VERBOSE_BASIC)
            printf("[0] Single process: defaulting to CUDA/OMP only mode\n");

        Graph *subGraph = analysis.getGraph(PARADIGM_COMPUTE_LOCAL);

        getCriticalPathIntern(analysis.getSourceNode(),
                analysis.getLastGraphNode(PARADIGM_COMPUTE_LOCAL),
                criticalNodes, *subGraph);

        delete subGraph;
    }

    /* compute the time-on-critical-path counter */
    if (mpiRank == 0)
        printf("[%u] Computing additional counters\n", mpiRank);
    
    const uint32_t cpCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH);
    for (Process::SortedGraphNodeList::const_iterator iter = criticalNodes.begin();
            iter != criticalNodes.end(); ++iter)
    {
        (*iter)->setCounter(cpCtrId, 1);
    }

    Allocation::ProcessList processes;
    analysis.getProcesses(processes);
    const uint32_t cpTimeCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH_TIME);
    
#pragma omp parallel for
    for (size_t i = 0; i < processes.size(); ++i)
    {
        Process *p = processes[i];
        if (p->isRemoteProcess())
            continue;

        Process::SortedNodeList &nodes = p->getNodes();

        GraphNode *lastNode = NULL;
        std::stack<uint64_t> cpTime;

        for (Process::SortedNodeList::const_iterator nIter = nodes.begin();
                nIter != nodes.end(); ++nIter)
        {
            if (!(*nIter)->isGraphNode() || (*nIter)->isAtomic())
                continue;

            GraphNode *node = (GraphNode*)(*nIter);
            if (!lastNode)
                lastNode = node;

            bool lastNodeOnCP = lastNode->getCounter(cpCtrId, NULL);
            if (node->isEnter())
            {
                if (lastNodeOnCP && !cpTime.empty())
                    cpTime.top() += node->getTime() - lastNode->getTime();

                cpTime.push(0);
            }
            else
            {
                uint64_t myCPTime = cpTime.top();
                cpTime.pop();

                /* compute time for non-stacked function (parts) */
                if (lastNodeOnCP && (lastNode == node->getPartner() || lastNode->isLeave()))
                    myCPTime += node->getTime() - lastNode->getTime();

                node->getPartner()->setCounter(cpTimeCtrId, myCPTime);
            }

            lastNode = node;
        }
    }

    if (options.mergeActivities)
    {
        if (mpiRank == 0)
            printf("[%u] Merging activity statistics\n", mpiRank);

        /* compute total program runtime, i.e. total length of the critical path */
        GraphNode *firstNode = NULL;
        GraphNode *lastNode = NULL;

        for (Process::SortedGraphNodeList::const_iterator iter = criticalNodes.begin();
                iter != criticalNodes.end(); ++iter)
        {
            GraphNode* currentEvent = *iter;
            if (!firstNode || (Node::compareLess(currentEvent, firstNode)))
            {
                firstNode = currentEvent;
            }

            if (!lastNode || (Node::compareLess(lastNode, currentEvent)))
            {
                lastNode = currentEvent;
            }
        }

        uint64_t firstTimestamp = firstNode->getTime();
        if (firstNode->isMPIInit() && firstNode->isLeave())
            firstTimestamp = firstNode->getPartner()->getTime();
        uint64_t lastTimestamp = lastNode->getTime();

        if (mpiSize > 1)
        {
            MPI_Allreduce(&firstTimestamp, &firstTimestamp, 1, MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&lastTimestamp, &lastTimestamp, 1, MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
        }

        uint64_t globalCPLength = lastTimestamp - firstTimestamp;
        if (mpiRank == 0)
            std::cout << "[0] Critical path length: " << analysis.getRealTime(globalCPLength);

        ActivityGroupMap activityGroupMap;
        mergeActivityGroups(activityGroupMap, false);
        printAllActivities(globalCPLength, activityGroupMap);
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

void CDMRunner::getCriticalPathIntern(GraphNode *start, GraphNode *end,
        Process::SortedGraphNodeList& cpNodes, Graph& subGraph)
{
    VT_TRACER("getCriticalPathIntern");
    if (options.printCriticalPath)
    {
        printf("\n[%u] Longest path (%s,%s):\n",
                analysis.getMPIRank(),
                start->getUniqueName().c_str(),
                end->getUniqueName().c_str());
    }

    GraphNode::GraphNodeList criticalPath;
    subGraph.getLongestPath(start, end, criticalPath);

    uint32_t i = 0;
    for (GraphNode::GraphNodeList::const_iterator cpNode = criticalPath.begin();
            cpNode != criticalPath.end(); ++cpNode)
    {
        GraphNode *node = *cpNode;
        cpNodes.push_back(node);

        if (options.printCriticalPath)
        {
            printf("[%u] %u: %s (%f)\n",
                    analysis.getMPIRank(), i,
                    node->getUniqueName().c_str(),
                    analysis.getRealTime(node->getTime()));
        }

        i++;
    }
}

void CDMRunner::getCriticalLocalSections(MPIAnalysis::CriticalPathSection *sections,
        uint32_t numSections, Process::SortedGraphNodeList& criticalNodes,
        MPIAnalysis::CriticalSectionsMap& sectionsMap)
{
    uint32_t mpiRank = analysis.getMPIRank();
    Graph *subGraph = analysis.getGraph(PARADIGM_COMPUTE_LOCAL);

    omp_lock_t localNodesLock;
    omp_init_lock(&localNodesLock);

    uint32_t lastSecCtr = 0;

#pragma omp parallel for
    for (uint32_t i = 0; i < numSections; ++i)
    {
        MPIAnalysis::CriticalPathSection *section = &(sections[i]);

        if (options.verbose >= VERBOSE_BASIC)
        {
            printf("[%u] computing local critical path between MPI nodes [%u, %u] on process %lu\n",
                    mpiRank, section->nodeStartID, section->nodeEndID, section->processID);
        }

        // find local MPI nodes with their IDs
        GraphNode *startNode = NULL, *endNode = NULL;
        const Process::SortedNodeList &mpiNodes =
                analysis.getProcess(sections->processID)->getNodes();
        for (Process::SortedNodeList::const_iterator iter = mpiNodes.begin();
                iter != mpiNodes.end(); ++iter)
        {
            if (!(*iter)->isGraphNode() || !(*iter)->isMPI())
                continue;

            if (startNode && endNode)
                break;

            GraphNode *node = (GraphNode*) (*iter);
            if (node->getId() == section->nodeStartID)
            {
                startNode = node;
                continue;
            }

            if (node->getId() == section->nodeEndID)
            {
                endNode = node;
                continue;
            }
        }

        if (!startNode || !endNode)
            throw RTException("[%u] Did not find local nodes for node IDs %u and %u",
                mpiRank, section->nodeStartID, section->nodeEndID);
        else
        {
            if (options.verbose >= VERBOSE_BASIC)
            {
                printf("[%u] node mapping: %u = %s (%f), %u = %s (%f)\n",
                        mpiRank, section->nodeStartID, startNode->getUniqueName().c_str(),
                        analysis.getRealTime(startNode->getTime()),
                        section->nodeEndID, endNode->getUniqueName().c_str(),
                        analysis.getRealTime(endNode->getTime()));
            }
        }

        MPIAnalysis::CriticalPathSection mappedSection;
        mappedSection.processID = section->processID;
        mappedSection.nodeStartID = section->nodeStartID;
        mappedSection.nodeEndID = section->nodeEndID;

        if (startNode->isEnter())
            startNode = startNode->getPartner();

        if (endNode->isLeave())
            endNode = endNode->getPartner();

        GraphNode *csStartOuter = startNode;
        GraphNode *csEndOuter = endNode;

        sectionsMap[csStartOuter] = mappedSection;
        sectionsMap[csEndOuter] = mappedSection;

        GraphNode *startLocalNode = startNode->getLinkRight();
        GraphNode *endLocalNode = endNode->getLinkLeft();

        if ((!startLocalNode || !endLocalNode) || (startLocalNode->getTime() >= endLocalNode->getTime()))
        {
            if (options.verbose >= VERBOSE_BASIC)
            {
                printf("[%u] No local path possible between MPI nodes %s (link %p) and %s (link %p)\n",
                        mpiRank,
                        startNode->getUniqueName().c_str(),
                        startLocalNode,
                        endNode->getUniqueName().c_str(),
                        endLocalNode);
                if (startLocalNode && endLocalNode)
                {
                    printf("[%u] local nodes %s and %s\n",
                            mpiRank,
                            startLocalNode->getUniqueName().c_str(),
                            endLocalNode->getUniqueName().c_str());
                }
            }
            continue;
        }

        if (options.verbose >= VERBOSE_BASIC)
        {
            printf("[%u] MPI/local mapping: %u > %s, %u > %s\n",
                    mpiRank, section->nodeStartID, startLocalNode->getUniqueName().c_str(),
                    section->nodeEndID, endLocalNode->getUniqueName().c_str());

            printf("[%d] Computing local critical path (%s, %s)\n", mpiRank,
                    startLocalNode->getUniqueName().c_str(),
                    endLocalNode->getUniqueName().c_str());
        }

        Process::SortedGraphNodeList sectionLocalNodes;
        sectionLocalNodes.push_back(startNode);

        getCriticalPathIntern(startLocalNode, endLocalNode, sectionLocalNodes, *subGraph);

        sectionLocalNodes.push_back(endNode);
        if (endNode->isMPIFinalize() && endNode->isEnter())
            sectionLocalNodes.push_back(endNode->getPartner());

        omp_set_lock(&localNodesLock);
        criticalNodes.insert(criticalNodes.end(), sectionLocalNodes.begin(), sectionLocalNodes.end());
        omp_unset_lock(&localNodesLock);

        if ((mpiRank == 0) && (omp_get_thread_num() == 0) && (i - lastSecCtr > numSections / 10))
        {
            printf("[%u] %lu%% ", mpiRank, (size_t)(100.0 * (double) i / (double) numSections));
            fflush(NULL);
            lastSecCtr = i;
        }
    }

    omp_destroy_lock(&localNodesLock);

    if (mpiRank == 0)
        printf("[%u] 100%%\n", mpiRank);

    delete subGraph;
}

void CDMRunner::findLastMpiNode(GraphNode **node)
{
    uint64_t lastMpiNodeTime = 0;
    int lastMpiRank = mpiRank;
    uint64_t nodeTimes[analysis.getMPIAnalysis().getMPISize()];
    *node = NULL;

    GraphNode *myLastMpiNode = analysis.getLastGraphNode(PARADIGM_MPI);
    if (myLastMpiNode)
        lastMpiNodeTime = myLastMpiNode->getTime();

    MPI_CHECK(MPI_Allgather(&lastMpiNodeTime, 1, MPI_UNSIGNED_LONG_LONG, nodeTimes,
            1, MPI_UNSIGNED_LONG_LONG, MPI_COMM_WORLD));

    for (uint32_t i = 0; i < analysis.getMPIAnalysis().getMPISize(); ++i)
        if (nodeTimes[i] > lastMpiNodeTime)
        {
            lastMpiNodeTime = nodeTimes[i];
            lastMpiRank = i;
        }

    if (lastMpiRank == mpiRank)
    {
        *node = myLastMpiNode;
        myLastMpiNode->setCounter(analysis.getCtrTable().getCtrId(CTR_CRITICALPATH), 1);

        if (options.verbose >= VERBOSE_ANNOY)
        {
            printf("[%u] critical path reverse replay starts at node %s (%f)\n",
                    mpiRank, myLastMpiNode->getUniqueName().c_str(),
                    analysis.getRealTime(myLastMpiNode->getTime()));
        }
    }
}

void CDMRunner::reverseReplayMPICriticalPath(MPIAnalysis::CriticalSectionsList & sectionsList)
{
    const uint32_t NO_MSG = 0;
    const uint32_t PATH_FOUND_MSG = 1;

    const size_t BUFFER_SIZE = 3;

    /* decide on globally last MPI node to start with */
    GraphNode *currentNode = NULL;
    GraphNode *lastNode = NULL;
    GraphNode *sectionEndNode = NULL;
    bool isMaster = false;

    findLastMpiNode(&currentNode);
    if (currentNode)
    {
        isMaster = true;
        sectionEndNode = currentNode;
    }

    Graph *mpiGraph = analysis.getGraph(PARADIGM_MPI);
    uint32_t cpCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH);
    uint64_t sendBfr[BUFFER_SIZE];
    uint64_t recvBfr[BUFFER_SIZE];

    while (true)
    {
        if (isMaster)
        {
            /* master */
            if (options.verbose >= VERBOSE_ANNOY)
            {
                if (lastNode)
                    printf("[%u] isMaster, currentNode = %s (%f), lastNode = %s (%f)\n",
                        mpiRank, currentNode->getUniqueName().c_str(),
                        analysis.getRealTime(currentNode->getTime()),
                        lastNode->getUniqueName().c_str(),
                        analysis.getRealTime(lastNode->getTime()));
                else
                    printf("[%u] isMaster, currentNode = %s (%f)\n",
                        mpiRank, currentNode->getUniqueName().c_str(),
                        analysis.getRealTime(currentNode->getTime()));
            }

            if (lastNode && (lastNode->getId() <= currentNode->getId()))
                printf("[%u] ! [Warning] current node ID is not strictly decreasing\n", mpiRank);

            if (currentNode->isLeave())
            {
                /* isLeave */
                Edge *activityEdge = analysis.getEdge(currentNode->getPartner(), currentNode);

                if (activityEdge->isBlocking() || currentNode->isMPIInit())
                {
                    /* create section */
                    if (currentNode != sectionEndNode)
                    {
                        MPIAnalysis::CriticalPathSection section;
                        section.processID = currentNode->getProcessId();
                        section.nodeStartID = currentNode->getId();
                        section.nodeEndID = sectionEndNode->getId();
                        sectionsList.push_back(section);

                        currentNode->setCounter(cpCtrId, 1);
                    }
                    sectionEndNode = NULL;
                }

                /* change process for blocking edges */
                if (activityEdge->isBlocking())
                {
                    /* make myself a new slave */
                    isMaster = false;

                    /* commnicate with slaves to decide new master */
                    GraphNode *commMaster = currentNode->getGraphPair().second;
                    bool nodeHasRemoteInfo = false;
                    analysis.getMPIAnalysis().getRemoteNodeInfo(commMaster, &nodeHasRemoteInfo);
                    if (!nodeHasRemoteInfo)
                    {
                        commMaster = currentNode->getGraphPair().first;
                    }

                    if (options.verbose >= VERBOSE_ANNOY)
                    {
                        printf("[%u]  found waitstate for %s (%f), changing process at %s\n",
                                mpiRank, currentNode->getUniqueName().c_str(),
                                analysis.getRealTime(currentNode->getTime()),
                                commMaster->getUniqueName().c_str());
                    }

                    std::set<uint32_t> mpiPartnerRanks = analysis.getMPIAnalysis().getMpiPartnersRank(commMaster);
                    for (std::set<uint32_t>::const_iterator iter = mpiPartnerRanks.begin();
                            iter != mpiPartnerRanks.end(); ++iter)
                    {
                        /* communicate with all slaves to find new master */
                        uint32_t commMpiRank = *iter;

                        if (commMpiRank == (uint32_t) mpiRank)
                            continue;

                        sendBfr[0] = commMaster->getId();
                        sendBfr[1] = commMaster->getProcessId();
                        sendBfr[2] = NO_MSG;

                        if (options.verbose >= VERBOSE_ANNOY)
                        {
                            printf("[%u]  testing remote MPI worker %u for remote edge to my node %u on process %lu\n",
                                    mpiRank, commMpiRank, (uint32_t) sendBfr[0], sendBfr[1]);
                        }
                        MPI_Request request;
                        MPI_CHECK(MPI_Isend(sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                commMpiRank, 0, MPI_COMM_WORLD, &request));
                    }
                    /* continue main loop as slave */
                } else
                {
                    currentNode->setCounter(cpCtrId, 1);
                    lastNode = currentNode;
                    currentNode = activityEdge->getStartNode();
                    /* continue main loop as master */
                }
            } else
            {
                /* isEnter */
                if (currentNode->isMPIInit())
                {
                    currentNode->setCounter(cpCtrId, 1);

                    /* notify all slaves that we are done */
                    if (options.verbose >= VERBOSE_ANNOY)
                        printf("[%u] * asking all slaves to terminate\n", mpiRank);

                    sendBfr[0] = 0;
                    sendBfr[1] = 0;
                    sendBfr[2] = PATH_FOUND_MSG;

                    std::set<uint64_t> mpiPartners = analysis.getMPIAnalysis().getMPICommGroup(0).procs;
                    for (std::set<uint64_t>::const_iterator iter = mpiPartners.begin();
                            iter != mpiPartners.end(); ++iter)
                    {
                        int commMpiRank = analysis.getMPIAnalysis().getMPIRank(*iter);
                        if (commMpiRank == mpiRank)
                            continue;

                        MPI_CHECK(MPI_Send(sendBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                                commMpiRank, 0, MPI_COMM_WORLD));
                    }

                    /* leave main loop */
                    break;
                }


                bool foundPredecessor = false;
                const Graph::EdgeList &inEdges = mpiGraph->getInEdges(currentNode);
                for (Graph::EdgeList::const_iterator iter = inEdges.begin();
                        iter != inEdges.end(); ++iter)
                {
                    Edge *intraEdge = *iter;
                    if (intraEdge->isIntraProcessEdge())
                    {
                        currentNode->setCounter(cpCtrId, 1);
                        lastNode = currentNode;
                        currentNode = intraEdge->getStartNode();
                        foundPredecessor = true;
                        /* continue main loop as master */
                        break;
                    }
                }

                if (!foundPredecessor)
                    throw RTException("[%u] No ingoing intra-process edge for node %s",
                        currentNode->getUniqueName().c_str());

            }
        } else
        {
            /* slave */
            //int flag = 0;
            MPI_Status status;
            MPI_CHECK(MPI_Recv(recvBfr, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE,
                    MPI_ANY_TAG, MPI_COMM_WORLD, &status));

            if (recvBfr[2] == PATH_FOUND_MSG)
            {
                if (options.verbose >= VERBOSE_ANNOY)
                    printf("[%u] * terminate requested by master\n", mpiRank);
                break;
            }

            /* find local node for remote node id and decide if we can continue here */
            if (options.verbose >= VERBOSE_ANNOY)
            {
                printf("[%u]  tested by remote MPI worker %u for remote edge to its node %u on process %lu\n",
                        mpiRank, status.MPI_SOURCE, (uint32_t)recvBfr[0], recvBfr[1]);
            }

            /* check if the activity is a wait state */
            MPIAnalysis::MPIEdge mpiEdge;
            if (analysis.getMPIAnalysis().getRemoteMPIEdge((uint32_t)recvBfr[0], recvBfr[1], mpiEdge))
            {
                GraphNode::GraphNodePair &slaveActivity = mpiEdge.localNode->getGraphPair();
                /* we are the new master */

                isMaster = true;
                slaveActivity.second->setCounter(cpCtrId, 1);
                lastNode = slaveActivity.second;
                currentNode = slaveActivity.first;

                sectionEndNode = lastNode;

                if (options.verbose >= VERBOSE_ANNOY)
                {
                    printf("[%u] becomes new master at node %s, lastNode = %s\n",
                            mpiRank,
                            currentNode->getUniqueName().c_str(),
                            lastNode->getUniqueName().c_str());
                }

                /* continue main loop as master */
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

}

void CDMRunner::runAnalysis(Paradigm paradigm, Process::SortedNodeList &allNodes)
{
    analysis.removeRules();

    switch (paradigm)
    {
        case PARADIGM_CUDA:
            if (mpiRank == 0)
                printf("[%u] Running analysis: CUDA\n", mpiRank);

            analysis.addRule(new KernelLaunchRule(9));
            analysis.addRule(new BlameSyncRule(1));
            analysis.addRule(new BlameKernelRule(1));
            analysis.addRule(new LateSyncRule(1));
            analysis.addRule(new EventLaunchRule(1));
            analysis.addRule(new EventSyncRule(1));
            analysis.addRule(new EventQueryRule(1));
            analysis.addRule(new StreamWaitRule(1));
            break;

        case PARADIGM_MPI:
            if (mpiRank == 0)
                printf("[%u] Running analysis: MPI\n", mpiRank);

            analysis.addRule(new RecvRule(1));
            analysis.addRule(new SendRule(1));
            analysis.addRule(new CollectiveRule(1));
            analysis.addRule(new SendRecvRule(1));
            analysis.addRule(new OneToAllRule(1));
            analysis.addRule(new AllToOneRule(1));
            break;

        case PARADIGM_OMP:
            if (mpiRank == 0)
                printf("[%u] Running analysis: OMP\n", mpiRank);

            analysis.addRule(new OMPParallelRegionRule(1));
            analysis.addRule(new OMPComputeRule(1));
            analysis.addRule(new OMPBarrierRule(1));
            break;

        default:
            if (mpiRank == 0)
                printf("[%u] No analysis for unknown paradigm %d\n",
                    mpiRank, paradigm);
            return;
    }

    size_t ctr = 0, last_ctr = 0;
    size_t num_nodes = allNodes.size();

    for (Process::SortedNodeList::const_iterator nIter = allNodes.begin();
            nIter != allNodes.end(); ++nIter)
    {
        VT_TRACER("analysis iter");
        Node *node = *nIter;
        ctr++;

        if (!node->isGraphNode()) // || !node->isLeave())
            continue;

        if (!(node->getParadigm() & paradigm))
            continue;

        analysis.applyRules(node, options.verbose >= VERBOSE_BASIC);

        if ((mpiRank == 0) && (ctr - last_ctr > num_nodes / 10))
        {
            printf("[%u] %lu%% ", mpiRank, (size_t)(100.0 * (double) ctr / (double) num_nodes));
            fflush(NULL);
            last_ctr = ctr;
        }
    }
    if (mpiRank == 0)
        printf("[%u] 100%%\n", mpiRank);

#ifdef DEBUG
    analysis.runSanityCheck(mpiRank);
#endif
}

void CDMRunner::printAllActivities(uint64_t globalCPLength, std::map<uint64_t, ActivityGroup>& activityGroupMap)
{
    if (mpiRank == 0)
    {
        printf("\n%50s %10s %10s %11s %12s %21s %9s\n", "Activity Group",
                "Instances", "Time", "Time on CP", "Fraction CP", "Fraction Global Blame", "Rating");

        std::set<ActivityGroup, ActivityGroupCompare> sortedActivityGroups;

        for (std::map<uint64_t, ActivityGroup>::iterator iter = activityGroupMap.begin();
                iter != activityGroupMap.end(); ++iter)
        {
            iter->second.fractionCP = 0.0;
            if (iter->second.totalDurationOnCP > 0)
            {
                iter->second.fractionCP =
                        (double) iter->second.totalDurationOnCP / (double) globalCPLength;
            }

            sortedActivityGroups.insert(iter->second);
        }

        const size_t max_ctr = 10;
        size_t ctr = 0;
        for (std::set<ActivityGroup, ActivityGroupCompare>::const_iterator iter =
                sortedActivityGroups.begin(); iter != sortedActivityGroups.end() && ctr < max_ctr; ++iter)
        {
            ++ctr;
            printf("%50.50s %10u %10f %11f %11.2f%% %20.2f%%  %7.6f\n",
                    analysis.getFunctionName(iter->functionId),
                    iter->numInstances,
                    analysis.getRealTime(iter->totalDuration),
                    analysis.getRealTime(iter->totalDurationOnCP),
                    100.0 * iter->fractionCP,
                    100.0 * iter->fractionBlame,
                    iter->fractionCP +
                    iter->fractionBlame);
        }
    }
}
