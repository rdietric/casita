/* 
 * File:   CDMRunner.cpp
 * Author: felix
 * 
 * Created on 13. August 2013, 10:40
 */

#include <sys/types.h>

#include "CDMRunner.hpp"

#include "otf/IKeyValueList.hpp"
#include "otf/OTF1TraceReader.hpp"

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

using namespace cdm;
using namespace cdm::io;

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
        printf("# %s (%u)\n", p->getName(), p->getId());
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

CDMRunner::CDMRunner(int mpiRank, int mpiSize, ProgramOptions options) :
mpiRank(mpiRank),
mpiSize(mpiSize),
analysis(mpiRank, mpiSize)
{
    memcpy(&(this->options), &options, sizeof (ProgramOptions));
    if (options.noErrors)
        ErrorUtils::getInstance().setNoExceptions();
}

CDMRunner::~CDMRunner()
{
}

CDMRunner::ProgramOptions& CDMRunner::getOptions()
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
        printf(" [%u]", analysis.getMPIRank());
        if (node->isEnter())
            printf(" E ");
        else
            printf(" L ");

        printf("[%12lu:%12.8fs:%10u,%5u] [%20s] proc [%15s], pid [%10u], [%s]",
                node->getTime(),
                (double) (node->getTime()) / (double) analysis.getTimerResolution(),
                node->getId(),
                node->getFunctionId(),
                node->getName(),
                process->getName(),
                process->getId(),
                Node::typeToStr(node->getType()).c_str());

        uint32_t refProcess = node->getReferencedProcessId();
        if (refProcess)
            printf(", ref = %u", refProcess);

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

void CDMRunner::handleProcessMPIMapping(ITraceReader *reader, uint32_t processId, uint32_t mpiRank)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    runner->getAnalysis().getMPIAnalysis().setMPIRank(processId, mpiRank);
}

void CDMRunner::handleDefProcess(ITraceReader *reader, uint32_t stream, uint32_t processId,
        uint32_t parentId, const char *name, IKeyValueList * list, bool isCUDA, bool isCUDANull)
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
        printf("  [%u] Found process %s (%u) with type %u, stream %u\n",
            analysis.getMPIRank(), name, processId, processType, stream);

    analysis.newProcess(processId, parentId, name, processType, GRAPH_CUDA);
}

void CDMRunner::handleDefFunction(ITraceReader *reader, uint32_t streamId,
        uint32_t functionId, const char *name, uint32_t functionGroupId)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    runner->getAnalysis().addFunction(functionId, name);
}

void CDMRunner::handleEnter(ITraceReader *reader, uint64_t time, uint32_t functionId,
        uint32_t processId, IKeyValueList *list)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();
    ProgramOptions &options = runner->getOptions();

    if (options.maxEvents > 0 && (options.eventsProcessed >= options.maxEvents))
        return;

    Process *process = analysis.getProcess(processId);
    if (!process)
        throw RTException("Process %u not found.", processId);

    const char *funcName = reader->getFunctionName(functionId).c_str();

    int functionType = TraceData::getFunctionType(funcName, process);
    if (functionType & NT_FT_CPU)
    {
        if (runner->getOptions().verbose >= VERBOSE_ALL)
            printf(" [%u] e [%12lu:%12.8fs] [%31s] (f%u:p%u)\n",
                analysis.getMPIRank(),
                time, (double) time / (double) analysis.getTimerResolution(),
                funcName, functionId, processId);
        return;
    }

    GraphNode *enterNode = NULL;
    if (Node::isEventType(functionType))
    {
        enterNode = analysis.addNewEventNode(time, 0, EventNode::FR_UNKNOWN, process,
                funcName, NT_RT_ENTER | functionType, NULL, NULL);
    } else
    {
        enterNode = analysis.addNewGraphNode(time, process, funcName,
                NT_RT_ENTER | functionType, NULL, NULL);
    }

    enterNode->setFunctionId(functionId);

    if (enterNode->isKernelLaunch())
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
            std::cout << sizeof (Edge) << std::endl;
            throw RTException("Memory limit exceeded (%d KByte / %d KByte)",
                    memUsage, options.memLimit);
        }
    }
}

void CDMRunner::handleLeave(ITraceReader *reader, uint64_t time,
        uint32_t functionId, uint32_t processId, IKeyValueList *list)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());
    AnalysisEngine &analysis = runner->getAnalysis();
    ProgramOptions &options = runner->getOptions();

    if (options.maxEvents > 0 && (options.eventsProcessed >= options.maxEvents))
        return;

    Process *process = runner->getAnalysis().getProcess(processId);
    if (!process)
        throw RTException("Process %u not found", processId);

    const char *funcName = reader->getFunctionName(functionId).c_str();

    int functionType = TraceData::getFunctionType(funcName, process);
    if (functionType & NT_FT_CPU)
    {
        if (runner->getOptions().verbose >= VERBOSE_ALL)
            printf(" [%u] l [%12lu:%12.8fs] [%31s] (f%u:p%u)\n",
                runner->getAnalysis().getMPIRank(),
                time, (double) time / (double) analysis.getTimerResolution(),
                funcName, functionId, processId);
        return;
    }

    GraphNode *leaveNode = NULL;
    if (Node::isEventType(functionType))
    {
        uint32_t eventId = readKeyVal(reader, VT_CUPTI_CUDA_EVENTREF_KEY, list);
        CUresult cuResult = (CUresult) readKeyVal(reader, VT_CUPTI_CUDA_CURESULT_KEY, list);
        EventNode::FunctionResultType fResult = EventNode::FR_UNKNOWN;
        if (cuResult == CUDA_SUCCESS)
            fResult = EventNode::FR_SUCCESS;

        leaveNode = runner->getAnalysis().addNewEventNode(time, eventId, fResult, process,
                funcName, NT_RT_LEAVE | functionType, NULL, NULL);

        if (eventId == 0)
            throw RTException("No eventId for event node %s found",
                leaveNode->getUniqueName().c_str());
    } else
    {
        leaveNode = analysis.addNewGraphNode(time, process, funcName,
                NT_RT_LEAVE | functionType, NULL, NULL);
    }

    leaveNode->setFunctionId(functionId);

    CDMRunner::applyStreamRefsLeave(reader, leaveNode, leaveNode->getGraphPair().first, list);
    if (leaveNode->isMPI())
    {
        Process::MPICommRecordList mpiCommRecords = process->getPendingMPIRecords();
        for (Process::MPICommRecordList::const_iterator iter = mpiCommRecords.begin();
                iter != mpiCommRecords.end(); ++iter)
        {
            uint32_t *sendPartnerId = NULL;

            switch (iter->mpiType)
            {
                case Process::MPI_RECV:
                case Process::MPI_COLLECTIVE:
                    leaveNode->setReferencedProcessId(iter->partnerId);
                    break;

                case Process::MPI_SEND:
                    sendPartnerId = new uint32_t;
                    *sendPartnerId = iter->partnerId;
                    leaveNode->setData(sendPartnerId);
                    break;

                default:
                    throw RTException("Not a valid MPICommRecord type here");
            }
        }
    }

    if (leaveNode->isKernelLaunch())
    {
        analysis.addPendingKernelLaunch(leaveNode);
    }

    runner->printNode(leaveNode, process);
    options.eventsProcessed++;
}

void CDMRunner::handleMPIComm(ITraceReader *reader, MPIType mpiType, uint32_t processId,
        uint32_t partnerId, uint32_t tag)
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
        printf(" [%u] mpi record, [%u > %u], type %u, tag %u\n",
                analysis.getMPIRank(),
                processId, partnerId,
                pMPIType, tag);
    }

    process->setPendingMPIRecord(pMPIType, partnerId);
}

void CDMRunner::handleMPICommGroup(ITraceReader *reader, uint32_t group,
        uint32_t numProcs, const uint32_t *procs)
{
    CDMRunner *runner = (CDMRunner*) (reader->getUserData());

    runner->getAnalysis().getMPIAnalysis().setMPICommGroupMap(group, numProcs, procs);
}

void CDMRunner::readOTF(const std::string filename)
{
    uint32_t mpiRank = analysis.getMPIRank();

    printf("[%u] Reading OTF %s\n", mpiRank, options.filename);

    ITraceReader *traceReader = new OTF1TraceReader(this, mpiRank);
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

void CDMRunner::getOptFactors(char *optKernels, std::map<uint32_t, double>& optFactors)
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

uint64_t CDMRunner::runOptimization(char *optKernels)
{
    std::map<uint32_t, double> optFactors;
    getOptFactors(optKernels, optFactors);

    if (optFactors.size() > 0)
    {
        analysis.optimizeKernel(optFactors, options.verbose >= VERBOSE_ANNOY);

        analysis.runSanityCheck(analysis.getMPIRank());
    } else
        printf("Found no matching kernel(s)\n");

    return analysis.getLastGraphNode()->getTime();
}

void CDMRunner::mergeActivityGroups(const Process::SortedGraphNodeList& cpActivities,
        std::map<uint32_t, ActivityGroup> &activityGroupMap, bool cpKernelsOnly)
{
    /* phase 1: each MPI process*/

    uint32_t blameCtrId = analysis.getCtrTable().getCtrId(CTR_BLAME);
    uint64_t lengthCritPath = analysis.getLastGraphNode(GRAPH_CUDA)->getTime() -
            analysis.getSourceNode()->getTime();

    uint64_t globalBlame = 0;

    // group all activity instances
    TraceData::ActivityList &activities = analysis.getActivities();
    for (TraceData::ActivityList::const_iterator iter = activities.begin();
            iter != activities.end(); ++iter)
    {
        Activity *activity = *iter;
        bool onCriticalPath = false;

        for (Process::SortedGraphNodeList::const_iterator actIter = cpActivities.begin();
                actIter != cpActivities.end(); ++actIter)
        {
            if (*actIter == activity->getEnd())
            {
                onCriticalPath = true;
                break;
            };
        }

        if (cpKernelsOnly && (!onCriticalPath || !activity->getStart()->isKernel()))
            continue;

        bool valid = false;
        uint64_t blameCtr = activity->getStart()->getCounter(blameCtrId, &valid);
        if (!valid)
            blameCtr = 0;

        uint32_t fId = activity->getFunctionId();
        std::map<uint32_t, ActivityGroup>::iterator groupIter = activityGroupMap.find(fId);
        if (groupIter != activityGroupMap.end())
        {
            // update
            groupIter->second.numInstances++;
            groupIter->second.totalBlame += blameCtr;
            groupIter->second.totalDuration += activity->getDuration();
            if (onCriticalPath)
                groupIter->second.totalDurationOnCP += activity->getDuration();
        } else
        {
            activityGroupMap[fId].functionId = fId;
            activityGroupMap[fId].numInstances = 1;
            activityGroupMap[fId].numUnifyProcesses = 1;
            activityGroupMap[fId].totalBlame = blameCtr;
            activityGroupMap[fId].totalDuration = activity->getDuration();
            if (onCriticalPath)
                activityGroupMap[fId].totalDurationOnCP = activity->getDuration();
            else
                activityGroupMap[fId].totalDurationOnCP = 0;
        }
    }

    // compute some final metrics
    for (std::map<uint32_t, ActivityGroup>::iterator groupIter =
            activityGroupMap.begin(); groupIter != activityGroupMap.end();)
    {
        std::map<uint32_t, ActivityGroup>::iterator iter = groupIter;
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
                std::map<uint32_t, ActivityGroup>::iterator groupIter;
                for (uint32_t i = 0; i < numEntries; ++i)
                {
                    ActivityGroup *group = &(buf[i]);
                    uint32_t fId = group->functionId;
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

        for (std::map<uint32_t, ActivityGroup>::iterator groupIter = activityGroupMap.begin();
                groupIter != activityGroupMap.end(); ++groupIter)
        {
            std::map<uint32_t, ActivityGroup>::iterator iter = groupIter;
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
        for (std::map<uint32_t, ActivityGroup>::iterator groupIter = activityGroupMap.begin();
                groupIter != activityGroupMap.end(); ++groupIter)
        {
            memcpy(&(buf[i]), &(groupIter->second), sizeof (ActivityGroup));
            ++i;
        }

        MPI_Send(buf, numEntries * sizeof (ActivityGroup), MPI_BYTE, 0, 2, MPI_COMM_WORLD);

        delete[] buf;
    }
}

void CDMRunner::getCriticalPath(Process::SortedGraphNodeList &gpuNodes,
        Process::SortedGraphNodeList &mpiNodes)
{
    VT_TRACER("getCriticalPath");
    MPIAnalysis::CriticalSectionsMap sectionsMap;
    gpuNodes.clear();
    mpiNodes.clear();

    if (mpiSize > 1)
    {
#ifdef MPI_CP_MERGE
        // merging strategy
        /* compute MPI critical path on root and distribute GPU sections */
        if (mpiRank == 0)
        {
            GraphNode *startNode = analysis.getSourceNode();
            GraphNode *endNode = analysis.getLastGraphNode(GRAPH_MPI);

            if (options.verbose >= VERBOSE_BASIC)
            {
                printf("[0] Computing MPI critical path (%s, %s)\n",
                        startNode->getUniqueName().c_str(),
                        endNode->getUniqueName().c_str());
            }

            getCriticalPathIntern(startNode, endNode, GRAPH_MPI, mpiNodes);

            distributeCriticalMPINodes(mpiNodes);
            distributeCriticalPathSections(mpiNodes, gpuNodes, sectionsMap);
        } else
        {
            receiveCriticalMPINodes();
            receiveCriticalPathSections(gpuNodes, sectionsMap);
        }
#else
        // better reverse replay strategy
        MPIAnalysis::CriticalSectionsList sectionsList;
        reverseReplayMPICriticalPath(sectionsList);
        getCriticalGPUSections(sectionsList.data(), sectionsList.size(), gpuNodes, sectionsMap);
#endif
    } else
    {
        /* compute GPU critical path on root, only */
        if (options.verbose >= VERBOSE_BASIC)
            printf("[0] Single process: defaulting to CUDA only mode\n");

        getCriticalPathIntern(analysis.getSourceNode(),
                analysis.getLastGraphNode(GRAPH_CUDA),
                GRAPH_CUDA, gpuNodes);
    }

    if (options.mergeActivities)
    {
        Process::SortedGraphNodeList allCriticalEvents;
        allCriticalEvents.insert(allCriticalEvents.end(), mpiNodes.begin(), mpiNodes.end());
        allCriticalEvents.insert(allCriticalEvents.end(), gpuNodes.begin(), gpuNodes.end());
        std::sort(allCriticalEvents.begin(), allCriticalEvents.end(), Node::compareLess);

        /* compute total program runtime, i.e. total length of the critical path */
        uint64_t firstTimestamp = 0;
        uint64_t lastTimestamp = 0;

        if (allCriticalEvents.size() > 0)
        {
            firstTimestamp = allCriticalEvents[0]->getTime();
            lastTimestamp = allCriticalEvents[allCriticalEvents.size() - 1]->getTime();
        }

        MPI_Allreduce(&firstTimestamp, &firstTimestamp, 1, MPI_INTEGER8, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&lastTimestamp, &lastTimestamp, 1, MPI_INTEGER8, MPI_MAX, MPI_COMM_WORLD);

        uint64_t globalCPLength = lastTimestamp - firstTimestamp;

        printAllActivities(allCriticalEvents, globalCPLength);
    }
}

void CDMRunner::getCriticalPathIntern(GraphNode *start, GraphNode *end,
        GraphNodeType g, Process::SortedGraphNodeList& cpNodes)
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
    analysis.getCriticalPath(start, end, &criticalPath, g);

    uint32_t i = 0;
    for (GraphNode::GraphNodeList::iterator cpNode = criticalPath.begin();
            cpNode != criticalPath.end(); ++cpNode)
    {
        GraphNode *node = *cpNode;
        cpNodes.push_back(node);

        node->setCounter(analysis.getCtrTable().getCtrId(CTR_CRITICALPATH), 1);

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

void CDMRunner::getCriticalGPUSections(MPIAnalysis::CriticalPathSection *sections,
        uint32_t numSections, Process::SortedGraphNodeList& gpuNodes,
        MPIAnalysis::CriticalSectionsMap& sectionsMap)
{
    uint32_t mpiRank = analysis.getMPIRank();
    for (uint32_t i = 0; i < numSections; ++i)
    {
        MPIAnalysis::CriticalPathSection *section = &(sections[i]);

        if (options.verbose >= VERBOSE_BASIC)
        {
            printf("[%u] computing GPU critical path between MPI nodes [%u, %u] on process %u\n",
                    mpiRank, section->nodeStartID, section->nodeEndID, section->processID);
        }

        // find local MPI nodes with their IDs
        GraphNode *startNode = NULL, *endNode = NULL;
        Process::SortedNodeList &mpiNodes =
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
        //mappedSection.nextProcessID = section->nextProcessID;
        //mappedSection.prevProcessID = section->prevProcessID;
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

        GraphNode *startGPUNode = startNode->getCUDALinkRight();
        GraphNode *endGPUNode = endNode->getCUDALinkLeft();

        if ((!startGPUNode || !endGPUNode) || (startGPUNode->getTime() >= endGPUNode->getTime()))
        {
            if (options.verbose >= VERBOSE_BASIC)
                printf("[%u] No CUDA path possible between MPI nodes %s (link %p) and %s (link %p)\n",
                    mpiRank,
                    startNode->getUniqueName().c_str(),
                    startGPUNode,
                    endNode->getUniqueName().c_str(),
                    endGPUNode);
            continue;
        }

        if (options.verbose >= VERBOSE_BASIC)
        {
            printf("[%u] MPI/GPU mapping: %u > %s, %u > %s\n",
                    mpiRank, section->nodeStartID, startGPUNode->getUniqueName().c_str(),
                    section->nodeEndID, endGPUNode->getUniqueName().c_str());

            printf("[%d] Computing GPU critical path (%s, %s)\n", mpiRank,
                    startGPUNode->getUniqueName().c_str(),
                    endGPUNode->getUniqueName().c_str());
        }

        Process::SortedGraphNodeList sectionGPUNodes;
        getCriticalPathIntern(startGPUNode, endGPUNode, GRAPH_CUDA, sectionGPUNodes);

        gpuNodes.insert(gpuNodes.end(), sectionGPUNodes.begin(), sectionGPUNodes.end());
    }
}

void CDMRunner::findLastMpiNode(GraphNode **node)
{
    uint64_t lastMpiNodeTime = 0;
    int lastMpiRank = mpiRank;
    uint64_t nodeTimes[analysis.getMPIAnalysis().getMPISize()];
    *node = NULL;

    GraphNode *myLastMpiNode = analysis.getLastGraphNode(GRAPH_MPI);
    if (myLastMpiNode)
        lastMpiNodeTime = myLastMpiNode->getTime();

    MPI_CHECK(MPI_Allgather(&lastMpiNodeTime, 1, MPI_INTEGER8, nodeTimes,
            1, MPI_INTEGER8, MPI_COMM_WORLD));

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

    Graph *mpiGraph = analysis.getGraph(GRAPH_MPI);
    uint32_t cpCtrId = analysis.getCtrTable().getCtrId(CTR_CRITICALPATH);
    uint32_t sendBfr[BUFFER_SIZE];
    uint32_t recvBfr[BUFFER_SIZE];

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
                Edge *activityEdge = analysis.getEdge(currentNode->getGraphPair().first,
                        currentNode->getGraphPair().second);

                /* change process for blocking edges */
                if (activityEdge->isBlocking())
                {
                    /* make myself a new slave */
                    isMaster = false;

                    /* create section */
                    if (currentNode != sectionEndNode)
                    {
                        MPIAnalysis::CriticalPathSection section;
                        section.processID = currentNode->getProcessId();
                        section.nodeStartID = currentNode->getId();
                        section.nodeEndID = sectionEndNode->getId();
                        sectionsList.push_back(section);
                    }
                    sectionEndNode = NULL;

                    /* commnicate with slaves to decide new master */
                    GraphNode *commMaster = currentNode->getGraphPair().first;

                    if (options.verbose >= VERBOSE_ANNOY)
                    {
                        printf("[%u]  found waitstate for %s (%f), changing process at %s\n",
                                mpiRank, currentNode->getUniqueName().c_str(),
                                analysis.getRealTime(currentNode->getTime()),
                                commMaster->getUniqueName().c_str());
                    }

                    std::set<uint32_t> mpiPartners = analysis.getMPIAnalysis().getMpiPartners(commMaster);
                    for (std::set<uint32_t>::const_iterator iter = mpiPartners.begin();
                            iter != mpiPartners.end(); ++iter)
                    {
                        /* communicate with all slaves to find new master */
                        uint32_t commMpiRank = analysis.getMPIAnalysis().getMPIRank(*iter);
                        if (commMpiRank == (uint32_t) mpiRank)
                            continue;

                        sendBfr[0] = commMaster->getId();
                        sendBfr[1] = commMaster->getProcessId();
                        sendBfr[2] = NO_MSG;

                        if (options.verbose >= VERBOSE_ANNOY)
                        {
                            printf("[%u]  testing remote MPI worker %u for remote edge to my node %u on process %u\n",
                                    mpiRank, commMpiRank, sendBfr[0], sendBfr[1]);
                        }
                        MPI_Request request;
                        MPI_CHECK(MPI_Isend(sendBfr, BUFFER_SIZE, MPI_INTEGER4, commMpiRank, 0,
                                MPI_COMM_WORLD, &request));
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

                    std::set<uint32_t> mpiPartners = analysis.getMPIAnalysis().getMPICommGroup(0).procs;
                    for (std::set<uint32_t>::const_iterator iter = mpiPartners.begin();
                            iter != mpiPartners.end(); ++iter)
                    {
                        int commMpiRank = analysis.getMPIAnalysis().getMPIRank(*iter);
                        if (commMpiRank == mpiRank)
                            continue;

                        MPI_CHECK(MPI_Send(sendBfr, BUFFER_SIZE, MPI_INTEGER4, commMpiRank, 0, MPI_COMM_WORLD));
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
            MPI_CHECK(MPI_Recv(recvBfr, BUFFER_SIZE, MPI_INTEGER4, MPI_ANY_SOURCE,
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
                printf("[%u]  tested by remote MPI worker %u for remote edge to its node %u on process %u\n",
                        mpiRank, status.MPI_SOURCE, recvBfr[0], recvBfr[1]);
            }
            GraphNode *localSrcNode =
                    analysis.getMPIAnalysis().getRemoteMPIEdgeLocalNode(recvBfr[0], recvBfr[1]);

            /* check if the activity is a wait state */
            if (localSrcNode)
            {
                GraphNode::GraphNodePair &slaveActivity = localSrcNode->getGraphPair();
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

void CDMRunner::createSection(SectionsList *sections,
        GraphNode* start, GraphNode* end, uint32_t prevProcessId,
        uint32_t currentProcessId, uint32_t nextProcessId)
{
    MPIAnalysis &mpiAnalysis = analysis.getMPIAnalysis();

    uint32_t sectionMPIRank = mpiAnalysis.getMPIRank(currentProcessId);

    if (options.verbose >= VERBOSE_BASIC)
    {

        printf("[0] creating section [%s (%f), %s (%f)] for process %u on rank %u\n",
                start->getUniqueName().c_str(),
                analysis.getRealTime(start->getTime()),
                end->getUniqueName().c_str(),
                analysis.getRealTime(end->getTime()),
                currentProcessId,
                sectionMPIRank);
    }

    MPIAnalysis::CriticalPathSection section;
    section.processID = currentProcessId;
    //section.prevProcessID = prevProcessId;
    //section.nextProcessID = nextProcessId;
    // convert the original IDs for remote MPI nodes 
    section.nodeStartID = mpiAnalysis.getRemoteNodeInfo(start).nodeID;
    section.nodeEndID = mpiAnalysis.getRemoteNodeInfo(end).nodeID;

    sections[sectionMPIRank].push_back(section);
}

#ifdef MPI_CP_MERGE

void CDMRunner::mergeMPIGraphs()
{
    if (mpiSize > 1)
    {
        if (options.verbose >= VERBOSE_BASIC)
            printf("[%u] merging MPI graphs\n", mpiRank);

        analysis.mergeMPIGraphs();
    }
}

void CDMRunner::distributeCriticalPathSections(Process::SortedGraphNodeList& mpiNodes,
        Process::SortedGraphNodeList& gpuNodes,
        MPIAnalysis::CriticalSectionsMap & sectionsMap)
{
    MPIAnalysis &mpiAnalysis = analysis.getMPIAnalysis();
    SectionsList sections[mpiAnalysis.getMPISize()];

    uint32_t currentProcessId = 0;
    uint32_t prevProcessId = 0;
    GraphNode *sectionStart = NULL, *lastNode = NULL;

    /* split the critical path in sections for each process */
    for (Process::SortedGraphNodeList::const_iterator iter = mpiNodes.begin();
            iter != mpiNodes.end();)
    {
        GraphNode *node = *iter;
        Process::SortedGraphNodeList::const_iterator next = ++iter;

        if (node->isAtomic())
            continue;

        if (!currentProcessId)
        {
            sectionStart = node;
            currentProcessId = node->getProcessId();
            prevProcessId = currentProcessId;
        } else
        {
            // critical path changed the process
            if (node->getProcessId() != currentProcessId)
            {
                // add section
                if (sectionStart != lastNode)
                {
                    createSection(sections, sectionStart, lastNode, prevProcessId,
                            currentProcessId, node->getProcessId());
                }

                sectionStart = node;
                prevProcessId = currentProcessId;
                currentProcessId = node->getProcessId();
            }
        }

        lastNode = node;
        iter = next;
    }

    // create final section
    if (sectionStart != lastNode)
    {
        createSection(sections, sectionStart, lastNode, currentProcessId,
                currentProcessId, currentProcessId);
    }

    /* send each rank the number of its sections and the actual sections if any */
    for (uint32_t rank = 1; rank < mpiAnalysis.getMPISize(); ++rank)
    {
        uint32_t numSections = sections[rank].size();
        if (options.verbose >= VERBOSE_BASIC)
            printf("[%u] has %u critical path sections\n", rank, numSections);

        MPI_CHECK(MPI_Send(&numSections, 1, MPI_INTEGER4, rank, 0, MPI_COMM_WORLD));

        if (numSections > 0)
        {
            MPI_CHECK(MPI_Send(sections[rank].data(),
                    (sizeof (MPIAnalysis::CriticalPathSection) / sizeof (uint32_t)) * numSections,
                    MPI_INTEGER4, rank, 0, MPI_COMM_WORLD));
        }
    }

    if (options.verbose >= VERBOSE_BASIC)
    {

        printf("[%u] has %lu critical path sections\n",
                analysis.getMPIRank(), sections[0].size());
    }

    getCriticalGPUSections(sections[0].data(), sections[0].size(), gpuNodes, sectionsMap);
}

void CDMRunner::distributeCriticalMPINodes(Process::SortedGraphNodeList & mpiNodes)
{
    MPIAnalysis &mpiAnalysis = analysis.getMPIAnalysis();
    MPINodeList nodesPerRank[mpiAnalysis.getMPISize()];
    uint32_t my_mpi_rank = mpiAnalysis.getMPIRank();

    /* split the critical MPI nodes in chunks for each process */
    for (Process::SortedGraphNodeList::const_iterator iter = mpiNodes.begin();
            iter != mpiNodes.end(); ++iter)
    {
        GraphNode *node = *iter;
        if (node->isEnter() || node->isLeave())
        {
            uint32_t mpi_rank = mpiAnalysis.getMPIRank(node->getProcessId());

            if (mpi_rank != my_mpi_rank)
            {
                MPIAnalysis::ProcessNodePair pNodePair;
                pNodePair.nodeID = mpiAnalysis.getRemoteNodeInfo(node).nodeID;
                pNodePair.processID = node->getProcessId();
                nodesPerRank[mpi_rank].push_back(pNodePair);
            }
        }
    }

    /* send each rank the number of its critical MPI nodes and the actual nodes if any */
    for (uint32_t rank = 1; rank < mpiAnalysis.getMPISize(); ++rank)
    {
        uint32_t numNodes = nodesPerRank[rank].size();
        if (options.verbose >= VERBOSE_BASIC)
            printf("[%u] has %u critical MPI nodes\n", rank, numNodes);

        MPI_CHECK(MPI_Send(&numNodes, 1, MPI_INTEGER4, rank, 0, MPI_COMM_WORLD));

        if (numNodes > 0)
        {

            MPI_CHECK(MPI_Send(nodesPerRank[rank].data(), 2 * numNodes,
                    MPI_INTEGER4, rank, 0, MPI_COMM_WORLD));
        }
    }
}

void CDMRunner::receiveCriticalPathSections(Process::SortedGraphNodeList& gpuNodes,
        MPIAnalysis::CriticalSectionsMap & sectionsMap)
{
    /* receive the number of this rank's critical path sections */
    uint32_t numSections = 0;
    MPI_Status status;
    MPI_CHECK(MPI_Recv(&numSections, 1, MPI_INTEGER4, 0, 0, MPI_COMM_WORLD, &status));

    if (numSections > 0)
    {
        /* receive sections */
        MPIAnalysis::CriticalPathSection sections[numSections];
        MPI_CHECK(MPI_Recv(sections,
                (sizeof (MPIAnalysis::CriticalPathSection) / sizeof (uint32_t)) * numSections,
                MPI_INTEGER4, 0, 0, MPI_COMM_WORLD, &status));

        /* process sections */
        if (options.verbose >= VERBOSE_BASIC)
        {

            printf("[%u] %u critical path sections\n",
                    analysis.getMPIRank(), numSections);
        }
        getCriticalGPUSections(sections, numSections, gpuNodes, sectionsMap);
    }
}

void CDMRunner::receiveCriticalMPINodes()
{
    /* receive the number of this rank's critical MPI nodes */
    uint32_t numNodes = 0;
    MPI_Status status;
    MPI_CHECK(MPI_Recv(&numNodes, 1, MPI_INTEGER4, 0, 0, MPI_COMM_WORLD, &status));

    if (numNodes > 0)
    {
        /* receive sections */
        MPIAnalysis::ProcessNodePair mpiNodeIds[numNodes];
        MPI_CHECK(MPI_Recv(mpiNodeIds, 2 * numNodes, MPI_INTEGER4, 0, 0,
                MPI_COMM_WORLD, &status));

        for (uint32_t i = 0; i < numNodes; ++i)
        {
            Process *p = analysis.getProcess(mpiNodeIds[i].processID);
            Process::SortedNodeList &pNodes = p->getNodes();

            for (Process::SortedNodeList::const_iterator iter = pNodes.begin();
                    iter != pNodes.end(); ++iter)
            {
                Node *node = *iter;
                if (node->getId() == mpiNodeIds[i].nodeID)
                {
                    node->setCounter(CTR_CRITICALPATH, 1);

                    break;
                }
            }
        }
    }
}

#endif

void CDMRunner::runAnalysis(GraphNodeType g)
{
    if (g == GRAPH_CUDA)
        printf("[%u] Running analysis: CUDA\n", analysis.getMPIRank());
    else
        printf("[%u] Running analysis: MPI\n", analysis.getMPIRank());

    analysis.removeRules();

    if (g == GRAPH_CUDA)
    {
        analysis.addRule(new KernelLaunchRule(9));
        analysis.addRule(new BlameSyncRule(1));
        analysis.addRule(new BlameKernelRule(1));
        analysis.addRule(new LateSyncRule(1));
        analysis.addRule(new EventLaunchRule(1));
        analysis.addRule(new EventSyncRule(1));
        analysis.addRule(new EventQueryRule(1));
        analysis.addRule(new StreamWaitRule(1));
    } else
    {
        analysis.addRule(new RecvRule(1));
        analysis.addRule(new SendRule(1));
        analysis.addRule(new CollectiveRule(1));
        analysis.addRule(new SendRecvRule(1));
    }

    Process::SortedNodeList allNodes;
    analysis.getAllNodes(allNodes);

    size_t ctr = 0, last_ctr = 0;
    size_t num_nodes = allNodes.size();

    for (Process::SortedNodeList::const_iterator nIter = allNodes.begin();
            nIter != allNodes.end(); ++nIter)
    {
        VT_TRACER("analysis iter");
        Node *node = *nIter;
        ctr++;

        if (!node->isGraphNode() || !node->isLeave())
            continue;

        if ((g == GRAPH_MPI && (!node->isMPI())) || (g == GRAPH_CUDA && (node->isMPI())))
            continue;

        analysis.applyRules(node, options.verbose >= VERBOSE_BASIC);

        if ((analysis.getMPIRank() == 0) && (ctr - last_ctr > num_nodes / 10))
        {

            printf("[%u] %3.2f%% ", analysis.getMPIRank(), 100.0 * (double) ctr / (double) num_nodes);
            last_ctr = ctr;
        }
    }
    printf("\n");

#ifdef DEBUG
    analysis.runSanityCheck(analysis.getMPIRank());
#endif
}

void CDMRunner::printAllActivities(const Process::SortedGraphNodeList & criticalEvents,
        uint64_t globalCPLength)
{
    std::map<uint32_t, ActivityGroup> activityGroupMap;
    mergeActivityGroups(criticalEvents, activityGroupMap, false);

    if (analysis.getMPIRank() == 0)
    {
        printf("\n%50s %10s %10s %11s %12s %21s %9s\n", "Activity Group",
                "Instances", "Time", "Time on CP", "Fraction CP", "Fraction Global Blame", "Rating");

        std::set<ActivityGroup, ActivityGroupCompare> sortedActivityGroups;

        for (std::map<uint32_t, ActivityGroup>::iterator iter = activityGroupMap.begin();
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

        for (std::set<ActivityGroup, ActivityGroupCompare>::const_iterator iter =
                sortedActivityGroups.begin(); iter != sortedActivityGroups.end(); ++iter)
        {
            printf("%50s %10u %10f %11f %11.2f%% %20.2f%%  %7.6f\n",
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
