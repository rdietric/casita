#define __STDC_LIMIT_MACROS

#ifdef VAMPIR
#include <vt_user.h>
#endif

#include <stdlib.h>
#include <string.h>

#include "common.hpp"
#include "CDMRunner.hpp"

using namespace cdm;
using namespace cdm::io;

void parseCmdLine(int argc, char **argv, CDMRunner::ProgramOptions &options)
{
    int i = 0;

    if (argc < 2)
    {
        printf("Too few arguments. Usage %s <otf> [-v <level>|-a|-o <kernel>|-otf <otf>|-p|-dot|--mem-limit <MB>]\n",
                argv[0]);
        exit(-1);
    }

    if (argc > 2)
    {
        for (i = 0; i < argc; ++i)
        {
            if ((strcmp(argv[i], "-v") == 0) && (i < argc - 1))
            {
                options.verbose = atoi(argv[i + 1]);
                i++;
            }

            if (strcmp(argv[i], "-p") == 0)
                options.printCriticalPath = true;

            if (strcmp(argv[i], "-a") == 0)
                options.mergeActivities = true;

            if ((strcmp(argv[i], "-o") == 0) && (i < argc - 1))
            {
                options.optimize = 1;
                options.optKernels = argv[i + 1];
                i++;
            }

            if ((strcmp(argv[i], "--otf") == 0) && (i < argc - 1))
            {
                options.createOTF = 1;
                options.outOtfFile = argv[i + 1];

                if (strlen(options.outOtfFile) < 5 || strstr(options.outOtfFile, ".otf") == NULL)
                {
                    printf("The filename %s does not seem to be a valid .otf file.\n",
                            options.outOtfFile);
                    exit(-1);
                }
                i++;
            }

            if ((strcmp(argv[i], "--max-events") == 0) && (i < argc - 1))
            {
                options.maxEvents = atoi(argv[i + 1]);
                i++;
            }

            if ((strcmp(argv[i], "--mem-limit") == 0) && (i < argc - 1))
            {
                options.memLimit = atoi(argv[i + 1]) * 1024;
                i++;
            }

            if (strcmp(argv[i], "--dot") == 0)
                options.createGraphs = true;
            
            if (strcmp(argv[i], "--no-errors") == 0)
                options.noErrors = true;
        }
    }

    options.filename = argv[1];
    if (strlen(options.filename) < 5 || strstr(options.filename, ".otf") == NULL)
    {
        printf("The filename %s does not seem to be a valid .otf file.\n",
                options.filename);
        exit(-1);
    }
}

void computeCriticalPaths(CDMRunner *runner, CDMRunner::ProgramOptions& options, uint32_t mpiRank)
{
    Process::SortedGraphNodeList localCriticalPathNodes;
    Process::SortedGraphNodeList mpiCriticalPathNodes;

#ifdef MPI_CP_MERGE
    // merging strategy
    runner->mergeMPIGraphs();
#endif

    runner->getCriticalPath(localCriticalPathNodes, mpiCriticalPathNodes);

    if (options.createGraphs)
    {
        std::set<GraphNode*> cnodes;
        cnodes.insert(localCriticalPathNodes.begin(), localCriticalPathNodes.end());
        std::stringstream local_graph_filename;
        local_graph_filename << runner->getAnalysis().getMPIRank() << "_" <<
                GRAPH_LOCAL_FILENAME;

        if (mpiRank == 0)
        {
            cnodes.insert(mpiCriticalPathNodes.begin(), mpiCriticalPathNodes.end());
            Graph *mpiGraph = runner->getAnalysis().getGraph().getSubGraph(PARADIGM_MPI);
            mpiGraph->saveToFile(GRAPH_MPI_FILENAME, &cnodes);
            delete mpiGraph;
        }

        Graph *localGraph = runner->getAnalysis().getGraph().getSubGraph(PARADIGM_COMPUTE_LOCAL);
        localGraph->saveToFile(local_graph_filename.str().c_str(), &cnodes);
        delete localGraph;

        runner->getAnalysis().getGraph().saveToFile(GRAPH_GLOBAL_FILENAME, &cnodes);
    }
}

void testResources(int mpiRank)
{
    int memUsage = CDMRunner::getCurrentResources();

    printf("[%d] memusage: %d kByte\n", mpiRank, memUsage);
}

int main(int argc, char **argv)
{
    int mpiRank = 0;
    int mpiSize = 0;


#ifdef VAMPIR
    unsigned int mid = VT_MARKER_DEF("REGION", VT_MARKER_TYPE_HINT);
    VT_MARKER(mid, "START");
#endif

    MPI_CHECK(MPI_Init(&argc, &argv));

    MPI_CHECK(MPI_Comm_rank(MPI_COMM_WORLD, &mpiRank));
    MPI_CHECK(MPI_Comm_size(MPI_COMM_WORLD, &mpiSize));
    testResources(mpiRank);

    if (mpiRank == 0)
        printf("[0] Running with %d analysis processes\n", mpiSize);

    CDMRunner::ProgramOptions options;
    memset(&options, 0, sizeof (CDMRunner::ProgramOptions));
    options.memLimit = 4 * 1024 * 1024; // 4GByte
    parseCmdLine(argc, argv, options);

    CDMRunner *runner = new CDMRunner(mpiRank, mpiSize, options);

    runner->readOTF(options.filename);
    testResources(mpiRank);

#ifdef VAMPIR
    VT_MARKER(mid, "READING finished");
#endif

    runner->runAnalysis(PARADIGM_CUDA);
    runner->runAnalysis(PARADIGM_OMP);
    runner->runAnalysis(PARADIGM_MPI);
    testResources(mpiRank);

#ifdef VAMPIR
    VT_MARKER(mid, "ANALYSIS finished");
#endif

    uint64_t timerResolution = runner->getAnalysis().getTimerResolution();
    GraphNode *firstNode = runner->getAnalysis().getFirstTimedGraphNode(PARADIGM_CUDA);

    uint64_t oldRuntime = runner->getAnalysis().getLastGraphNode(PARADIGM_CUDA)->getTime();
    if (firstNode)
        oldRuntime -= firstNode->getTime();

    printf("[%u] Old CUDA runtime: %f ms\n",
            mpiRank, 1000.0 * (double) oldRuntime / (double) timerResolution);

    MPI_Barrier(MPI_COMM_WORLD);

    computeCriticalPaths(runner, options, mpiRank);
    testResources(mpiRank);

#ifdef VAMPIR
    VT_MARKER(mid, "CRITICAL_PATH finished");
#endif

    if (options.optimize && (runner->getAnalysis().getActivities().size() > 0))
    {
        runner->getAnalysis().reset();
        uint64_t newRuntime = runner->runOptimization(options.optKernels);

#ifdef VAMPIR
        VT_MARKER(mid, "OPTIMIZATION finished");
#endif

        if (newRuntime != 0)
        {
            if (firstNode)
                newRuntime -= firstNode->getTime();

            printf("[%u] New CUDA runtime: %.4f ms (%.2f%% faster)\n",
                    mpiRank, 1000.0 * (double) newRuntime / (double) timerResolution,
                    100.0 - (double) newRuntime * 100.0 / (double) oldRuntime);

            computeCriticalPaths(runner, options, mpiRank);
        }
        
        testResources(mpiRank);
    }

    /* create OTF with wait states and critical path */
    if (options.createOTF)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        printf("[%u] Writing result to %s\n", mpiRank, options.outOtfFile);
        runner->getAnalysis().saveParallelAllocationToFile(options.outOtfFile,
                options.filename, false, options.verbose >= VERBOSE_ANNOY);
    }

    delete runner;

    MPI_CHECK(MPI_Finalize());
    return 0;
}
