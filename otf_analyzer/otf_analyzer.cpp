#define __STDC_LIMIT_MACROS

#include <stdlib.h>
#include <string.h>

#include "common.hpp"
#include "Parser.hpp"
#include "CDMRunner.hpp"

using namespace cdm;
using namespace cdm::io;

void computeCriticalPaths(CDMRunner *runner, uint32_t mpiRank)
{
    Process::SortedGraphNodeList localCriticalPathNodes;
    Process::SortedGraphNodeList mpiCriticalPathNodes;

    runner->getCriticalPath(localCriticalPathNodes, mpiCriticalPathNodes);

    if (Parser::getInstance().getProgramOptions().createGraphs)
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
    if (mpiRank == 0)
        printf("[%d] memusage: %d kByte\n", mpiRank, memUsage);
}

int main(int argc, char **argv)
{
    int mpiRank = 0;
    int mpiSize = 0;

    MPI_CHECK(MPI_Init(&argc, &argv));

    MPI_CHECK(MPI_Comm_rank(MPI_COMM_WORLD, &mpiRank));
    MPI_CHECK(MPI_Comm_size(MPI_COMM_WORLD, &mpiSize));
    testResources(mpiRank);

    if (mpiRank == 0)
        printf("[0] Running with %d analysis processes\n", mpiSize);

    if (!Parser::getInstance().init(argc, argv))
        return -1;
    ProgramOptions& options = Parser::getInstance().getProgramOptions();

    CDMRunner *runner = new CDMRunner(mpiRank, mpiSize);

    runner->readOTF();
    testResources(mpiRank);
    
    Process::SortedNodeList allNodes;
    runner->getAnalysis().getAllNodes(allNodes);
    
    runner->runAnalysis(PARADIGM_CUDA, allNodes);
    runner->runAnalysis(PARADIGM_OMP, allNodes);
    runner->runAnalysis(PARADIGM_MPI, allNodes);
    testResources(mpiRank);

    MPI_Barrier(MPI_COMM_WORLD);

    if (mpiRank == 0)
        printf("[%u] Computing the critical path\n", mpiRank);
    computeCriticalPaths(runner, mpiRank);
    testResources(mpiRank);

    /* create OTF with wait states and critical path */
    if (options.createOTF)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        if (mpiRank == 0)
            printf("[%u] Writing result to %s\n", mpiRank, options.outOtfFile.c_str());

        runner->getAnalysis().saveParallelAllocationToFile(options.outOtfFile,
                options.filename, false, options.verbose >= VERBOSE_ANNOY);
    }

    delete runner;

    MPI_CHECK(MPI_Finalize());
    return 0;
}
