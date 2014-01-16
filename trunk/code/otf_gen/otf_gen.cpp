#define __STDC_LIMIT_MACROS

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "Generator.hpp"
#include "SingleLateSyncPattern.hpp"
#include "SingleBlameSyncPattern.hpp"
#include "SingleBlameKernelPattern.hpp"
#include "MultiLateSyncPattern.hpp"
#include "CollLateSyncPattern.hpp"
#include "MultiBlameSyncPattern.hpp"
#include "MultiBlameKernelPattern.hpp"
#include "NullStreamPattern.hpp"
#include "EventSyncPattern.hpp"
#include "EventQueryPattern.hpp"
#include "StreamWaitPattern.hpp"

using namespace cdm;

int main(int argc, char **argv)
{
    int i = 0;
    bool verbose = false;
    unsigned int seed = time(NULL);
    const char *pattern = NULL;

    if (argc < 6)
    {
        printf("Too few arguments.\n");
        printf("Usage: otfgen <otf-file> <host-streams> <device-streams> \
<null-stream [0|1]> <blocks> [-v|-s <seed>|-p <pattern>]\n");
        return -1;
    }

    const char *filename = argv[1];
    if (strlen(filename) < 5 || strstr(filename, ".otf") == NULL)
    {
        printf("The filename does not seem to be a valid .otf file.\n");
        return -1;
    }

    size_t numHosts = atoi(argv[2]);
    size_t numDevices = atoi(argv[3]);
    bool hasNullStream = false;
    if (atoi(argv[4]) != 0)
        hasNullStream = true;
    size_t numBlocksGenerated = 0, numBlocks = atoi(argv[5]);

    if (argc > 6)
    {
        for (i = 6; i < argc; ++i)
        {
            if (strcmp(argv[i], "-v") == 0)
                verbose = true;

            if ((i < argc - 1) && strcmp(argv[i], "-p") == 0)
            {
                pattern = argv[i + 1];
                i++;
            }

            if ((i < argc - 1) && strcmp(argv[i], "-seed") == 0)
            {
                seed = atoi(argv[i + 1]);
                i++;
            }
        }
    }

    srand(seed);

    printf("generating '%s' (%lu host processes, %lu device processes, %lu null streams, %lu blocks)...\n",
            filename, numHosts, numDevices, (long unsigned) hasNullStream, numBlocks);

    Generator generator(numHosts, numDevices, hasNullStream);
    if (!pattern || strcmp(pattern, "SingleLateSyncPattern") == 0)
        generator.addPattern(new SingleLateSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "SingleBlameSyncPattern") == 0)
        generator.addPattern(new SingleBlameSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "SingleBlameKernelPattern") == 0)
        generator.addPattern(new SingleBlameKernelPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "CollLateSyncPattern") == 0)
        generator.addPattern(new CollLateSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "MultiLateSyncPattern") == 0)
        generator.addPattern(new MultiLateSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "MultiBlameSyncPattern") == 0)
        generator.addPattern(new MultiBlameSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "MultiBlameKernelPattern") == 0)
        generator.addPattern(new MultiBlameKernelPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "EventSyncPattern") == 0)
        generator.addPattern(new EventSyncPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "EventQueryPattern") == 0)
        generator.addPattern(new EventQueryPattern(numHosts, numDevices, hasNullStream));
    if (!pattern || strcmp(pattern, "StreamWaitPattern") == 0)
        generator.addPattern(new StreamWaitPattern(numHosts, numDevices, hasNullStream));

    if ((!pattern && hasNullStream) || (pattern && strcmp(pattern, "NullStreamPattern") == 0))
        generator.addPattern(new NullStreamPattern(numHosts, numDevices, hasNullStream));

    numBlocksGenerated = generator.generate(numBlocks);

    printf("\ngenerated %lu/%lu blocks\n", numBlocksGenerated, numBlocks);
    if (numBlocksGenerated == 0)
        return 0;

    if (verbose)
        printf("mapping events to functions...\n");
    generator.mapFunctions();

    if (verbose)
        printf("\ncomputing critical path...\n");
    Process::SortedGraphNodeList nodes;
    Allocation::ProcessList procs;
    generator.getProcesses(procs);
    for (Allocation::ProcessList::const_iterator iter = procs.begin();
            iter != procs.end(); ++iter)
    {
        GraphNode *gn = (*iter)->getLastGraphNode();
        if (gn)
            nodes.push_back(gn);
    }

    for (Process::SortedGraphNodeList::const_reverse_iterator iter = nodes.rbegin();
            iter != nodes.rend(); ++iter)
    {
        GraphNode *lastNode = (*iter);
        GraphNode::GraphNodeList criticalPath;
        generator.getCriticalPath(generator.getFirstTimedGraphNode(GRAPH_CUDA),
                lastNode, &criticalPath, GRAPH_CUDA);

        if (criticalPath.size() > 1)
        {
            i = 0;
            for (GraphNode::GraphNodeList::iterator cpNode = criticalPath.begin();
                    cpNode != criticalPath.end(); ++cpNode)
            {
                GraphNode *node = *cpNode;

                if (verbose)
                    printf("[%u] %s\n", i, node->getUniqueName().c_str());
                i++;
            }
            break;
        }
    }

    if (verbose)
        printf("\nadding CPU nodes...\n");
    generator.fillCPUNodes();

    if (verbose)
        printf("\nwriting trace file '%s'...\n", filename);
    
    generator.saveAllocationToFile(filename, false, verbose);

    return 0;
}
