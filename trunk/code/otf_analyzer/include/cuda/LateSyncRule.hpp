/* 
 * File:   LateSyncRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef LATESYNCRULE_HPP
#define	LATESYNCRULE_HPP

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"

namespace cdm
{

    class LateSyncRule : public AbstractRule
    {
    public:

        LateSyncRule(int priority) :
        AbstractRule("LateSyncRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("LateSyncRule");
            if (!node->isCUDASync() || !node->isLeave())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair& sync = ((GraphNode*) node)->getGraphPair();

            bool ruleResult = false;
            // find all referenced (device) processes
            Allocation::ProcessList deviceProcesses;
            analysis->getAllDeviceProcesses(deviceProcesses);
            
            for (Allocation::ProcessList::const_iterator pIter = deviceProcesses.begin();
                    pIter != deviceProcesses.end();
                    ++pIter)
            {
                Process *deviceProcess = *pIter;

                if (!sync.first->referencesProcess(deviceProcess->getId()))
                {
                    continue;
                }

                // test that there is a pending kernel leave
                GraphNode *kernelLeave = deviceProcess->getPendingKernel();

                if (kernelLeave && kernelLeave->getTime() <= sync.first->getTime())
                {
                    printf("latesync %s\n", kernelLeave->getUniqueName().c_str());
                    GraphNode *lastLeaveNode = analysis->getLastLeave(sync.second->getTime(),
                            deviceProcess->getId());
                    GraphNode *waitEnter = NULL, *waitLeave = NULL;

                    if (lastLeaveNode && lastLeaveNode->isWaitstate())
                    {
                        if (lastLeaveNode->getTime() == sync.second->getTime())
                            waitLeave = lastLeaveNode;
                        else
                        {
                            waitEnter = analysis->addNewGraphNode(
                                    std::max(lastLeaveNode->getTime(),
                                    sync.first->getTime()),
                                    deviceProcess, NAME_WAITSTATE,
                                    PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE);
                        }
                    } else
                    {
                        waitEnter = analysis->addNewGraphNode(
                                sync.first->getTime(),
                                deviceProcess, NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE);
                    }

                    if (!waitLeave)
                    {
                        waitLeave = analysis->addNewGraphNode(
                                sync.second->getTime(),
                                deviceProcess, NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE);
                    }

                    analysis->newEdge(sync.first, waitEnter, EDGE_CAUSES_WAITSTATE);
                    analysis->newEdge(sync.second, waitLeave);
                    
                    if (sync.first->isCUDAKernel())
                        analysis->newEdge(kernelLeave, sync.first);

                    // set counters
                    sync.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                            sync.second->getTime() - sync.first->getTime());
                    waitEnter->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 
                            sync.second->getTime() - sync.first->getTime());

                    deviceProcess->clearPendingKernels();
                    ruleResult = true;
                }
            }

            return ruleResult;
        }
    };

}

#endif	/* LATESYNCRULE_HPP */

