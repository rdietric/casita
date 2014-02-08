/* 
 * File:   BlameSyncRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef BLAMESYNCRULE_HPP
#define	BLAMESYNCRULE_HPP

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"

namespace cdm
{

    class BlameSyncRule : public AbstractRule
    {
    public:

        BlameSyncRule(int priority) :
        AbstractRule("BlameSyncRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            if (!node->isCUDASync() || !node->isLeave())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair& sync = ((GraphNode*) node)->getGraphPair();

            uint64_t syncDeltaTicks = analysis->getDeltaTicks();

            bool ruleResult = false;
            // find all referenced (device) processes
            Allocation::ProcessList deviceProcesses;
            analysis->getAllDeviceProcesses(deviceProcesses);
            
            for (Allocation::ProcessList::const_iterator pIter = deviceProcesses.begin();
                    pIter != deviceProcesses.end(); ++pIter)
            {
                Process *deviceProcess = *pIter;

                if (!sync.first->referencesProcess(deviceProcess->getId()))
                    continue;

                // test that there is a pending kernel (leave)
                GraphNode *kernelLeave = deviceProcess->getPendingKernel();
                if (!kernelLeave)
                    break;

                GraphNode::GraphNodePair & kernel = kernelLeave->getGraphPair();

                if ((sync.first->getTime() < kernel.second->getTime()) &&
                        (sync.second->getTime() - kernel.second->getTime() >
                        syncDeltaTicks))
                {
                    GraphNode *lastLeaveNode = analysis->getLastLeave(
                            sync.second->getTime(), deviceProcess->getId());
                    GraphNode *waitEnter = NULL, *waitLeave = NULL;

                    if (lastLeaveNode && lastLeaveNode->isWaitstate())
                    {
                        if (lastLeaveNode->getTime() == sync.second->getTime())
                            waitLeave = lastLeaveNode;
                        else
                        {
                            waitEnter = analysis->addNewGraphNode(
                                    std::max(lastLeaveNode->getTime(),
                                    kernel.second->getTime()),
                                    deviceProcess, NAME_WAITSTATE,
                                    PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE,
                                    NULL);
                        }
                    } else
                    {
                        waitEnter = analysis->addNewGraphNode(
                                kernel.second->getTime(),
                                deviceProcess, NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE,
                                NULL);
                    }

                    if (!waitLeave)
                    {
                        waitLeave = analysis->addNewGraphNode(
                                sync.second->getTime(),
                                deviceProcess, NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE,
                                NULL);
                    }

                    analysis->newEdge(sync.second, waitLeave, EDGE_CAUSES_WAITSTATE);

                    // set counters
                    sync.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                            sync.second->getTime() - kernel.second->getTime());
                    waitEnter->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);

                    deviceProcess->clearPendingKernels();
                    ruleResult = true;
                }
            }

            return ruleResult;
        }
    };

}

#endif	/* BLAMESYNCRULE_HPP */

