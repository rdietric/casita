/* 
 * File:   BlameKernelRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef BLAMEKERNELRULE_HPP
#define	BLAMEKERNELRULE_HPP

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"

namespace cdm
{

    class BlameKernelRule : public AbstractRule
    {
    public:

        BlameKernelRule(int priority) :
        AbstractRule("BlameKernelRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("BlameKernelRule");
            // applied at sync
            if (!node->isCUDASync() || !node->isGraphNode()  || !node->isLeave())
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
                bool isFirstKernel = true;
                while (true)
                {
                    GraphNode *kernelLeave = deviceProcess->getPendingKernel();
                    if (!kernelLeave)
                        break;

                    GraphNode::GraphNodePair& kernel = kernelLeave->getGraphPair();

                    if ((isFirstKernel && (sync.first->getTime() < kernel.second->getTime()) &&
                            (sync.second->getTime() - kernel.second->getTime() <=
                            syncDeltaTicks)) ||
                            (!isFirstKernel && (sync.first->getTime() < kernel.second->getTime())))
                    {
                        if (isFirstKernel)
                        {
                            analysis->newEdge(kernel.second, sync.second, EDGE_CAUSES_WAITSTATE);
                        }

                        analysis->getEdge(sync.first, sync.second)->makeBlocking();

                        // set counters
                        sync.first->incCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 
                                std::min(sync.second->getTime(), kernel.second->getTime()) -
                                std::max(sync.first->getTime(), kernel.first->getTime()));
                        kernel.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                                std::min(sync.second->getTime(), kernel.second->getTime()) -
                                std::max(sync.first->getTime(), kernel.first->getTime()));

                        ruleResult = true;
                        isFirstKernel = false;
                        deviceProcess->consumePendingKernel();
                    } else
                    {
                        deviceProcess->clearPendingKernels();
                        break;
                    }
                }
            }

            return ruleResult;
        }
    };

}

#endif	/* BLAMEKERNELRULE_HPP */

