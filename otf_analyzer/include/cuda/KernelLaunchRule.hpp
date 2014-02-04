/* 
 * File:   KernelLaunchRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef KERNELLAUNCHRULE_HPP
#define	KERNELLAUNCHRULE_HPP

#include "AbstractRule.hpp"

namespace cdm
{

    class KernelLaunchRule : public AbstractRule
    {
    public:

        KernelLaunchRule(int priority) :
        AbstractRule("KernelLaunchRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("KernelLaunchRule");
            
            // applied at kernel leave
            if (!node->isCUDAKernel() || !node->isLeave())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair kernel = ((GraphNode*) node)->getGraphPair();

            // find the process which launched this kernel and consume the launch event
            uint32_t kernelProcessId = node->getProcessId();

            GraphNode *launchEnterEvent = analysis->consumePendingKernelLaunch(kernelProcessId);

            if (!launchEnterEvent)
                throw RTException("Found kernel %s without matching kernel launch",
                    node->getUniqueName().c_str());

            launchEnterEvent->setLink(kernel.first);
            kernel.first->setLink(launchEnterEvent);
            
            // add pending kernel
            analysis->getProcess(kernelProcessId)->addPendingKernel(kernel.second);

            // add dependency
            analysis->newEdge(launchEnterEvent, kernel.first, false);

            return true;
        }
    };

}

#endif	/* KERNELLAUNCHRULE_HPP */

