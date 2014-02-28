/* 
 * File:   StreamWaitRule.hpp
 * Author: felix
 *
 * Created on 19. Juni 2013, 10:45
 */

#ifndef STREAMWAITRULE_HPP
#define	STREAMWAITRULE_HPP

#include "AbstractRule.hpp"

namespace cdm
{

    class StreamWaitRule : public AbstractRule
    {
    public:

        StreamWaitRule(int priority) :
        AbstractRule("StreamWaitRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            VT_TRACER("StreamWaitRule");
            if (!node->isGraphNode() || !node->isLeave())
                return false;

            // applied at streamWaitEvent leave
            if (node->isEventNode() && node->isCUDAStreamWaitEvent())
            {
                uint64_t referencedDevWaitProc = node->getReferencedProcessId();
                if (!referencedDevWaitProc)
                    throw RTException("Stream wait %s does not reference any device process",
                        node->getUniqueName().c_str());

                EventNode *swEventNode = (EventNode*) node;
                swEventNode->setLink(analysis->getLastEventLaunchLeave(swEventNode->getEventId()));
                
                uint64_t eventProcessId = analysis->getEventProcessId(swEventNode->getEventId());
                if (!eventProcessId)
                    throw RTException("Could not find device process ID for event %u from %s",
                        swEventNode->getEventId(), swEventNode->getUniqueName().c_str());

                if (swEventNode->getLink() && (referencedDevWaitProc != eventProcessId))
                    analysis->addStreamWaitEvent(referencedDevWaitProc, swEventNode);
                else
                {
                    // found unnecessary streamWaitEvent call
                    // we could blame this here
                    //printf(" * Ignoring unnecessary stream wait event node %s\n",
                    //      eNode->getUniqueName().c_str());
                }
                return true;
            }

            // ... and applied at kernel leave
            if (node->isCUDAKernel())
            {
                // get the complete execution of this kernel
                GraphNode::GraphNodePair waitingKernel = ((GraphNode*) node)->getGraphPair();

                bool ruleMatched = false;
                bool insertWaitState = false;
                std::set<GraphNode*> processedSyncKernelLeaves;

                uint64_t waitStateEnterTime = std::numeric_limits<uint64_t>::max();
                GraphNode *lastSyncKernelLeave = NULL;

                // get launch for this (waiting) kernel
                GraphNode *waitingKernelLaunchEnter = (GraphNode*) waitingKernel.first->getLink();
                if (!waitingKernelLaunchEnter)
                    throw RTException("Kernel %s has no matching kernel launch",
                        waitingKernel.first->getUniqueName().c_str());

                // We have to manage all streamWaitEvents that may reference this kernel's
                // device stream, processed in chronological order (oldest first).
                while (true)
                {
                    // find the oldest streamWaitEvent that references this (waiting) device process
                    EventNode *streamWaitLeave = analysis->getFirstStreamWaitEvent(
                            node->getProcessId());
                    if (!streamWaitLeave)
                        break;

                    // if the streamWaitEvent is after this (waiting) kernel was launched,
                    // it's the wrong streamWaitEvent and we stop processing streamWaitEvents.
                    GraphNode *streamWaitEnter = streamWaitLeave->getGraphPair().first;
                    if (streamWaitEnter->getTime() > waitingKernelLaunchEnter->getTime())
                        break;

                    // remove from queue
                    analysis->consumeFirstStreamWaitEvent(node->getProcessId());

                    // find the eventLaunch for this event
                    EventNode *eventLaunchLeave = (EventNode*)streamWaitLeave->getLink();
                    if (!eventLaunchLeave)
                    {
                        //throw RTException("Found no event record for event %u",
                        //        streamWaitLeave->getEventId());
                        printf(" * Ignoring stream wait event %s without matching event record for event %u\n",
                                streamWaitLeave->getUniqueName().c_str(),
                                streamWaitLeave->getEventId());
                        break;
                    }
                    
                    // find the device process where the event of streamWaitEvent is enqueued
                    uint64_t swEventRefDevProc = eventLaunchLeave->getReferencedProcessId();
                    if (!swEventRefDevProc)
                        break;

                    // find closest kernelLaunch leave before this eventLaunch
                    GraphNode *launchLeave = (GraphNode*)eventLaunchLeave->getLink();
                    if (!launchLeave)
                        break;

                    GraphNode *syncKernelEnter =
                            (GraphNode*) launchLeave->getGraphPair().first->getLink();
                    if (!syncKernelEnter)
                    {
                        ErrorUtils::getInstance().throwError("Depending kernel %s (%f) started before kernel from %s (%f) started"
                                " (event id = %u, recorded at %f, streamWaitEvent %s)",
                                node->getUniqueName().c_str(), analysis->getRealTime(node->getTime()),
                                launchLeave->getUniqueName().c_str(), analysis->getRealTime(launchLeave->getTime()),
                                streamWaitLeave->getEventId(),
                                analysis->getRealTime(eventLaunchLeave->getTime()),
                                streamWaitLeave->getUniqueName().c_str());
                        return false;
                    }

                    GraphNode *syncKernelLeave = syncKernelEnter->getGraphPair().second;
                    if (!syncKernelLeave)
                    {
                        ErrorUtils::getInstance().throwError("Depending kernel %s (%f) started before kernel from %s (%f) finished",
                                node->getUniqueName().c_str(), analysis->getRealTime(node->getTime()),
                                launchLeave->getUniqueName().c_str(), analysis->getRealTime(launchLeave->getTime()));
                        return false;
                    }

                    // don't add multiple dependencies to the same (syncing) kernel
                    if (processedSyncKernelLeaves.find(syncKernelLeave) !=
                            processedSyncKernelLeaves.end())
                    {
                        break;
                    }

                    processedSyncKernelLeaves.insert(syncKernelLeave);

                    // add dependency
                    analysis->newEdge(syncKernelLeave, waitingKernel.first, EDGE_CAUSES_WAITSTATE);

                    // insert wait state only if launch of next (waiting) kernel
                    // is before the blocking kernel finishes
                    if (waitingKernelLaunchEnter->getTime() < syncKernelLeave->getTime())
                    {
                        // set counters
                        syncKernelEnter->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                                syncKernelLeave->getTime() - waitingKernelLaunchEnter->getTime());

                        waitStateEnterTime = std::min(waitStateEnterTime,
                                waitingKernelLaunchEnter->getTime());
                        if (!lastSyncKernelLeave ||
                                (syncKernelLeave->getTime() > lastSyncKernelLeave->getTime()))
                        {
                            lastSyncKernelLeave = syncKernelLeave;
                        }
                        insertWaitState = true;
                    }

                    ruleMatched = true;
                }

                if (insertWaitState)
                {
                    // get last leave node on this device process
                    Process *waitingDevProc = analysis->getProcess(node->getProcessId());
                    Process::SortedNodeList& nodes = waitingDevProc->getNodes();

                    GraphNode *lastLeaveNode = NULL;
                    for (Process::SortedNodeList::const_reverse_iterator rIter = nodes.rbegin();
                            rIter != nodes.rend(); ++rIter)
                    {
                        Node *n = (*rIter);
                        if (n->isMPI())
                            continue;

                        if (n->getTime() <= node->getTime() && n != node && n->isGraphNode() && n->isLeave() && n->isCUDAKernel())
                        {
                            uint64_t lastLeaveNodeTime = n->getTime();
                            lastLeaveNode = (GraphNode*) n;
                            if (lastLeaveNodeTime > waitStateEnterTime)
                                waitStateEnterTime = lastLeaveNodeTime;
                            break;
                        }
                    }

                    // add wait state if a preceeding leave node in this stream has been found
                    // which ends earlier than the current kernel started
                    if (lastLeaveNode && (waitStateEnterTime < waitingKernel.first->getTime()))
                    {
                        Edge *kernelKernelEdge = analysis->getEdge(
                                lastLeaveNode, waitingKernel.first);
                        if (!kernelKernelEdge)
                        {
                            ErrorUtils::getInstance().throwError("Did not find expected edge [%s (p %u), %s (p %u)]",
                                    lastLeaveNode->getUniqueName().c_str(),
                                    lastLeaveNode->getProcessId(),
                                    waitingKernel.first->getUniqueName().c_str(),
                                    waitingKernel.first->getProcessId());
                            return false;
                        }

                        GraphNode *waitEnter = analysis->addNewGraphNode(
                                waitStateEnterTime,
                                waitingDevProc,
                                NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_ENTER, CUDA_WAITSTATE);
                        GraphNode *waitLeave = analysis->addNewGraphNode(
                                lastSyncKernelLeave->getTime(),
                                waitingDevProc,
                                NAME_WAITSTATE,
                                PARADIGM_CUDA, RECORD_LEAVE, CUDA_WAITSTATE);

                        analysis->newEdge(lastLeaveNode, waitEnter);
                        analysis->newEdge(waitEnter, waitLeave, EDGE_IS_BLOCKING);
                        analysis->newEdge(waitLeave, waitingKernel.first);

                        // set counters
                        waitEnter->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 
                                1);

                        // add dependency to all sync kernel leave nodes
                        // (some dependencies can become reverse edges during optimization)
                        for (std::set<GraphNode*>::const_iterator gIter =
                                processedSyncKernelLeaves.begin();
                                gIter != processedSyncKernelLeaves.end(); ++gIter)
                        {
                            ///\todo check if this should have EDGE_CAUSES_WAITSTATE property
                            analysis->newEdge(*gIter, waitLeave);
                        }
                    }
                }

                return ruleMatched;
            }

            return false;
        }
    };
}

#endif	/* STREAMWAITRULE_HPP */

