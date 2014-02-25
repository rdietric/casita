/* 
 * File:   BlameDistribution.hpp
 * Author: felix
 *
 * Created on 10. Februar 2014, 08:58
 */

#ifndef BLAMEDISTRIBUTION_HPP
#define	BLAMEDISTRIBUTION_HPP

#include "AnalysisEngine.hpp"

namespace cdm
{

    typedef struct
    {
        GraphNode::GraphNodeList list;
        uint64_t waitStateTime;
    } ProcessWalkInfo;

    static void distributeBlame(AnalysisEngine *analysis, GraphNode* node, uint64_t totalBlame,
            Process::ProcessWalkCallback callback)
    {
        if (totalBlame == 0)
            return;

        ProcessWalkInfo walkListAndWaitTime;
        walkListAndWaitTime.waitStateTime = 0;
        analysis->getProcess(node->getProcessId())->walkBack(
                node, callback, &walkListAndWaitTime);
        
        GraphNode::GraphNodeList walkList = walkListAndWaitTime.list;
        uint32_t waitTime = walkListAndWaitTime.waitStateTime;

        if (walkList.size() < 2)
        {
            ErrorUtils::getInstance().throwError("Can't walk list back from %s",
                    node->getUniqueName().c_str());
        }
        
        uint64_t totalWalkTime = walkList.front()->getTime() -
                walkList.back()->getTime();
        GraphNode *lastWalkNode = walkList.front();

        uint32_t waitCtrId = analysis->getCtrTable().getCtrId(CTR_WAITSTATE);
        uint32_t blameCtrId = analysis->getCtrTable().getCtrId(CTR_BLAME);
        uint32_t statBlameCtrId = analysis->getCtrTable().getCtrId(CTR_BLAME_STATISTICS);
        
        for (GraphNode::GraphNodeList::const_iterator iter = (++walkList.begin());
                iter != walkList.end(); ++iter)
        {
            GraphNode *currentWalkNode = *iter;

            uint64_t timeDiff = lastWalkNode->getTime() - currentWalkNode->getTime();
            uint64_t ratioBlame = (double) totalBlame * 
                                  (double) (timeDiff - currentWalkNode->getCounter(waitCtrId, NULL)) /
                                  (double) (totalWalkTime - waitTime);
            
            if (ratioBlame > 0)
                currentWalkNode->incCounter(blameCtrId, ratioBlame);

            /*bool activityIsWaitstate = currentWalkNode->isWaitstate();
            if (!activityIsWaitstate && currentWalkNode->isEnter())
            {
                Edge *edge = analysis->getEdge(currentWalkNode, lastWalkNode);
                if (edge->isBlocking())
                    activityIsWaitstate = true;
            }

            if (!activityIsWaitstate)
            {
                currentWalkNode->incCounter(blameCtrId, ratioBlame);
            } else
            {
                if (currentWalkNode->isEnter() &&
                        (lastWalkNode == currentWalkNode->getPartner()))
                {
                    // find the ratio of blame that has already been attributed to
                    // the root cause
                    Graph &g = analysis->getGraph();
                    const Graph::EdgeList &edges = g.getInEdges(lastWalkNode);
                    for (Graph::EdgeList::const_iterator eIter = edges.begin();
                            eIter != edges.end(); ++eIter)
                    {
                        Edge *e = *eIter;
                        if (e->causesWaitState())
                        {
                            GraphNode *rootEnter = NULL, *rootLeave = NULL;
                            uint64_t myBlameRatio = 0;

                            if (e->getStartNode()->isEnter())
                            {
                                rootEnter = e->getStartNode();
                                rootLeave = rootEnter->getPartner();
                            } else
                            {
                                rootLeave = e->getStartNode();
                                rootEnter = rootLeave->getPartner();
                            }

                            // get non-overlap part after my leave
                            if (rootLeave->getTime() > lastWalkNode->getTime())
                            {
                                myBlameRatio +=
                                        (double) ratioBlame *
                                        (double) (rootLeave->getTime() - lastWalkNode->getTime()) /
                                        (double) (lastWalkNode->getTime() - currentWalkNode->getTime()
                                        - waitTime);
                            }

                            // get non-overlap part before my enter
                            if (rootEnter->getTime() > currentWalkNode->getTime())
                            {
                                myBlameRatio +=
                                        (double) ratioBlame *
                                        (double) (rootEnter->getTime() - currentWalkNode->getTime()) /
                                        (double) (lastWalkNode->getTime() - currentWalkNode->getTime()
                                        - waitTime);
                            }

                            currentWalkNode->incCounter(blameCtrId, myBlameRatio);
                            break;
                        }
                    }
                }
            }*/

            // caller is always enter node
            if (currentWalkNode->isLeave() && currentWalkNode->getCaller() && ratioBlame > 0)
                currentWalkNode->getCaller()->incCounter(statBlameCtrId, ratioBlame);

            lastWalkNode = currentWalkNode;
        }
    }
}

#endif	/* BLAMEDISTRIBUTION_HPP */

