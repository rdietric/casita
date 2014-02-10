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
    static void distributeBlame(AnalysisEngine *analysis, GraphNode* node, uint64_t totalBlame,
            Process::ProcessWalkCallback callback)
    {
        GraphNode::GraphNodeList walkList;
        analysis->getProcess(node->getProcessId())->walkBack(
                node, callback, &walkList);

        if (walkList.size() < 2)
        {
            ErrorUtils::getInstance().throwError("Can't walk list back from %s",
                    node->getUniqueName().c_str());
        }

        uint64_t totalWalkTime = walkList.front()->getTime() -
                walkList.back()->getTime();
        GraphNode *lastWalkNode = walkList.front();

        uint32_t blameCtrId = analysis->getCtrTable().getCtrId(CTR_BLAME);
        uint32_t statBlameCtrId = analysis->getCtrTable().getCtrId(CTR_BLAME_STATISTICS);

        for (GraphNode::GraphNodeList::const_iterator iter = (++walkList.begin());
                iter != walkList.end(); ++iter)
        {
            GraphNode *currentWalkNode = *iter;

            uint64_t timeDiff = lastWalkNode->getTime() - currentWalkNode->getTime();
            uint64_t ratioBlame = (double) totalBlame * (double) timeDiff / (double) totalWalkTime;

            bool activityIsWaitstate = currentWalkNode->isWaitstate();
            if (!activityIsWaitstate && currentWalkNode->isEnter())
            {
                Edge *edge = analysis->getEdge(currentWalkNode, lastWalkNode);
                if (edge->isBlocking())
                    activityIsWaitstate = true;
            }

            if (!activityIsWaitstate)
                currentWalkNode->incCounter(blameCtrId, ratioBlame);
            else
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
                            GraphNode *rootCauseNode = e->getStartNode();
                            if (rootCauseNode->isEnter())
                                rootCauseNode = rootCauseNode->getPartner();
                            
                            uint64_t myBlameRatio = 
                                    (double)ratioBlame *
                                    (double)(lastWalkNode->getTime() - rootCauseNode->getTime()) /
                                    (double)(lastWalkNode->getTime() - currentWalkNode->getTime());
                            
                            currentWalkNode->incCounter(blameCtrId, myBlameRatio);
                            break;
                        }
                    }
                }
            }

            // caller is always enter node
            if (currentWalkNode->isLeave() && currentWalkNode->getCaller())
                currentWalkNode->getCaller()->incCounter(statBlameCtrId, ratioBlame);

            lastWalkNode = currentWalkNode;
        }
    }
}

#endif	/* BLAMEDISTRIBUTION_HPP */

