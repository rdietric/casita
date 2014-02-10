/* 
 * File:   OMPBarrierRule.hpp
 * Author: stolle
 *
 * Blame the last barrier Enter
 * 
 * Created on January 16, 2014, 2:58 PM
 */

#ifndef OMPBARRIERRULE_HPP
#define	OMPBARRIERRULE_HPP

#include "AbstractRule.hpp"
#include "OMPRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace omp
    {

        class OMPBarrierRule : public AbstractRule
        {
        public:

            OMPBarrierRule(int priority) :
            AbstractRule("OMPBarrierRule", priority)
            {

            }

            bool apply(AnalysisEngine *analysis, Node *node)
            {
                if (!node->isOMPSync() || !node->isLeave())
                    return false;

                // save barrier enter events to BarrierEventList
                GraphNode* enterEvent = ((GraphNode*) node)->getPartner();
                analysis->addBarrierEventToList(enterEvent);

                const Allocation::ProcessList &procs = analysis->getHostProcesses();
                const GraphNode::GraphNodeList& barrierList = analysis->getBarrierEventList();

                GraphNode::GraphNodeList::const_iterator iter = barrierList.begin();
                GraphNode* maxEnterTimeNode = *iter; // keep enter event with max enter timestamp

                uint64_t blame = 0;

                // check if all barriers were passed
                if (procs.size() == barrierList.size())
                {
                    // find last barrierEnter
                    for (; iter != barrierList.end(); ++iter)
                    {
                        if ((*iter)->getTime() > maxEnterTimeNode->getTime())
                            maxEnterTimeNode = *iter;
                    }

                    // accumulate blame, set edges from latest enter to all other leaves
                    for (iter = barrierList.begin(); iter != barrierList.end(); ++iter)
                    {
                        GraphNode::GraphNodePair& barrier = (*iter)->getGraphPair();
                        if (barrier.first != maxEnterTimeNode)
                        {
                            // make this barrier a blocking waitstate 
                            analysis->getEdge(barrier.first, barrier.second)->makeBlocking();
                            barrier.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);

                            // create edge from latest enter to other leaves
                            analysis->newEdge(maxEnterTimeNode, barrier.second, EDGE_CAUSES_WAITSTATE);
                        }

                        blame += maxEnterTimeNode->getTime() - barrier.first->getTime();
                    }

                    // set blame
                    distributeBlame(analysis, maxEnterTimeNode, blame, processWalkCallback);

                    // clear list of buffered barriers
                    analysis->clearBarrierEventList();

                    return true;
                }

                return false;

            }

        };

    }
}

#endif	/* OMPBARRIERRULE_HPP */

