/* 
 * File:   OMPParallelRegionRule.hpp
 * Author: stolle
 *
 * Save opening parallel regions and connect kernels with closing ones
 * 
 * Complement of this rule is OMPKernelRule.hpp which connects coming kernels with pending parallel region
 * 
 * Created on January 27, 2014, 9:21 AM
 */

#ifndef OMPPARALLELREGIONRULE_HPP
#define	OMPPARALLELREGIONRULE_HPP

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"


namespace cdm
{
    namespace omp
    {

        class OMPParallelRegionRule : public AbstractRule
        {
        public:

            OMPParallelRegionRule(int priority) :
            AbstractRule("OMPParallelRegionRule", priority)
            {

            }

            bool apply(AnalysisEngine *analysis, Node *node)
            {

                if (!node->isOMPParellelRegion())
                    return false;

                GraphNode* ppr = analysis->getPendingParallelRegion();

                if (ppr == NULL)
                {
                    // open parallel region and add as pending for connecting next upcoming kernel
                    analysis->setPendingParallelRegion((GraphNode*) node);
                    return true;
                }

                // check if closing parallel region matches the pending one
                if (ppr->getFunctionId() != node->getFunctionId())
                {
                    ErrorUtils::getInstance().outputMessage("[OMPParallelRegionRule] ERROR: "
                            "parallel region %s doesn't match open parallel region %s \n",
                            node->getUniqueName().c_str(), ppr->getUniqueName().c_str());
                    ErrorUtils::getInstance().outputMessage("[OMPParallelRegionRule] close "
                            "ParallelRegion %s and reset to %s \nCorrectness not guaranteed",
                            ppr->getUniqueName().c_str(), node->getUniqueName().c_str());

                    // close parallel region and reset
                    analysis->setPendingParallelRegion((GraphNode*) node);
                }

                // handle collected omp kernels to add dependency to previous parallel region
                // 1) get all OMP-processes
                const Allocation::ProcessList &procs = analysis->getHostProcesses();

                // 2) iterate over all omp processes and add dependency edge to parallel region leave
                GraphNode* parallelRegionSecond = ((GraphNode*) node)->getGraphPair().second;

                for (Allocation::ProcessList::const_iterator pIter = procs.begin();
                        pIter != procs.end(); ++pIter)
                {
                    Process *p = *pIter;
                    GraphNode* kernel = analysis->getOmpCompute(p->getId());
                    if ((kernel != NULL))
                    {
                        analysis->newEdge(kernel, parallelRegionSecond);

                        ErrorUtils::getInstance().outputMessage("[OMPPRR] add Edge %s to %s (%s)\n",
                                kernel->getUniqueName().c_str(),
                                parallelRegionSecond->getUniqueName().c_str(), p->getName());
                    }
                    analysis->setOmpCompute(NULL, p->getId());
                }

                // close parallel region and set as null
                ErrorUtils::getInstance().outputMessage("[OMPPRR] close ParallelRegion %s \n",
                        ppr->getUniqueName().c_str());
                analysis->setPendingParallelRegion(NULL);

                return true;

            }

        };

    }
}

#endif	/* OMPPARALLELREGIONRULE_HPP */

