/* 
 * File:   OMPComputeRule.hpp
 * Author: stolle
 *
 * Connects compute regions with pending parallel region.
 * 
 * Complement of this rule is OMPParallelRegionRule.hpp which saves opening parallel regions and connect kernels with closing ones
 * 
 * Created on January 27, 2014, 2:16 PM
 */

#ifndef OMPCOMPUTERULE_HPP
#define	OMPCOMPUTERULE_HPP

#include "AbstractRule.hpp"
#include "graph/GraphNode.hpp"


namespace cdm{
    
    class OMPComputeRule : public AbstractRule 
    {
    public:
        OMPComputeRule(int priority) : 
        AbstractRule("OMPComputeRule", priority)
        {
            
        }
        
        bool apply(AnalysisEngine *analysis, Node *node){
        
            if(!node->isOMPCompute())
                return false;
            
            // if no kernel buffered -> save this one
            if(analysis->getOmpCompute(node->getProcessId()) == NULL)
            {
                GraphNode * ppr = analysis->getPendingParallelRegion();
                
                // if pending parallel region -> connect kernel to it
                if((ppr != NULL)&& (ppr->getProcessId() != node->getProcessId()))
                {
                    // get the complete execution
                    GraphNode::GraphNodePair& kernelPair = ((GraphNode*) node)->getGraphPair();
                                               
                    Process* p = analysis->getProcess(node->getProcessId());
                    // create Edges
                    analysis->newEdge(ppr, kernelPair.first, false);
                                        
                    ErrorUtils::getInstance().outputMessage("[OMPCR] add Edge %s to %s (%s)\n",
                            ppr->getUniqueName().c_str(),kernelPair.first->getUniqueName().c_str(),p->getName());
                }
                analysis->setOmpCompute((GraphNode *) node, node->getProcessId());
            } 
            else //if already kernels buffered -> overwrite
            {
                analysis->setOmpCompute((GraphNode *) node, node->getProcessId());
            }
          
            return true;
                       
        }
    
    };


}

#endif	/* OMPCOMPUTERULE_HPP */

