/* 
 * File:   AbstractRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:00 PM
 */

#ifndef ABSTRACTRULE_HPP
#define	ABSTRACTRULE_HPP

#include "graph/Node.hpp"
#include "AnalysisEngine.hpp"

namespace cdm
{

    class AnalysisEngine;

    class AbstractRule
    {
    public:

        AbstractRule(const char *name, int priority) :
        priority(priority),
        name(name)
        {

        }
        
        virtual ~AbstractRule()
        {
            
        }

        const char *getName()
        {
            return name;
        }

        int getPriority()
        {
            return priority;
        }

        virtual bool apply(AnalysisEngine *analysis, Node *n1) = 0;

    private:
        int priority;
        const char *name;
    };

}

#endif	/* ABSTRACTRULE_HPP */

