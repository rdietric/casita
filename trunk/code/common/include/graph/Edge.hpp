/* 
 * File:   Edge.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 12:37 PM
 */

#ifndef EDGE_HPP
#define	EDGE_HPP

#include <sstream>
#include <limits>
#include <map>

#include "GraphNode.hpp"

namespace cdm
{

    enum EdgeProperties
    {
        EDGE_NONE = 0,
        EDGE_IS_BLOCKING = (1 << 0),
        EDGE_CAUSES_WAITSTATE = (1 << 1)
    };
    
    class Edge
    {
    public:
        typedef std::map<uint32_t, uint64_t> TimeProfileMap;
        typedef std::map<Paradigm, Edge*> ParadigmEdgeMap;

        Edge(GraphNode *start, GraphNode *end, uint64_t duration,
            int properties, Paradigm edgeParadigm) :
            properties(EDGE_NONE)
        {
            pair = std::make_pair(start, end);
            if (isReverseEdge())
            {
                properties |= EDGE_IS_BLOCKING;
                duration = 0;
            }

            this->properties = properties;
            this->edgeDuration = duration;
            this->initialDuration = duration;
            this->edgeWeight = computeWeight(duration, isBlocking());
            this->edgeParadigm = edgeParadigm;
            this->timeProfile = NULL;
        }

        bool hasEdgeType(Paradigm edgeParadigm) const
        {
            return edgeParadigm & this->edgeParadigm;
        }

        Paradigm getEdgeType() const
        {
            return edgeParadigm;
        }

        uint64_t getDuration() const
        {
            return edgeDuration;
        }

        uint64_t getInitialDuration() const
        {
            return initialDuration;
        }

        uint64_t getWeight() const
        {
            return edgeWeight;
        }

        const std::string getName() const
        {
            std::stringstream name;
            name << "[" << pair.first->getUniqueName().c_str() << ", " <<
                    pair.second->getUniqueName().c_str() << ", (";

            if (edgeParadigm == PARADIGM_ALL)
                name << "ALL";
            else
            {
                if (edgeParadigm & PARADIGM_CUDA)
                    name << "CUDA,";
                if (edgeParadigm & PARADIGM_MPI)
                    name << "MPI,";
                if (edgeParadigm & PARADIGM_OMP)
                    name << "OMP,";
                if (edgeParadigm & PARADIGM_CPU)
                    name << "CPU,";
            }

            name << ")]";

            return name.str();
        }

        void setDuration(uint64_t duration)
        {
            edgeDuration = duration;
            edgeWeight = computeWeight(edgeDuration, isBlocking());
        }

        bool isBlocking() const
        {
            return properties & EDGE_IS_BLOCKING;
        }

        void makeBlocking()
        {
            edgeWeight = std::numeric_limits<uint64_t>::max();
            properties |= EDGE_IS_BLOCKING;
        }
        
        bool causesWaitState() const
        {
            return properties & EDGE_CAUSES_WAITSTATE;
        }

        GraphNode *getStartNode() const
        {
            return pair.first;
        }

        GraphNode *getEndNode() const
        {
            return pair.second;
        }

        GraphNode::GraphNodePair& getNodes()
        {
            return pair;
        }

        bool isReverseEdge() const
        {
            return pair.first->getTime() > pair.second->getTime();
        }

        bool isIntraProcessEdge() const
        {
            return pair.first->isLeave() &&
                    (pair.first->getProcessId() == pair.second->getProcessId());
        }

        bool isInterProcessEdge() const
        {
            return pair.first->getProcessId() != pair.second->getProcessId();
        }

        bool isRemoteEdge() const
        {
            return pair.first->isRemoteNode() || pair.second->isRemoteNode();
        }

        TimeProfileMap *getTimeProfile()
        {
            return timeProfile;
        }

    private:
        int properties;
        uint64_t edgeDuration;
        uint64_t initialDuration;
        uint64_t edgeWeight;
        Paradigm edgeParadigm;
        GraphNode::GraphNodePair pair;
        TimeProfileMap *timeProfile;

        void setWeight(uint64_t weight)
        {
            edgeWeight = weight;
        }

        static uint64_t computeWeight(uint64_t duration, bool blocking)
        {
            if (!blocking)
            {
                uint64_t tmpDuration = duration;
                if (tmpDuration == 0)
                    tmpDuration = 1;

                return std::numeric_limits<uint64_t>::max() - tmpDuration;
            } else
            {
                return std::numeric_limits<uint64_t>::max();
            }
        }
    };

}

#endif	/* EDGE_HPP */

