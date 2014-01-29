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

    class Edge
    {
    public:
        typedef std::map<uint32_t, uint64_t> TimeProfileMap;

        Edge(GraphNode *start, GraphNode *end, uint64_t duration, bool blocking,
                Paradigm edgeParadigm)
        {
            pair = std::make_pair(start, end);
            if (isReverseEdge())
            {
                blocking = true;
                duration = 0;
            }

            this->blocking = blocking;
            this->edgeDuration = duration;
            this->initialDuration = duration;
            this->edgeWeight = computeWeight(duration, blocking);
            this->edgeParadigm = edgeParadigm;
            this->timeProfile = NULL;
        }

        bool hasEdgeType(Paradigm edgeParadigm)
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

            if (edgeParadigm & PARADIGM_ALL)
                name << "ALL";
            else
            {
                if (edgeParadigm & PARADIGM_CUDA)
                    name << "CUDA,";
                if (edgeParadigm & PARADIGM_MPI)
                    name << "MPI,";
            }

            name << ")]";

            return name.str();
        }

        void setDuration(uint64_t duration)
        {
            edgeDuration = duration;
            edgeWeight = computeWeight(edgeDuration, blocking);
        }

        bool isBlocking() const
        {
            return blocking;
        }

        void makeBlocking()
        {
            if (!blocking)
            {
                edgeWeight = std::numeric_limits<uint64_t>::max();
                blocking = true;
            }
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
        bool blocking;
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

