/* 
 * File:   GraphNode.hpp
 * Author: felix
 *
 * Created on 13. Juni 2013, 12:20
 */

#ifndef GRAPHNODE_HPP
#define	GRAPHNODE_HPP

#include <list>
#include <set>

#include "Node.hpp"

namespace cdm
{

    enum GraphNodeType
    {
        GRAPH_CUDA = 0,
        GRAPH_MPI = 1,

        GRAPH_MAX = 2
    };

    class GraphNode : public Node
    {
    public:

        typedef std::list<GraphNode*> GraphNodeList;
        typedef std::set<GraphNode*> GraphNodeSet;
        typedef std::pair<GraphNode*, GraphNode*> GraphNodePair;

        GraphNode(uint64_t time, uint32_t processId, const std::string name, int nodeType) :
        Node(time, processId, name, nodeType),
        cudaLinkLeft(NULL),
        cudaLinkRight(NULL),
        data(NULL)
        {
            pair.first = (this);
            pair.second = (NULL);
        }

        virtual ~GraphNode()
        {

        }
        
        void setName(const std::string newName)
        {
            name = newName;
        }

        bool isGraphNode() const
        {
            return true;
        }

        void setPartner(GraphNode *partner)
        {
            if (partner == NULL || (this->time < partner->time))
            {
                pair.first = (this);
                pair.second = (partner);
            } else
            {
                pair.first = (partner);
                pair.second = (this);
            }
        }

        virtual bool hasPartner() const
        {
            return pair.first && pair.second;
        }

        GraphNode *getPartner() const
        {
            if (isEnter())
                return pair.second;
            else
                return pair.first;
        }

        GraphNodePair& getGraphPair()
        {
            return pair;
        }

        void reduceTimestamp(uint64_t delta)
        {
            this->time -= delta;
        }

        void setCUDALinkLeft(GraphNode *cudaLinkLeft)
        {
            this->cudaLinkLeft = cudaLinkLeft;
        }

        void setCUDALinkRight(GraphNode *cudaLinkRight)
        {
            this->cudaLinkRight = cudaLinkRight;
        }

        GraphNode *getCUDALinkLeft()
        {
            return cudaLinkLeft;
        }

        GraphNode *getCUDALinkRight()
        {
            return cudaLinkRight;
        }
        
        bool hasGraphNodeType(GraphNodeType g)
        {
            return (((g == GRAPH_MPI) && isMPI()) || ((g == GRAPH_CUDA) && isCUDA()));
        }

        GraphNodeType getGraphNodeType()
        {
            if (isMPI() && isCUDA())
                return GRAPH_MAX;
            
            if (isMPI())
                return GRAPH_MPI;
            else
                return GRAPH_CUDA;
        }
        
        void setData(void *value)
        {
            this->data = value;
        }
        
        void *getData() const
        {
            return this->data;
        }

    protected:
        GraphNodePair pair;
        GraphNode *cudaLinkLeft, *cudaLinkRight;
        void *data;
    };

    typedef GraphNode* GraphNodePtr;
}

#endif	/* GRAPHNODE_HPP */

