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
    class GraphNode : public Node
    {
    public:

        typedef std::list<GraphNode*> GraphNodeList;
        typedef std::set<GraphNode*> GraphNodeSet;
        typedef std::pair<GraphNode*, GraphNode*> GraphNodePair;

        GraphNode(uint64_t time, uint32_t processId, const std::string name,
            Paradigm paradigm, NodeRecordType recordType, int nodeType) :
        Node(time, processId, name, paradigm, recordType, nodeType),
        linkLeft(NULL),
        linkRight(NULL),
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

        void setLinkLeft(GraphNode *cudaLinkLeft)
        {
            this->linkLeft = cudaLinkLeft;
        }

        void setLinkRight(GraphNode *cudaLinkRight)
        {
            this->linkRight = cudaLinkRight;
        }

        GraphNode *getLinkLeft()
        {
            return linkLeft;
        }

        GraphNode *getLinkRight()
        {
            return linkRight;
        }
        
        bool hasParadigm(Paradigm p)
        {
            return paradigm & p;
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
        GraphNode *linkLeft, *linkRight;
        void *data;
    };

    typedef GraphNode* GraphNodePtr;
}

#endif	/* GRAPHNODE_HPP */

