/* 
 * File:   RemoteGraphNode.hpp
 * Author: felix
 *
 * Created on 13. Juni 2013, 12:20
 */

#ifndef REMOTEGRAPHNODE_HPP
#define	REMOTEGRAPHNODE_HPP

#include <list>
#include <set>

#include "GraphNode.hpp"

namespace cdm
{
    class RemoteGraphNode : public GraphNode
    {
    public:

        typedef std::list<RemoteGraphNode*> RemoteGraphNodeList;
        typedef std::set<RemoteGraphNode*> RemoteGraphNodeSet;

        RemoteGraphNode(uint64_t time, uint32_t remoteProcessId, uint32_t remoteNodeId,
        uint32_t mpiRank, Paradigm paradigm, NodeRecordType recordType, int nodeType) :
        GraphNode(time, remoteProcessId, "<remote>", paradigm, recordType, nodeType),
        mpiRank(mpiRank),
        remoteNodeId(remoteNodeId)
        {
            
        }

        virtual ~RemoteGraphNode()
        {

        }
        
        virtual bool isRemoteNode() const
        {
            return true;
        }

        uint32_t getMPIRank() const
        {
            return mpiRank;
        }
        
        uint32_t getRemoteNodeId() const
        {
            return remoteNodeId;
        }
        
    protected:
        uint32_t mpiRank;
        uint32_t remoteNodeId;
    };

    typedef RemoteGraphNode* RemoteGraphNodePtr;
}

#endif	/* REMOTEGRAPHNODE_HPP */

