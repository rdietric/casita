/* 
 * File:   Process.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 11:30 AM
 */

#ifndef PROCESS_HPP
#define	PROCESS_HPP

#include <vector>
#include <algorithm>
#include <string>
#include <list>
#include <iostream>
#include <map>

#include "graph/GraphNode.hpp"
#include "graph/Edge.hpp"
#include "common.hpp"

#include <sys/time.h>

namespace cdm
{

    class Process
    {
    public:

        enum ProcessType
        {
            PT_HOST = 1, PT_DEVICE = 2, PT_DEVICE_NULL = 3
        };

        enum MPIType
        {
            MPI_SEND, MPI_RECV, MPI_COLLECTIVE, MPI_SENDRECV
        };

        typedef struct
        {
            MPIType mpiType;
            uint32_t partnerId; // process or process group
        } MPICommRecord;
        
        typedef std::vector<MPICommRecord> MPICommRecordList;

        //typedef std::set<Node*, nodeCompareLess> SortedNodeList;
        //typedef std::set<GraphNode*, nodeCompareLess> SortedGraphNodeList;

        typedef std::vector<Node*> SortedNodeList;
        typedef std::vector<GraphNode*> SortedGraphNodeList;
        
    private:

        typedef struct
        {
            GraphNode *firstNode;
            GraphNode *lastNode;
        } GraphData;

    public:

        Process(uint32_t id, uint32_t parentId, const std::string name,
                ProcessType processType, bool remoteProcess = false) :
        id(id),
        parentId(parentId),
        name(name),
        processType(processType),
        remoteProcess(remoteProcess),
        lastNode(NULL)
        {
            for (int i = 0; i < GRAPH_MAX; ++i)
            {
                graphData[i].firstNode = NULL;
                graphData[i].lastNode = NULL;
            }
        }

        virtual ~Process()
        {
            for (SortedNodeList::iterator iter = nodes.begin();
                    iter != nodes.end(); ++iter)
            {
                delete (*iter);
            }
        }

        uint32_t getId() const
        {
            return id;
        }

        uint32_t getParentId() const
        {
            return parentId;
        }

        const char *getName() const
        {
            return name.c_str();
        }

        ProcessType getProcessType() const
        {
            return processType;
        }

        bool isHostProcess() const
        {
            return processType == PT_HOST;
        }

        bool isDeviceProcess() const
        {
            return processType != PT_HOST;
        }

        bool isDeviceNullProcess() const
        {
            return processType == PT_DEVICE_NULL;
        }

        bool isRemoteProcess() const
        {
            return remoteProcess;
        }

        Node *getLastNode() const
        {
            return lastNode;
        }

        GraphNode *getLastGraphNode() const
        {
            if (graphData[GRAPH_CUDA].lastNode == NULL)
                return graphData[GRAPH_MPI].lastNode;
            
            if (graphData[GRAPH_MPI].lastNode == NULL)
                return graphData[GRAPH_CUDA].lastNode;
            
            if (Node::compareLess(graphData[GRAPH_CUDA].lastNode,
                    graphData[GRAPH_MPI].lastNode))
            {
                return graphData[GRAPH_MPI].lastNode;
            } else
                return graphData[GRAPH_CUDA].lastNode;
        }

        GraphNode *getLastGraphNode(GraphNodeType g) const
        {
            if (g == GRAPH_MAX)
                return getLastGraphNode();
            else
                return graphData[g].lastNode;
        }

        GraphNode *getFirstGraphNode(GraphNodeType g) const
        {
            return graphData[g].firstNode;
        }

        uint64_t getLastEventTime() const
        {
            if (lastNode)
                return lastNode->getTime();
            else
                return 0;
        }

        void addGraphNode(GraphNode *node, GraphNode **predCUDA, GraphNode **predMPI)
        {
            GraphNode * oldNode[GRAPH_MAX] = {NULL, NULL};
            GraphNodeType g = node->getGraphNodeType();

            for (int o = GRAPH_CUDA; o < GRAPH_MAX; ++o)
            {
                GraphNodeType og = (GraphNodeType) o;
                oldNode[og] = getLastGraphNode(og);

                if (node->hasGraphNodeType(og))
                {
                    if (oldNode[og] && Node::compareLess(node, oldNode[og]))
                        throw RTException("Can't add graph node (%s) before last graph node (%s)",
                            node->getUniqueName().c_str(),
                            oldNode[og]->getUniqueName().c_str());

                    if (graphData[og].firstNode == NULL)
                        graphData[og].firstNode = node;

                    graphData[og].lastNode = node;
                }
            }

            addNodeInternal(nodes, node);

            if (g == GRAPH_MPI)
            {
                GraphNode *lastCuda = getLastGraphNode(GRAPH_CUDA);
                node->setCUDALinkLeft(lastCuda);
                unlinkedMPINodes.push_back(node);
            }

            if ((g == GRAPH_CUDA) && (node->isEnter()))
            {
                for (SortedGraphNodeList::const_iterator iter = unlinkedMPINodes.begin();
                        iter != unlinkedMPINodes.end(); ++iter)
                {
                    (*iter)->setCUDALinkRight(node);
                }
                unlinkedMPINodes.clear();
            }

            if (predCUDA)
                *predCUDA = oldNode[GRAPH_CUDA];

            if (predMPI)
                *predMPI = oldNode[GRAPH_MPI];
        }

        void insertGraphNode(GraphNode *node,
                GraphNode **predNodeCUDA, GraphNode **nextNodeCUDA,
                GraphNode **predNodeMPI, GraphNode **nextNodeMPI)
        {
            if (!lastNode || Node::compareLess(lastNode, node))
            {
                lastNode = node;
            }

            SortedNodeList::iterator result = nodes.end();
            for (SortedNodeList::iterator iter = nodes.begin();
                    iter != nodes.end(); ++iter)
            {
                SortedNodeList::iterator next = iter;
                ++next;
                
                if (next == nodes.end())
                {
                    nodes.push_back(node);
                    break;
                }
                
                if (Node::compareLess(node, *next))
                {
                    result = nodes.insert(next, node);
                    break;
                }
            }

            SortedNodeList::iterator current;

            // find previous CUDA node
            if (predNodeCUDA)
            {
                *predNodeCUDA = NULL;
                current = result;
                while (current != nodes.begin())
                {
                    --current;
                    if ((*current)->isGraphNode())
                    {
                        GraphNode *gNode = (GraphNode*) (*current);
                        if (gNode->hasGraphNodeType(GRAPH_CUDA))
                        {
                            *predNodeCUDA = gNode;
                            break;
                        }
                    }
                }
            }

            // find previous MPI node
            if (predNodeMPI)
            {
                *predNodeMPI = NULL;
                current = result;
                while (current != nodes.begin())
                {
                    --current;
                    if ((*current)->isGraphNode())
                    {
                        GraphNode *gNode = (GraphNode*) (*current);
                        if (gNode->hasGraphNodeType(GRAPH_MPI))
                        {
                            *predNodeMPI = gNode;
                            break;
                        }
                    }
                }
            }

            // find next CUDA node
            current = result;
            SortedNodeList::iterator next = ++current;
            bool hasNextNodeCUDA = false;
            if (nextNodeCUDA)
                *nextNodeCUDA = NULL;

            while (next != nodes.end())
            {
                if ((*next)->isGraphNode())
                {
                    GraphNode *gNode = (GraphNode*) (*next);
                    if (gNode->hasGraphNodeType(GRAPH_CUDA))
                    {
                        if (nextNodeCUDA)
                            *nextNodeCUDA = gNode;
                        hasNextNodeCUDA = true;
                        break;
                    }
                }
                ++next;
            }

            // find next MPI node
            current = result;
            next = ++current;
            bool hasNextNodeMPI = false;
            if (nextNodeMPI)
                *nextNodeMPI = NULL;

            while (next != nodes.end())
            {
                if ((*next)->isGraphNode())
                {
                    GraphNode *gNode = (GraphNode*) (*next);
                    if (gNode->hasGraphNodeType(GRAPH_MPI))
                    {
                        if (nextNodeMPI)
                            *nextNodeMPI = gNode;
                        hasNextNodeMPI = true;
                        break;
                    }
                }
                ++next;
            }

            if (node->hasGraphNodeType(GRAPH_CUDA))
            {
                if (!graphData[GRAPH_CUDA].firstNode)
                    graphData[GRAPH_CUDA].firstNode = node;

                if (!hasNextNodeCUDA)
                    graphData[GRAPH_CUDA].lastNode = node;
            }

            if (node->hasGraphNodeType(GRAPH_MPI))
            {
                if (!graphData[GRAPH_MPI].firstNode)
                    graphData[GRAPH_MPI].firstNode = node;

                if (!hasNextNodeMPI)
                    graphData[GRAPH_MPI].lastNode = node;
            }
        }

        SortedNodeList& getNodes()
        {
            return nodes;
        }

        void addPendingKernel(GraphNode *kernelLeave)
        {
            pendingKernels.push_back(kernelLeave);
        }

        GraphNode *getPendingKernel()
        {
            SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
            if (iter != pendingKernels.rend())
                return *iter;
            else
                return NULL;
        }

        GraphNode *consumePendingKernel()
        {
            SortedGraphNodeList::reverse_iterator iter = pendingKernels.rbegin();
            if (iter != pendingKernels.rend())
            {
                GraphNode *result = *iter;
                pendingKernels.pop_back();
                return result;
            }

            return NULL;
        }

        void clearPendingKernels()
        {
            pendingKernels.clear();
        }

        void setPendingMPIRecord(MPIType mpiType, uint32_t partnerId)
        {
            MPICommRecord record;
            record.mpiType = mpiType;
            record.partnerId = partnerId;
            
            mpiCommRecords.push_back(record);
        }

        MPICommRecordList getPendingMPIRecords()
        {
            MPICommRecordList copyList;
            copyList.assign(mpiCommRecords.begin(), mpiCommRecords.end());
            mpiCommRecords.clear();
            return copyList;
        }
        
        Edge::TimeProfileMap *newTimeProfile()
        {
            currentTimeProfile = new Edge::TimeProfileMap();
            
            return getTimeProfile();
        }
        
        Edge::TimeProfileMap *getTimeProfile()
        {
            return currentTimeProfile;
        }

    private:
        uint32_t id;
        uint32_t parentId;
        const std::string name;
        ProcessType processType;
        bool remoteProcess;

        SortedGraphNodeList pendingKernels; // list of unsynchronized kernels (leave records)

        Node *lastNode;
        GraphData graphData[GRAPH_MAX];
        SortedNodeList nodes;
        SortedGraphNodeList unlinkedMPINodes;

        MPICommRecordList mpiCommRecords;
        
        Edge::TimeProfileMap *currentTimeProfile;

        void addNodeInternal(SortedNodeList& nodes, Node *node)
        {
            nodes.push_back(node);

            lastNode = node;
        }
    };

}

#endif	/* PROCESS_HPP */

