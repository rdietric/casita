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
#include <math.h>

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
            for (size_t i = 0; i < NODE_PARADIGM_COUNT; ++i)
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
            size_t i = 0;
            GraphNode *tmpLastNode = NULL;

            for (i = 0; i < NODE_PARADIGM_COUNT; ++i)
                if (graphData[i].lastNode)
                {
                    tmpLastNode = graphData[i].lastNode;
                    break;
                }

            i++;

            for (; i < NODE_PARADIGM_COUNT; ++i)
            {
                if (graphData[i].lastNode &&
                        Node::compareLess(graphData[i].lastNode, tmpLastNode))
                {
                    tmpLastNode = graphData[i].lastNode;
                }
            }

            return tmpLastNode;
        }

        GraphNode *getLastGraphNode(Paradigm paradigm) const
        {
            if (paradigm == PARADIGM_ALL)
                return getLastGraphNode();
            else
                return graphData[(size_t) log2(paradigm)].lastNode;
        }

        GraphNode *getFirstGraphNode(Paradigm paradigm) const
        {
            return graphData[(int) log2(paradigm)].firstNode;
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
            GraphNode * oldNode[NODE_PARADIGM_COUNT];
            for (size_t i = 0; i < NODE_PARADIGM_COUNT; ++i)
                oldNode[i] = NULL;

            Paradigm nodeParadigm = node->getParadigm();

            for (size_t o = 1; o < NODE_PARADIGM_INVALID; o *= 2)
            {
                Paradigm oparadigm = (Paradigm) o;
                size_t paradigm_index = (size_t) log2(oparadigm);

                oldNode[paradigm_index] = getLastGraphNode(oparadigm);

                if (node->hasParadigm(oparadigm))
                {
                    if (oldNode[paradigm_index] && Node::compareLess(node, oldNode[paradigm_index]))
                        throw RTException("Can't add graph node (%s) before last graph node (%s)",
                            node->getUniqueName().c_str(),
                            oldNode[paradigm_index]->getUniqueName().c_str());

                    if (graphData[paradigm_index].firstNode == NULL)
                        graphData[paradigm_index].firstNode = node;

                    graphData[paradigm_index].lastNode = node;
                }
            }

            addNodeInternal(nodes, node);

            if (nodeParadigm == PARADIGM_MPI)
            {
                GraphNode *lastCuda = getLastGraphNode(PARADIGM_CUDA);
                node->setLinkLeft(lastCuda);
                unlinkedMPINodes.push_back(node);
            }

            if ((nodeParadigm == PARADIGM_CUDA) && (node->isEnter()))
            {
                for (SortedGraphNodeList::const_iterator iter = unlinkedMPINodes.begin();
                        iter != unlinkedMPINodes.end(); ++iter)
                {
                    (*iter)->setLinkRight(node);
                }
                unlinkedMPINodes.clear();
            }

            if (predCUDA)
                *predCUDA = oldNode[(size_t) log2(PARADIGM_CUDA)];

            if (predMPI)
                *predMPI = oldNode[(size_t) log2(PARADIGM_MPI)];
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

            for (size_t paradigm = 1; paradigm < NODE_PARADIGM_INVALID; paradigm *= 2)
            {
                // find previous node
                GraphNode *predNode = NULL;
                current = result;
                while (current != nodes.begin())
                {
                    --current;
                    if ((*current)->isGraphNode())
                    {
                        GraphNode *gNode = (GraphNode*) (*current);
                        if (gNode->hasParadigm((Paradigm)paradigm))
                        {
                            predNode = gNode;
                            break;
                        }
                    }
                }

                switch ((Paradigm) paradigm)
                {
                    case PARADIGM_CUDA:
                        if (predNodeCUDA)
                            *predNodeCUDA = predNode;
                        break;

                    case PARADIGM_MPI:
                        if (predNodeMPI)
                            *predNodeMPI = predNode;
                        break;
                        
                    default:
                        break;
                }
            }

            // find next node
            bool hasNextNode[NODE_PARADIGM_COUNT];

            for (size_t paradigm = 1; paradigm < NODE_PARADIGM_INVALID; paradigm *= 2)
            {
                current = result;
                SortedNodeList::iterator next = ++current;
                size_t paradigm_index = (size_t) log2(paradigm);
                hasNextNode[paradigm_index] = false;

                GraphNode *nextNode = NULL;

                while (next != nodes.end())
                {
                    if ((*next)->isGraphNode())
                    {
                        GraphNode *gNode = (GraphNode*) (*next);
                        if (gNode->hasParadigm((Paradigm)paradigm))
                        {
                            nextNode = gNode;
                            hasNextNode[paradigm_index] = true;
                            break;
                        }
                    }
                    ++next;
                }

                switch ((Paradigm) paradigm)
                {
                    case PARADIGM_CUDA:
                        if (nextNodeCUDA)
                            *nextNodeCUDA = nextNode;
                        break;

                    case PARADIGM_MPI:
                        if (nextNodeMPI)
                            *nextNodeMPI = nextNode;
                        break;
                        
                    default:
                        break;
                }

                if (node->hasParadigm((Paradigm)paradigm))
                {
                    if (!graphData[paradigm_index].firstNode)
                        graphData[paradigm_index].firstNode = node;

                    if (!hasNextNode[paradigm_index])
                        graphData[paradigm_index].lastNode = node;
                }
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
        GraphData graphData[NODE_PARADIGM_COUNT];
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

