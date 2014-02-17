/* 
 * File:   OneToAllRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef ONETOALLRULE_HPP
#define	ONETOALLRULE_HPP

#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace mpi
    {

        class OneToAllRule : public AbstractRule
        {
        public:

            OneToAllRule(int priority) :
            AbstractRule("OneToAllRule", priority)
            {

            }

            bool apply(AnalysisEngine *analysis, Node *node)
            {
                // applied at MPI OneToAll leave
                if (!node->isMPIOneToAll() || !node->isLeave())
                    return false;

                // get the complete execution
                GraphNode::GraphNodePair oneToAll = ((GraphNode*) node)->getGraphPair();
                uint32_t mpiGroupId = node->getReferencedProcessId();
                uint32_t *root = (uint32_t*) (oneToAll.second->getData());
                if (!root)
                    ErrorUtils::getInstance().throwFatalError("Root must be known for MPI OneToAll");

                const MPIAnalysis::MPICommGroup& mpiCommGroup =
                        analysis->getMPIAnalysis().getMPICommGroup(mpiGroupId);
                
                uint32_t rootId = *root;
                uint32_t rootMPIRank = analysis->getMPIAnalysis().getMPIRank(rootId, mpiCommGroup);

                const uint32_t BUFFER_SIZE = 7;
                uint32_t recvBufferSize = 0;
                if (node->getProcessId() == rootId)
                    recvBufferSize = mpiCommGroup.procs.size() * BUFFER_SIZE;
                else
                    recvBufferSize = BUFFER_SIZE;

                uint32_t sendBuffer[BUFFER_SIZE];
                uint32_t *recvBuffer = new uint32_t[recvBufferSize];
                memset(recvBuffer, 0, recvBufferSize * sizeof (uint32_t));

                uint64_t oneToAllStartTime = oneToAll.first->getTime();
                uint64_t oneToAllEndTime = oneToAll.second->getTime();

                memcpy(sendBuffer, &oneToAllStartTime, sizeof (uint64_t));
                memcpy(sendBuffer + 2, &oneToAllEndTime, sizeof (uint64_t));
                sendBuffer[4] = oneToAll.first->getId();
                sendBuffer[5] = oneToAll.second->getId();
                sendBuffer[6] = node->getProcessId();

                MPI_CHECK(MPI_Gather(sendBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        recvBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        rootMPIRank, mpiCommGroup.comm));
                
                if (node->getProcessId() == rootId)
                {
                    // root computes its blame
                    uint64_t total_blame = 0;
                    for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                    {
                        uint64_t enterTime = ((uint64_t) recvBuffer[i + 1] << 32) + recvBuffer[i];

                        if (enterTime < oneToAllStartTime)
                            total_blame += oneToAllStartTime - enterTime;
                        
                        analysis->getMPIAnalysis().addRemoteMPIEdge(oneToAll.first,
                            recvBuffer[i + 5], recvBuffer[i + 6],
                            MPIAnalysis::MPI_EDGE_LOCAL_REMOTE);
                    }

                    distributeBlame(analysis, oneToAll.first, total_blame, processWalkCallback);
                }
                
                MPI_Barrier(mpiCommGroup.comm);
                
                memcpy(recvBuffer, sendBuffer, sizeof(uint32_t) * BUFFER_SIZE);
                MPI_CHECK(MPI_Bcast(recvBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        rootMPIRank, mpiCommGroup.comm));

                if (node->getProcessId() != rootId)
                {
                    // all others compute their wait states and create dependency edges
                    uint64_t rootEnterTime = ((uint64_t) recvBuffer[1] << 32) + recvBuffer[0];

                    if (rootEnterTime > oneToAllStartTime)
                    {
                        Edge *oneToAllRecordEdge = analysis->getEdge(
                                oneToAll.first, oneToAll.second);
                        oneToAllRecordEdge->makeBlocking();
                        oneToAll.first->setCounter(
                                analysis->getCtrTable().getCtrId(CTR_WAITSTATE),
                                rootEnterTime - oneToAllStartTime);
                    }

                    analysis->getMPIAnalysis().addRemoteMPIEdge(oneToAll.second,
                            recvBuffer[4], recvBuffer[6],
                            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL);
                }

                delete[] recvBuffer;

                return true;
            }
        };
    }
}

#endif	/* ONETOALLRULE_HPP */

