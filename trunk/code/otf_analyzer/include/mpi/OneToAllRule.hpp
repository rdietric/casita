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
                VT_TRACER("OneToAllRule");
                // applied at MPI OneToAll leave
                if (!node->isMPIOneToAll() || !node->isLeave())
                    return false;

                // get the complete execution
                GraphNode::GraphNodePair oneToAll = ((GraphNode*) node)->getGraphPair();
                uint32_t mpiGroupId = node->getReferencedProcessId();
                uint64_t *root = (uint64_t*) (oneToAll.second->getData());
                if (!root)
                    ErrorUtils::getInstance().throwFatalError("Root must be known for MPI OneToAll");

                const MPIAnalysis::MPICommGroup& mpiCommGroup =
                        analysis->getMPIAnalysis().getMPICommGroup(mpiGroupId);
                
                uint64_t rootId = *root;
                uint32_t rootMPIRank = analysis->getMPIAnalysis().getMPIRank(rootId, mpiCommGroup);

                const uint32_t BUFFER_SIZE = 7;
                uint32_t recvBufferSize = 0;
                if (node->getProcessId() == rootId)
                    recvBufferSize = mpiCommGroup.procs.size() * BUFFER_SIZE;
                else
                    recvBufferSize = BUFFER_SIZE;

                uint64_t sendBuffer[BUFFER_SIZE];
                uint64_t *recvBuffer = new uint64_t[recvBufferSize];
                memset(recvBuffer, 0, recvBufferSize * sizeof (uint64_t));

                uint64_t oneToAllStartTime = oneToAll.first->getTime();
                uint64_t oneToAllEndTime = oneToAll.second->getTime();

                //memcpy(sendBuffer, &oneToAllStartTime, sizeof (uint64_t));
                //memcpy(sendBuffer + 2, &oneToAllEndTime, sizeof (uint64_t));
                sendBuffer[0] = oneToAllStartTime;
                sendBuffer[1] = oneToAllEndTime;
                sendBuffer[2] = oneToAll.first->getId();
                sendBuffer[3] = oneToAll.second->getId();
                sendBuffer[4] = node->getProcessId();

                MPI_CHECK(MPI_Gather(sendBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                        recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                        rootMPIRank, mpiCommGroup.comm));
                
                if (node->getProcessId() == rootId)
                {
                    // root computes its blame
                    uint64_t total_blame = 0;
                    for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                    {
                        uint64_t enterTime = recvBuffer[i];

                        if (enterTime < oneToAllStartTime)
                            total_blame += oneToAllStartTime - enterTime;
                        
                        analysis->getMPIAnalysis().addRemoteMPIEdge(oneToAll.first,
                            recvBuffer[i + 3], recvBuffer[i + 4],
                            MPIAnalysis::MPI_EDGE_LOCAL_REMOTE);
                    }

                    distributeBlame(analysis, oneToAll.first, total_blame, processWalkCallback);
                }
                
                MPI_Barrier(mpiCommGroup.comm);
                
                memcpy(recvBuffer, sendBuffer, sizeof(uint64_t) * BUFFER_SIZE);
                MPI_CHECK(MPI_Bcast(recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                        rootMPIRank, mpiCommGroup.comm));

                if (node->getProcessId() != rootId)
                {
                    // all others compute their wait states and create dependency edges
                    uint64_t rootEnterTime = recvBuffer[0];

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
                            recvBuffer[2], recvBuffer[4],
                            MPIAnalysis::MPI_EDGE_REMOTE_LOCAL);
                }

                delete[] recvBuffer;

                return true;
            }
        };
    }
}

#endif	/* ONETOALLRULE_HPP */

