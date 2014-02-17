/* 
 * File:   AllToOneRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef ALLTOONERULE_HPP
#define	ALLTOONERULE_HPP

#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace mpi
    {

        class AllToOneRule : public AbstractRule
        {
        public:

            AllToOneRule(int priority) :
            AbstractRule("AllToOneRule", priority)
            {

            }

            bool apply(AnalysisEngine *analysis, Node *node)
            {
                // applied at MPI AllToOne leave
                if (!node->isMPIAllToOne() || !node->isLeave())
                    return false;

                // get the complete execution
                GraphNode::GraphNodePair allToOne = ((GraphNode*) node)->getGraphPair();
                uint32_t mpiGroupId = node->getReferencedProcessId();
                uint32_t *root = (uint32_t*) (allToOne.second->getData());
                if (!root)
                    ErrorUtils::getInstance().throwFatalError("Root must be known for MPI AllToOne");

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

                uint64_t allToOneStartTime = allToOne.first->getTime();
                uint64_t allToOneEndTime = allToOne.second->getTime();

                memcpy(sendBuffer, &allToOneStartTime, sizeof (uint64_t));
                memcpy(sendBuffer + 2, &allToOneEndTime, sizeof (uint64_t));
                sendBuffer[4] = allToOne.first->getId();
                sendBuffer[5] = allToOne.second->getId();
                sendBuffer[6] = node->getProcessId();

                MPI_CHECK(MPI_Gather(sendBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        recvBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        rootMPIRank, mpiCommGroup.comm));
                
                if (node->getProcessId() == rootId)
                {
                    // root computes its waiting time and creates dependency edges
                    uint64_t total_waiting_time = 0;
                    for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                    {
                        uint64_t enterTime = ((uint64_t) recvBuffer[i + 1] << 32) + recvBuffer[i];

                        if (enterTime > allToOneStartTime)
                            total_waiting_time += enterTime - allToOneStartTime;

                        analysis->getMPIAnalysis().addRemoteMPIEdge(allToOne.second,
                                recvBuffer[i + 4], recvBuffer[i + 6],
                                MPIAnalysis::MPI_EDGE_REMOTE_LOCAL);
                    }

                    if (total_waiting_time)
                    {
                        Edge *allToOneRecordEdge = analysis->getEdge(
                                allToOne.first, allToOne.second);
                        allToOneRecordEdge->makeBlocking();
                        allToOne.first->setCounter(
                                analysis->getCtrTable().getCtrId(CTR_WAITSTATE),
                                total_waiting_time);
                    }
                }
                
                MPI_Barrier(mpiCommGroup.comm);

                memcpy(recvBuffer, sendBuffer, sizeof(uint32_t) * BUFFER_SIZE);
                MPI_CHECK(MPI_Bcast(recvBuffer, BUFFER_SIZE, MPI_UNSIGNED,
                        rootMPIRank, mpiCommGroup.comm));

                if (node->getProcessId() != rootId)
                {
                    // all others compute their blame
                    uint64_t rootEnterTime = ((uint64_t) recvBuffer[1] << 32) + recvBuffer[0];

                    if (rootEnterTime < allToOneStartTime)
                    {
                        distributeBlame(analysis, allToOne.first,
                                allToOneStartTime - rootEnterTime,
                                processWalkCallback);
                    }

                    analysis->getMPIAnalysis().addRemoteMPIEdge(allToOne.first,
                                recvBuffer[5], recvBuffer[6],
                                MPIAnalysis::MPI_EDGE_LOCAL_REMOTE);
                }

                delete[] recvBuffer;

                return true;
            }
        };
    }
}

#endif	/* ALLTOONERULE_HPP */

