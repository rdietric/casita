/* 
 * File:   CollectiveRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef COLLECTIVERULE_HPP
#define	COLLECTIVERULE_HPP

#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"
#include "BlameDistribution.hpp"

namespace cdm
{
    namespace mpi
    {

        class CollectiveRule : public AbstractRule
        {
        public:

            CollectiveRule(int priority) :
            AbstractRule("CollectiveRule", priority)
            {

            }

            bool apply(AnalysisEngine *analysis, Node *node)
            {
                // applied at MPI collective leave
                if (!node->isMPICollective() || !node->isLeave())
                    return false;

                // get the complete execution
                GraphNode::GraphNodePair coll = ((GraphNode*) node)->getGraphPair();
                uint32_t mpiGroupId = node->getReferencedProcessId();
                const MPIAnalysis::MPICommGroup& mpiCommGroup =
                        analysis->getMPIAnalysis().getMPICommGroup(mpiGroupId);
                uint32_t myMpiRank = analysis->getMPIRank();

                const uint32_t BUFFER_SIZE = 7;
                uint32_t recvBufferSize = mpiCommGroup.procs.size() * BUFFER_SIZE;
                uint32_t sendBuffer[BUFFER_SIZE];
                uint32_t *recvBuffer = new uint32_t[recvBufferSize];
                memset(recvBuffer, 0, recvBufferSize * sizeof (uint32_t));

                uint64_t collStartTime = coll.first->getTime();
                uint64_t collEndTime = coll.second->getTime();

                memcpy(sendBuffer, &collStartTime, sizeof (uint64_t));
                memcpy(sendBuffer + 2, &collEndTime, sizeof (uint64_t));
                sendBuffer[4] = coll.first->getId();
                sendBuffer[5] = coll.second->getId();
                sendBuffer[6] = node->getProcessId();

                MPI_CHECK(MPI_Allgather(sendBuffer, BUFFER_SIZE, MPI_INTEGER4,
                        recvBuffer, BUFFER_SIZE, MPI_INTEGER4, mpiCommGroup.comm));

                // get last enter event for collective
                uint64_t lastEnterTime = 0, lastLeaveTime = 0;
                uint32_t lastEnterProcessId = 0, lastLeaveProcessId = 0;
                uint32_t lastEnterRemoteNodeId = 0, lastLeaveRemoteNodeId = 0;

                for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                {
                    uint64_t enterTime = ((uint64_t) recvBuffer[i + 1] << 32) + recvBuffer[i];
                    uint64_t leaveTime = ((uint64_t) recvBuffer[i + 3] << 32) + recvBuffer[i + 2];

                    if (enterTime > lastEnterTime)
                    {
                        lastEnterTime = enterTime;
                        lastEnterRemoteNodeId = recvBuffer[i + 4];
                        lastEnterProcessId = recvBuffer[i + 6];
                    }

                    if (leaveTime > lastLeaveTime)
                    {
                        lastLeaveTime = leaveTime;
#ifdef MPI_CP_MERGE
                        lastLeaveRemoteNodeId = recvBuffer[i + 4];
                        lastLeaveProcessId = recvBuffer[i + 6];
#endif
                    }
                }

                // I'm not latest collective -> blocking + remoteEdge to lastEnter
                if (collStartTime < lastEnterTime)
                {
                    /** These nodes/edges are needed for dependency correctness but are
                     *  omitted since they are currently not used anywhere.
                     * analysis->getMPIAnalysis().addRemoteMPIEdge(coll.second, lastEnterRemoteNodeId, lastEnterProcessId);
                     *
                     * GraphNode *remoteNode = analysis->addNewRemoteNode(lastEnterTime, lastEnterProcessId,
                     *         lastEnterRemoteNodeId, PARADIGM_MPI, RECORD_ENTER, MPI_COLL,
                     *         analysis->getMPIAnalysis().getMPIRank(lastEnterProcessId));
                     *
                     * analysis->newEdge(remoteNode, coll.second);
                     **/
                    Edge *collRecordEdge = analysis->getEdge(coll.first, coll.second);
                    collRecordEdge->makeBlocking();
                    coll.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 
                            lastEnterTime-collStartTime + lastLeaveTime-collEndTime);
#ifdef MPI_CP_MERGE
                    analysis->getMPIAnalysis().addMPIEdge(coll.first,
                            lastEnterRemoteNodeId, lastEnterProcessId);
#endif
                } else // I'm latest collective
                {
                    // aggregate blame from all other processes
                    uint64_t total_blame = 0;
                    for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                    {
                        uint64_t enterTime = ((uint64_t) recvBuffer[i + 1] << 32) + recvBuffer[i];
                        total_blame += lastEnterTime - enterTime;

                        if (i != myMpiRank)
                        {
                            analysis->getMPIAnalysis().addRemoteMPIEdge(coll.first,
                                    recvBuffer[i + 4], recvBuffer[i + 6]);

                            /** These nodes/edges are needed for dependency correctness but are
                             *  omitted since they are currently not used anywhere.
                             * uint64_t leaveTime = ((uint64_t) recvBuffer[i + 3] << 32) + recvBuffer[i + 2];
                             * uint32_t remoteLeaveNodeId = recvBuffer[i + 5];
                             * uint32_t remoteProcessId = recvBuffer[i + 6];
                             * 
                             * GraphNode *remoteNode = analysis->addNewRemoteNode(leaveTime, remoteProcessId,
                                    remoteLeaveNodeId, PARADIGM_MPI, RECORD_LEAVE, MPI_COLL,
                                    analysis->getMPIAnalysis().getMPIRank(remoteProcessId));

                             * analysis->newEdge(coll.first, remoteNode);*/

                        }
                    }

                    distributeBlame(analysis, coll.first, total_blame, processWalkCallback);
                }

                if (collEndTime < lastLeaveTime)
                {
#ifdef MPI_CP_MERGE
                    analysis->getMPIAnalysis().addMPIEdge(coll.second,
                            lastLeaveRemoteNodeId, lastLeaveProcessId);
#endif
                }

                delete[] recvBuffer;

                return true;
            }
        };
    }
}

#endif	/* COLLECTIVERULE_HPP */

