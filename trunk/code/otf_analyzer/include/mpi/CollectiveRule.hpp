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
                VT_TRACER("CollectiveRule");
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
                uint64_t sendBuffer[BUFFER_SIZE];
                uint64_t *recvBuffer = new uint64_t[recvBufferSize];
                memset(recvBuffer, 0, recvBufferSize * sizeof (uint64_t));

                uint64_t collStartTime = coll.first->getTime();
                uint64_t collEndTime = coll.second->getTime();

                //memcpy(sendBuffer, &collStartTime, sizeof (uint64_t));
                //memcpy(sendBuffer + 2, &collEndTime, sizeof (uint64_t));
                sendBuffer[0] = collStartTime;
                sendBuffer[1] = collEndTime;
                sendBuffer[2] = coll.first->getId();
                sendBuffer[3] = coll.second->getId();
                sendBuffer[4] = node->getProcessId();

                MPI_CHECK(MPI_Allgather(sendBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG,
                        recvBuffer, BUFFER_SIZE, MPI_UNSIGNED_LONG_LONG, mpiCommGroup.comm));

                // get last enter event for collective
                uint64_t lastEnterTime = 0, lastLeaveTime = 0;
                uint64_t lastEnterProcessId = 0;
                //uint64_t lastLeaveProcessId = 0;
                uint64_t lastEnterRemoteNodeId = 0;
                //uint64_t lastLeaveRemoteNodeId = 0;

                for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                {
                    uint64_t enterTime = recvBuffer[i];
                    uint64_t leaveTime = recvBuffer[i + 1];

                    if (enterTime > lastEnterTime)
                    {
                        lastEnterTime = enterTime;
                        lastEnterRemoteNodeId = recvBuffer[i + 2];
                        lastEnterProcessId = recvBuffer[i + 4];
                    }

                    if (leaveTime > lastLeaveTime)
                    {
                        lastLeaveTime = leaveTime;
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
                            lastEnterTime - collStartTime);
                    
                    analysis->getMPIAnalysis().addRemoteMPIEdge(coll.second,
                                    lastEnterRemoteNodeId, lastEnterProcessId,
                                    MPIAnalysis::MPI_EDGE_REMOTE_LOCAL);
                } else // I'm latest collective
                {
                    // aggregate blame from all other processes
                    uint64_t total_blame = 0;
                    for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
                    {
                        uint64_t enterTime = recvBuffer[i];
                        total_blame += lastEnterTime - enterTime;

                        if (i != myMpiRank)
                        {
                            analysis->getMPIAnalysis().addRemoteMPIEdge(coll.first,
                                    recvBuffer[i + 3], recvBuffer[i + 4],
                                    MPIAnalysis::MPI_EDGE_LOCAL_REMOTE);

                            /** These nodes/edges are needed for dependency correctness but are
                             *  omitted since they are currently not used anywhere.
                             * uint64_t leaveTime = recvBuffer[i + 1];
                             * uint64_t remoteLeaveNodeId = recvBuffer[i + 3];
                             * uint64_t remoteProcessId = recvBuffer[i + 4];
                             * 
                             * GraphNode *remoteNode = analysis->addNewRemoteNode(leaveTime, remoteProcessId,
                                    remoteLeaveNodeId, PARADIGM_MPI, RECORD_LEAVE, MPI_COLL,
                                    analysis->getMPIAnalysis().getMPIRank(remoteProcessId));

                             * analysis->newEdge(coll.first, remoteNode);*/

                        }
                    }

                    distributeBlame(analysis, coll.first, total_blame, processWalkCallback);
                }

                delete[] recvBuffer;

                return true;
            }
        };
    }
}

#endif	/* COLLECTIVERULE_HPP */

