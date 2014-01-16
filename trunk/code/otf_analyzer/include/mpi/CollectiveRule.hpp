/* 
 * File:   CollectiveRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef COLLECTIVERULE_HPP
#define	COLLECTIVERULE_HPP

#include "AbstractRule.hpp"

namespace cdm
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
            if (!node->isMPICollective())
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
#ifdef MPI_CP_MERGE
            uint32_t lastEnterProcessId = 0, lastLeaveProcessId = 0;
            uint32_t lastEnterRemoteNodeId = 0, lastLeaveRemoteNodeId = 0;
#endif

            for (size_t i = 0; i < recvBufferSize; i += BUFFER_SIZE)
            {
                uint64_t enterTime = ((uint64_t) recvBuffer[i + 1] << 32) + recvBuffer[i];
                uint64_t leaveTime = ((uint64_t) recvBuffer[i + 3] << 32) + recvBuffer[i + 2];

                if (enterTime > lastEnterTime)
                {
                    lastEnterTime = enterTime;
#ifdef MPI_CP_MERGE
                    lastEnterRemoteNodeId = recvBuffer[i + 4];
                    lastEnterProcessId = recvBuffer[i + 6];
#endif
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

            if (collStartTime < lastEnterTime)
            {
                Edge *collRecordEdge = analysis->getEdge(coll.first, coll.second);
                collRecordEdge->makeBlocking();
                coll.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
#ifdef MPI_CP_MERGE
                analysis->getMPIAnalysis().addMPIEdge(coll.first,
                        lastEnterRemoteNodeId, lastEnterProcessId);
#endif
            } else
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
                    }
                }

                coll.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                        total_blame);
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

#endif	/* COLLECTIVERULE_HPP */

