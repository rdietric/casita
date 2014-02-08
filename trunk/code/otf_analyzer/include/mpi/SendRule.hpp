/* 
 * File:   SendRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef SENDRULE_HPP
#define	SENDRULE_HPP

#include <mpi.h>
#include "AbstractRule.hpp"
#include "MPIRulesCommon.hpp"

namespace cdm
{

    class SendRule : public AbstractRule
    {
    public:

        SendRule(int priority) :
        AbstractRule("SendRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            // applied at MPI_Send leave
            if (!node->isMPISend() || !node->isLeave())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair send = ((GraphNode*) node)->getGraphPair();
            uint32_t *data = (uint32_t*) (send.second->getData());
            uint32_t partnerProcessId = *data;

            const int BUFFER_SIZE = 8;
            uint32_t buffer[BUFFER_SIZE];
            uint64_t *bfr64 = (uint64_t*) buffer;

            /* send */
            uint64_t sendStartTime = send.first->getTime();
            uint64_t sendEndTime = send.second->getTime();

            uint32_t partnerMPIRank = analysis->getMPIAnalysis().getMPIRank(partnerProcessId);
            memcpy(bfr64 + 0, &sendStartTime, sizeof (uint64_t));
            memcpy(bfr64 + 1, &sendEndTime, sizeof (uint64_t));

            buffer[4] = send.first->getId();
            buffer[5] = send.second->getId();
            buffer[BUFFER_SIZE - 1] = send.second->getType();
            MPI_CHECK(MPI_Send(buffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRank,
                    0, MPI_COMM_WORLD));

            /* receive */
            MPI_Status status;
            uint64_t recvStartTime = 0, recvEndTime = 0;
            MPI_CHECK(MPI_Recv(buffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRank,
                    0, MPI_COMM_WORLD, &status));
            recvStartTime = bfr64[0];
            recvEndTime = bfr64[1];
            //int partnerType = buffer[BUFFER_SIZE - 1];

            /* compute wait states */
            if ((sendStartTime <= recvStartTime))
            {
                if (sendStartTime < recvStartTime)
                {
                    Edge *sendRecordEdge = analysis->getEdge(send.first, send.second);
                    sendRecordEdge->makeBlocking();
                    send.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
                }
#ifdef MPI_CP_MERGE
                analysis->getMPIAnalysis().addMPIEdge(send.first, buffer[4], partnerProcessId);
#endif
            } else
            {
                distributeBlame(analysis, send.first, sendStartTime - recvStartTime);

                analysis->getMPIAnalysis().addRemoteMPIEdge(send.first, buffer[4], partnerProcessId);
            }

            uint32_t recvLeaveId = buffer[5];
            GraphNode *remoteNode = analysis->addNewRemoteNode(recvEndTime, partnerProcessId,
                    recvLeaveId, PARADIGM_MPI, RECORD_LEAVE, MPI_RECV, partnerMPIRank);
            analysis->newEdge(remoteNode, send.second);

            return true;
        }
    };

}

#endif	/* SENDRULE_HPP */

