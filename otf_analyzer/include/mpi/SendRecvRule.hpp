/* 
 * File:   SendRecvRule.hpp
 * Author: felix
 *
 * Created on May 8, 2013, 2:09 PM
 */

#ifndef SENDRECVRULE_HPP
#define	SENDRECVRULE_HPP

#include "AbstractRule.hpp"

namespace cdm
{

    class SendRecvRule : public AbstractRule
    {
    public:

        SendRecvRule(int priority) :
        AbstractRule("SendRecvRule", priority)
        {

        }

        bool apply(AnalysisEngine *analysis, Node *node)
        {
            // applied at MPI send recv leave
            if (!node->isMPISendRecv())
                return false;

            // get the complete execution
            GraphNode::GraphNodePair sendRecv = ((GraphNode*) node)->getGraphPair();

            uint32_t *data = (uint32_t*) (sendRecv.second->getData());

            uint32_t partnerProcessIdRecv = node->getReferencedProcessId();
            uint32_t partnerProcessIdSend = *data;

            const int BUFFER_SIZE = 8;
            uint32_t sendBuffer[BUFFER_SIZE], recvBuffer[BUFFER_SIZE];
            uint64_t *sendBfr64 = (uint64_t*) sendBuffer;
            uint64_t *recvBfr64 = (uint64_t*) recvBuffer;

            uint64_t myStartTime = sendRecv.first->getTime();
            uint64_t myEndTime = sendRecv.second->getTime();

            /* prepare send buffer */
            memcpy(sendBfr64 + 0, &myStartTime, sizeof (uint64_t));
            memcpy(sendBfr64 + 1, &myEndTime, sizeof (uint64_t));
            sendBuffer[4] = sendRecv.first->getId();
            sendBuffer[5] = sendRecv.second->getId();

            /* send + recv */
            uint32_t partnerMPIRankRecv = analysis->getMPIAnalysis().getMPIRank(partnerProcessIdRecv);
            uint32_t partnerMPIRankSend = analysis->getMPIAnalysis().getMPIRank(partnerProcessIdSend);
            MPI_Status status;

            /* round 1: send same direction. myself == send */

            MPI_CHECK(MPI_Sendrecv(sendBuffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRankSend, 0,
                    recvBuffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRankRecv, 0,
                    MPI_COMM_WORLD, &status));

            /* evaluate receive buffer */
            uint64_t otherStartTime = recvBfr64[0];
            uint64_t otherEndTime = recvBfr64[1];
            uint32_t otherEnterId = recvBuffer[4];
            uint32_t otherLeaveId = recvBuffer[5];

            /* compute wait states */
            if ((myStartTime <= otherStartTime))
            {
                if (myStartTime < otherStartTime)
                {
                    Edge *sendRecordEdge = analysis->getEdge(sendRecv.first, sendRecv.second);
                    sendRecordEdge->makeBlocking();
                    sendRecv.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
                }
#ifdef MPI_CP_MERGE
                analysis->getMPIAnalysis().addMPIEdge(sendRecv.first, otherEnterId, partnerProcessIdRecv);
#endif
            } else
            {
                sendRecv.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                        myStartTime - otherStartTime);

                analysis->getMPIAnalysis().addRemoteMPIEdge(sendRecv.first, otherEnterId, partnerProcessIdRecv);
            }

            GraphNode *remoteNode = analysis->addNewRemoteNode(otherEndTime, partnerProcessIdRecv,
                    otherLeaveId, NT_RT_LEAVE | NT_FT_MPI_RECV, partnerMPIRankRecv);
            analysis->newEdge(remoteNode, sendRecv.second, false, NULL);


            /* round 2: send reverse direction. myself == recv */

            MPI_CHECK(MPI_Sendrecv(sendBuffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRankRecv, 0,
                    recvBuffer, BUFFER_SIZE, MPI_INTEGER4, partnerMPIRankSend, 0,
                    MPI_COMM_WORLD, &status));

            otherStartTime = recvBfr64[0];
            otherEndTime = recvBfr64[1];
            otherEnterId = recvBuffer[4];
            otherLeaveId = recvBuffer[5];

            // compute wait states and edges
            if (myStartTime < otherStartTime)
            {
                Edge *recvRecordEdge = analysis->getEdge(sendRecv.first, sendRecv.second);
                recvRecordEdge->makeBlocking();
                sendRecv.first->setCounter(analysis->getCtrTable().getCtrId(CTR_WAITSTATE), 1);
#ifdef MPI_CP_MERGE
                analysis->getMPIAnalysis().addMPIEdge(sendRecv.first, otherEnterId, partnerProcessIdSend);
#endif
            }

            if (myStartTime > otherStartTime)
            {
                sendRecv.first->incCounter(analysis->getCtrTable().getCtrId(CTR_BLAME),
                        myStartTime - otherStartTime);

                analysis->getMPIAnalysis().addRemoteMPIEdge(sendRecv.first, otherEnterId, partnerProcessIdSend);
            }

#ifdef MPI_CP_MERGE
            analysis->getMPIAnalysis().addMPIEdge(sendRecv.second, otherLeaveId, partnerProcessIdSend);
#endif
            remoteNode = analysis->addNewRemoteNode(otherLeaveId, partnerProcessIdSend,
                    otherLeaveId, NT_RT_LEAVE | NT_FT_MPI_SEND, partnerMPIRankSend);
            analysis->newEdge(sendRecv.second, remoteNode, false, NULL);

            return true;
        }
    };

}

#endif	/* SENDRECVRULE_HPP */

