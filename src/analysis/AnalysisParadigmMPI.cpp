/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014, 2016, 2017
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * Add MPI specific rules.
 *
 */

#include "mpi/AnalysisParadigmMPI.hpp"
#include "AnalysisEngine.hpp"

#include "mpi/RecvRule.hpp"
#include "mpi/SendRule.hpp"
#include "mpi/CollectiveRule.hpp"
#include "mpi/SendRecvRule.hpp"
#include "mpi/OneToAllRule.hpp"
#include "mpi/AllToOneRule.hpp"
#include "mpi/IRecvRule.hpp"
#include "mpi/ISendRule.hpp"
#include "mpi/WaitAllRule.hpp"
#include "mpi/WaitRule.hpp"

using namespace casita;
using namespace casita::mpi;

AnalysisParadigmMPI::AnalysisParadigmMPI( AnalysisEngine* analysisEngine,
                                          uint32_t        mpiRank,
                                          uint32_t        mpiSize ) :
  IAnalysisParadigm( analysisEngine ),
  //pendingMPIRequests( NULL ),
  mpiRank( mpiRank ),
  mpiSize( mpiSize )
{
  addRule( new RecvRule( 1 ) );
  addRule( new SendRule( 1 ) );
  addRule( new SendRecvRule( 1 ) );
  addRule( new CollectiveRule( 1 ) );
  //addRule( new OneToAllRule( 1 ) );
  //addRule( new AllToOneRule( 1 ) );
  
  // do not add the rules for non-blocking MPI communication, if it shall be ignored
  if ( !(Parser::getInstance().getProgramOptions().ignoreAsyncMpi) )
  {
    addRule( new IRecvRule( 1 ) );
    addRule( new ISendRule( 1 ) );
    addRule( new WaitAllRule( 1 ) );
    addRule( new WaitRule( 1 ) );
    //addRule( new TestRule( 1 ) );
  }
}

AnalysisParadigmMPI::~AnalysisParadigmMPI()
{

}

Paradigm
AnalysisParadigmMPI::getParadigm()
{
  return PARADIGM_MPI;
}

//void
//AnalysisParadigmMPI::handlePostEnter( GraphNode* node )
//{
//  
//}

void
AnalysisParadigmMPI::handlePostLeave( GraphNode* node )
{
  MpiStream* stream = 
    commonAnalysis->getStreamGroup().getMpiStream( node->getStreamId() );

  if( node->isMPIBlocking() ) // handle blocking MPI communication events
  {
    MpiStream::MpiBlockingCommData& commData = stream->getPendingMpiCommRecord();

    if( node->isMPISendRecv() )
    {
      // send partner, receive partner, send tag, receive tag. communicator
      node->setReferencedStreamId( commData.comRef );
      uint32_t *tmpId = new uint32_t[ 4 ];
      tmpId[0] = commData.sendPartnerId;
      tmpId[1] = commData.sendTag;
      tmpId[2] = commData.recvPartnerId;
      tmpId[3] = commData.recvTag;
      node->setData( tmpId );
    }
    else if( node->isMPISend() )
    {
      // partner process, tag, communicator
      node->setReferencedStreamId( commData.sendPartnerId );
      uint32_t *tmpId = new uint32_t[ 2 ];
      tmpId[0] = commData.sendTag;
      tmpId[1] = commData.comRef;
      node->setData( tmpId );
    }
    else if( node->isMPIRecv() )
    {
      // partner process, tag, communicator
      node->setReferencedStreamId( commData.recvPartnerId );
      uint32_t *tmpId = new uint32_t[ 2 ];
      tmpId[0] = commData.recvTag;
      tmpId[1] = commData.comRef;
      node->setData( tmpId );
    }
    else if( node->isMPICollective() )
    {
      /*UTILS_WARNING( "[%"PRIu64"] set communicator for %s (group %u)", 
                       node->getStreamId(), 
                       node->getUniqueName().c_str(),
                       commData.comRef );*/
      
      // MPI_Init and MPI_Finalize do not have a communicator 
      // (referenced stream stays 0)
      if( commData.comRef != UINT32_MAX )
      {
        node->setReferencedStreamId( commData.comRef );
      }
    }
    
    // invalidate communicator
    commData.comRef = UINT32_MAX;
    
    // expose this blocking MPI operation, if an offloading kernel is active
    if( commonAnalysis->getAnalysis( PARADIGM_OFFLOAD ) )
    {
      offload::AnalysisParadigmOffload* ofldAnalysis = 
        ( offload::AnalysisParadigmOffload* ) commonAnalysis->getAnalysis( PARADIGM_OFFLOAD );
      
      ofldAnalysis->getActiveKernelCount();
    }
  }
  else // handle non-blocking MPI communication enter/leave events
  {
    if( node->isMPI_Isend() )
    {
      stream->setMPIIsendNodeData( node );
      return;
    }
    else  if( node->isMPI_Irecv() )
    {
      stream->addPendingMPIIrecvNode( node );
      return;
    }
    else if( node->isMPIWait() )
    {
      stream->setMPIWaitNodeData( node );
      return;
    }
    else if( node->isMPI_Test() )
    {
      stream->handleMPITest( node );
      return;
    }
    else if( node->isMPIWaitall() )
    {
      stream->setMPIWaitallNodeData( node );
      return;
    }
    else if( node->isMPI_Testall() )
    {
      stream->handleMPITestall( node );
      return;
    }
  }

}
