/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <iostream>

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

namespace casita
{
 static uint32_t globalNodeId = 0;

 typedef uint32_t LocalID;
 typedef uint64_t GlobalID;

 enum NodeRecordType
 {
   RECORD_ATOMIC,
   RECORD_ENTER,
   RECORD_LEAVE
 };

 enum Paradigm
 {
   PARADIGM_CPU = ( 1 << 0 ),
   PARADIGM_CUDA = ( 1 << 1 ),
   PARADIGM_MPI = ( 1 << 2 ),
   PARADIGM_OMP = ( 1 << 3 ),
   PARADIGM_VT = ( 1 << 4 ),

   PARADIGM_COMPUTE_LOCAL = ( PARADIGM_CPU | PARADIGM_CUDA | PARADIGM_OMP ),
   PARADIGM_ALL =
     ( PARADIGM_CPU | PARADIGM_CUDA | PARADIGM_MPI | PARADIGM_OMP | PARADIGM_VT )
 };

 const size_t NODE_PARADIGM_COUNT = 5;
 const size_t NODE_PARADIGM_INVALID = ( 1 << 5 );

 enum NodeTypeMisc
 {
   MISC_CPU = ( 1 << 1 ),
   MISC_PROCESS = ( 1 << 2 )
 };

 enum NodeTypeCUDA
 {
   CUDA_SYNC = ( 1 << 1 ),
   CUDA_COLLSYNC = ( 1 << 2 ),
   CUDA_WAITSTATE = ( 1 << 3 ),
   CUDA_KERNEL = ( 1 << 4 ),
   CUDA_KERNEL_LAUNCH = ( 1 << 5 ),
   CUDA_EV_SYNC = ( 1 << 6 ),
   CUDA_EV_LAUNCH = ( 1 << 7 ),
   CUDA_EV_QUERY = ( 1 << 8 ),
   CUDA_QUERY = ( 1 << 9 ),
   CUDA_STREAMWAIT = ( 1 << 10 )
 };

 enum NodeTypeMPI
 {
   MPI_RECV = ( 1 << 1 ),
   MPI_SEND = ( 1 << 2 ),
   MPI_WAIT = ( 1 << 3 ),
   MPI_COLL = ( 1 << 4 ),
   MPI_SENDRECV = ( 1 << 5 ),
   MPI_MISC = ( 1 << 6 ),
   MPI_EXIT = ( 1 << 7 ),
   MPI_WAITSTATE = ( 1 << 8 ),
   MPI_ONETOALL = ( 1 << 9 ),
   MPI_ALLTOONE = ( 1 << 10 )
 };

 enum NodeTypeOMP
 {
   OMP_SYNC = ( 1 << 1 ),
   OMP_PAR_REGION = ( 1 << 2 ),
   OMP_COMPUTE = ( 1 << 3 ),
   OMP_WAITSTATE = ( 1 << 4 ),
   OMP_TARGET_OFFLOAD = ( 1 << 5 ),
   OMP_TARGET_FLUSH = ( 1 << 6 )
 };

 enum NodeTypeVT
 {
   VT_FLUSH = ( 1 << 1 )
 };

 typedef struct
 {
   NodeTypeCUDA type;
   const char* str;
 } TypeStrEntryCUDA;

 typedef struct
 {
   NodeTypeMPI type;
   const char* str;
 } TypeStrEntryMPI;

 typedef struct
 {
   NodeTypeOMP type;
   const char* str;
 } TypeStrEntryOMP;

 typedef struct
 {
   NodeTypeVT type;
   const char* str;
 } TypeStrEntryVT;

 static const size_t numTypeStrEntriesCUDA = 11;
 static const TypeStrEntryCUDA typeStrTableCUDA[numTypeStrEntriesCUDA] =
 {
   { CUDA_COLLSYNC, "cuda_collsync" },
   { CUDA_SYNC, "cuda_sync" },
   { CUDA_EV_SYNC, "cuda_event_sync" },
   { CUDA_KERNEL, "cuda_kernel" },
   { CUDA_KERNEL_LAUNCH, "cuda_launch" },
   { CUDA_EV_LAUNCH, "cuda_event_launch" },
   { CUDA_WAITSTATE, "cuda_waitstate" },
   { CUDA_QUERY, "cuda_query" },
   { CUDA_EV_QUERY, "cuda_event_query" },
   { CUDA_STREAMWAIT, "cuda_streamwait" }
 };

 static const size_t numTypeStrEntriesMPI = 10;
 static const TypeStrEntryMPI typeStrTableMPI[numTypeStrEntriesMPI] =
 {
   { MPI_RECV, "mpi_recv" },
   { MPI_SEND, "mpi_send" },
   { MPI_WAIT, "mpi_wait" },
   { MPI_COLL, "mpi_coll" },
   { MPI_ONETOALL, "mpi_one_to_all" },
   { MPI_ALLTOONE, "mpi_all_to_one" },
   { MPI_SENDRECV, "mpi_sendrecv" },
   { MPI_MISC, "mpi_misc" },
   { MPI_EXIT, "mpi_exit" },
   { MPI_WAITSTATE, "mpi_waitstate" }
 };

 static const size_t numTypeStrEntriesOMP = 5;
 static const TypeStrEntryOMP typeStrTableOMP[numTypeStrEntriesOMP] =
 {
   { OMP_SYNC, "omp_sync" },
   { OMP_PAR_REGION, "omp_parallel_region" },
   { OMP_COMPUTE, "omp_compute" },
   { OMP_TARGET_OFFLOAD, "omp_target_offload" },
   { OMP_TARGET_FLUSH, "omp_target_flush" }
 };

 static const size_t numTypeStrEntriesVT = 1;
 static const TypeStrEntryVT typeStrTableVT[numTypeStrEntriesVT] =
 {
   { VT_FLUSH, "vt_flush" }
 };

 static const char NAME_WAITSTATE[] = "WaitState";
 static const char NAME_MPI_INIT[] = "MPI_Init";
 static const char NAME_MPI_FINALIZE[] = "MPI_Finalize";

 class Node
 {
   public:

     bool
     isEnter( ) const
     {
       return recordType == RECORD_ENTER;
     }

     bool
     isLeave( ) const
     {
       return recordType == RECORD_LEAVE;
     }

     bool
     isAtomic( ) const
     {
       return recordType == RECORD_ATOMIC;
     }

     bool
     isCPU( ) const
     {
       return paradigm & PARADIGM_CPU;
     }

     bool
     isCUDA( ) const
     {
       return paradigm & PARADIGM_CUDA;
     }

     bool
     isMPI( ) const
     {
       return paradigm & PARADIGM_MPI;
     }

     bool
     isOMP( ) const
     {
       return paradigm & PARADIGM_OMP;
     }

     bool
     isVT( ) const
     {
       return paradigm & PARADIGM_VT;
     }

     bool
     isCUDASync( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_SYNC );
     }

     bool
     isCUDACollSync( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_COLLSYNC );
     }

     bool
     isCUDAEventSync( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_EV_SYNC );
     }

     bool
     isWaitstate( ) const
     {
       return ( isOMP( ) && ( nodeType & OMP_WAITSTATE ) ) ||
              ( isCUDA( ) && ( nodeType & CUDA_WAITSTATE ) ) ||
              ( isMPI( ) && ( nodeType & MPI_WAITSTATE ) );
     }

     bool
     isPureWaitstate( ) const
     {

       return isWaitstate( ) &&
              ( strcmp( name.c_str( ), NAME_WAITSTATE ) == 0 );
     }

     bool
     isProcess( ) const
     {
       return isAtomic( ) && ( nodeType & MISC_PROCESS );
     }

     bool
     isCUDAKernel( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_KERNEL );
     }

     bool
     isCUDAKernelLaunch( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_KERNEL_LAUNCH );
     }

     bool
     isCUDAEventLaunch( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_EV_LAUNCH );
     }

     bool
     isCUDAQuery( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_QUERY );
     }

     bool
     isCUDAEventQuery( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_EV_QUERY );
     }

     bool
     isCUDAStreamWaitEvent( ) const
     {

       return isCUDA( ) && ( nodeType & CUDA_STREAMWAIT );
     }

     static bool
     isCUDAEventType( Paradigm paradigm, int type )
     {

       return ( paradigm == PARADIGM_CUDA ) &&
              ( ( type & CUDA_EV_LAUNCH ) ||
                ( type & CUDA_EV_SYNC ) ||
                ( type & CUDA_EV_QUERY ) ||
                ( type & CUDA_STREAMWAIT ) );
     }

     bool
     isMPIRecv( ) const
     {

       return isMPI( ) && ( nodeType & MPI_RECV );
     }

     bool
     isMPISend( ) const
     {

       return isMPI( ) && ( nodeType & MPI_SEND );
     }

     bool
     isMPIWait( ) const
     {

       return isMPI( ) && ( nodeType & MPI_WAIT );
     }

     bool
     isMPIInit( ) const
     {

       return isMPI( ) && ( nodeType & MPI_COLL ) &&
              ( strcmp( name.c_str( ), NAME_MPI_INIT ) == 0 );
     }

     bool
     isMPICollective( ) const
     {

       return isMPI( ) && ( nodeType & MPI_COLL );
     }

     bool
     isMPIOneToAll( ) const
     {

       return isMPI( ) && ( nodeType & MPI_ONETOALL );
     }

     bool
     isMPIAllToOne( ) const
     {

       return isMPI( ) && ( nodeType & MPI_ALLTOONE );
     }

     bool
     isMPISendRecv( ) const
     {

       return isMPI( ) && ( nodeType & MPI_SENDRECV );
     }

     bool
     isMPIFinalize( ) const
     {

       return isMPI( ) && ( nodeType & MPI_COLL ) &&
              ( strcmp( name.c_str( ), NAME_MPI_FINALIZE ) == 0 );
     }

     bool
     isOMPSync( ) const
     {
       return isOMP( ) && ( nodeType & OMP_SYNC );
     }

     bool
     isOMPParallelRegion( ) const
     {
       return isOMP( ) && ( nodeType & OMP_PAR_REGION );
     }

     bool
     isOMPCompute( ) const
     {
       return isOMP( ) && ( nodeType & OMP_COMPUTE );
     }

     bool
     isOMPTargetOffload( ) const
     {
       return isOMP( ) && ( nodeType & OMP_TARGET_OFFLOAD );
     }

     bool
     isOMPTargetFlush( ) const
     {
       return isOMP( ) && ( nodeType & OMP_TARGET_FLUSH );
     }
     
     static const std::string
     typeToStr( Paradigm paradigm, int type )
     {
       size_t i;
       std::stringstream stream;

       switch ( paradigm )
       {
         case PARADIGM_CUDA:
           for ( i = 0; i < numTypeStrEntriesCUDA; ++i )
           {
             if ( typeStrTableCUDA[i].type & type )
             {
               stream << typeStrTableCUDA[i].str << ",";
             }
           }
           break;

         case PARADIGM_MPI:
           for ( i = 0; i < numTypeStrEntriesMPI; ++i )
           {
             if ( typeStrTableMPI[i].type & type )
             {
               stream << typeStrTableMPI[i].str << ",";
             }
           }
           break;

         case PARADIGM_OMP:
           for ( i = 0; i < numTypeStrEntriesOMP; ++i )
           {
             if ( typeStrTableOMP[i].type & type )
             {
               stream << typeStrTableOMP[i].str << ",";
             }
           }
           break;

         case PARADIGM_VT:
           for ( i = 0; i < numTypeStrEntriesVT; ++i )
           {
             if ( typeStrTableVT[i].type & type )
             {
               stream << typeStrTableVT[i].str << ",";
             }
           }
           break;

         case PARADIGM_CPU:
           stream << "cpu";
           break;

         default:
           stream << "<unknown>";
       }

       return stream.str( );
     }

     static bool
     compareLess( const Node* n1, const Node* n2 )
     {
       uint64_t time1 = n1->getTime( );
       uint64_t time2 = n2->getTime( );

       /* printf("< (%s, %s)\n", n1->getUniqueName().c_str(),
        * n2->getUniqueName().c_str()); */

       if ( time1 != time2 )
       {
         return time1 < time2;
       }
       else
       {
         int type1 = n1->getType( );
         int type2 = n2->getType( );

         NodeRecordType recordType1 = n1->getRecordType( );
         NodeRecordType recordType2 = n2->getRecordType( );

         if ( recordType1 == RECORD_ATOMIC && recordType2 != RECORD_ATOMIC )
         {
           return true;
         }

         if ( recordType1 != RECORD_ATOMIC && recordType2 == RECORD_ATOMIC )
         {
           return false;
         }

         Paradigm paradigm1 = n1->getParadigm( );
         Paradigm paradigm2 = n2->getParadigm( );

         /**\todo fix me*/
         if ( paradigm1 != paradigm2 )
         {
           return paradigm1 < paradigm2;
         }

         /* nodes from same or different streams */
         if ( n1->getStreamId( ) == n2->getStreamId( ) )
         {
           /* nodes from same or different functions */
           if ( ( ( n1->getFunctionId( ) > 0 ) &&
                  ( n2->getFunctionId( ) > 0 ) &&
                  ( n1->getFunctionId( ) == n2->getFunctionId( ) ) ) ||
                ( ( n1->getFunctionId( ) == n2->getFunctionId( ) ) &&
                  strcmp( n1->getName( ), n2->getName( ) ) == 0 ) )
           {
             if ( ( recordType1 == RECORD_LEAVE ) &&
                  ( recordType2 == RECORD_ENTER ) )
             {
               return false;
             }

             if ( ( recordType1 == RECORD_ENTER ) &&
                  ( recordType2 == RECORD_LEAVE ) )
             {
               return true;
             }
           }
           else
           {
             if ( ( recordType1 == RECORD_LEAVE ) &&
                  ( recordType2 == RECORD_ENTER ) )
             {
               return true;
             }

             if ( ( recordType1 == RECORD_ENTER ) &&
                  ( recordType2 == RECORD_LEAVE ) )
             {
               return false;
             }
           }
         }
         else
         {
           if ( paradigm1 & PARADIGM_CUDA )
           {
             if ( ( type1 & CUDA_KERNEL ) && ( type2 & CUDA_SYNC ) )
             {
               return true;
             }

             if ( ( type2 & CUDA_KERNEL ) && ( type1 & CUDA_SYNC ) )
             {
               return false;
             }
           }
         }

         if ( paradigm1 & PARADIGM_MPI )
         {
           if ( type1 & MPI_EXIT )
           {
             return false;
           }

           if ( type2 & MPI_EXIT )
           {
             return true;
           }
         }

         return n1->getStreamId( ) > n2->getStreamId( );
       }
     }

     Node( uint64_t time, uint64_t streamId, const std::string name,
           Paradigm paradigm, NodeRecordType recordType, int nodeType ) :
       time( time ),
       streamId( streamId ),
       functionId( 0 ),
       name( name ),
       recordType( recordType ),
       paradigm( paradigm ),
       nodeType( nodeType ),
       link( NULL ),
       referencedStream( 0 )
     {

       id = ++globalNodeId;
     }

     virtual
     ~Node( )
     {
     }

     uint32_t
     getId( ) const
     {
       return id;
     }

     uint64_t
     getTime( ) const
     {
       return time;
     }

     uint64_t
     getStreamId( ) const
     {
       return streamId;
     }

     const char*
     getName( ) const
     {
       return name.c_str( );
     }

     virtual void
     setName( const std::string newName )
     {
       name = newName;
     }

     virtual const std::string
     getUniqueName( ) const
     {
       std::stringstream sstream;
       sstream << name << ".";

       if ( recordType == RECORD_ENTER )
       {
         sstream << "enter.";
       }

       if ( recordType == RECORD_LEAVE )
       {
         sstream << "leave.";
       }

       sstream << id << "." << time << "." << streamId;

       return sstream.str( );
     }

     const char*
     toString( ) const
     {
       return getUniqueName( ).c_str( );
     }

     uint64_t
     getFunctionId( ) const
     {
       return functionId;
     }

     void
     setFunctionId( uint64_t newId )
     {
       functionId = newId;
     }

     NodeRecordType
     getRecordType( ) const
     {
       return recordType;
     }

     Paradigm
     getParadigm( ) const
     {
       return paradigm;
     }

     bool
     hasParadigm( Paradigm p ) const
     {
       return paradigm & p;
     }

     int
     getType( ) const
     {
       return nodeType;
     }

     void
     setReferencedStreamId( uint64_t streamId )
     {
       referencedStream = streamId;
     }

     bool
     referencesStream( uint64_t streamId ) const
     {
       if ( streamId == this->streamId )
       {
         return false;
       }

       if ( isCUDACollSync( ) || ( referencedStream == streamId ) )
       {
         return true;
       }

       return false;
     }

     uint64_t
     getReferencedStreamId( ) const
     {

       return referencedStream;
     }

     virtual bool
     isEventNode( ) const
     {
       return false;
     }

     void
     setLink( Node* link )
     {
       if ( this->link )
       {
         assert( 0 );
       }
       this->link = link;
     }

     Node*
     getLink( ) const
     {
       return link;
     }

     void
     setCounter( uint32_t ctrId, uint64_t value )
     {
       counters[ctrId] = value;
     }

     void
     incCounter( uint32_t ctrId, uint64_t value )
     {
       if ( counters.find( ctrId ) != counters.end( ) )
       {
         counters[ctrId] += value;
       }
       else
       {
         counters[ctrId] = value;
       }
     }

     uint64_t
     getCounter( uint32_t ctrId, bool* valid ) const
     {
       std::map< uint32_t, uint64_t >::const_iterator iter =
         counters.find( ctrId );
       if ( iter == counters.end( ) )
       {
         if ( valid )
         {
           *valid = false;
         }
         return 0;
       }

       if ( valid )
       {
         *valid = true;
       }

       return iter->second;
     }

     void
     removeCounter( uint32_t ctrId )
     {
       counters.erase( ctrId );
     }

     void
     removeCounters( )
     {
       counters.clear( );
     }

   protected:
     uint32_t id;
     uint64_t time;
     uint64_t streamId;
     uint64_t functionId;
     std::string name;

     NodeRecordType recordType;
     Paradigm paradigm;
     int nodeType;

     Node* link;
     /**
      * Link mappings:
      * - KernelLaunch/enter > Kernel/enter
      * - Kernel/enter > KernelLaunch/enter
      * - EventLaunch/leave > KernelLaunch/leave
      * - EventQuery/leave > EventQuery/leave
      * - StreamWaitEvent/leave > EventLaunch/leave
      */
     uint64_t referencedStream;
     std::map< uint32_t, uint64_t > counters;
 };

 typedef struct
 {

   bool
   operator()( const Node* n1, const Node* n2 ) const
   {
     return Node::compareLess( n1, n2 );
   }

 } nodeCompareLess;
}
