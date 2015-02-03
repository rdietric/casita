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
#include <map>
#include <vector>
#include <sstream>
#include <stdio.h>

#include "graph/Node.hpp"
#include "common.hpp"

#define INVALID_ID 0

namespace casita
{
 typedef const char** ConstCharPtr;

 typedef struct
 {
   int          type;
   const size_t numEntries;
   ConstCharPtr table;
 } FTableEntry;

 typedef struct
 {
   Paradigm paradigm;
   int      type;
 } FunctionDescriptor;

 static const char*       FTABLE_CUDA_COLL_SYNC[] =
 {
   "cuCtxSynchronize",

   "cuMemcpyDtoD",
   "cuMemcpyDtoH",
   "cuMemcpyDtoH_v2",
   "cuMemcpyHtoD",
   "cuMemcpyHtoD_v2",

   "cuMemAlloc",
   "cuMemAlloc_v2",
   "cuMemAllocHost",
   "cuMemAllocPitch",
   "cuMemHostAlloc",

   "cuMemsetD8",
   "cuMemsetD16",
   "cuMemsetD32",
   "cuMemsetD2D8",
   "cuMemsetD2D16",
   "cuMemsetD2D32",

   "cudaSynchronize"
 };

 static const char*       FTABLE_CUDA_SYNC[]         =
 {
   "cuStreamSynchronize",
 };

 static const char*       FTABLE_CUDA_QUERY[]        =
 {
   "cuStreamQuery"
 };

 static const char*       FTABLE_CUDA_LAUNCH[]       =
 {
   "cuLaunch",
   "cuLaunchGrid",
   "cuLaunchGridAsync",
   "cuLaunchKernel",

   "cudaLaunch"
 };

 static const char*       FTABLE_CUDA_EVENT_QUERY[]  =
 {
   "cuEventQuery"
 };

 static const char*       FTABLE_CUDA_EVENT_SYNC[]   =
 {
   "cuEventSynchronize"
 };

 static const char*       FTABLE_CUDA_EVENT_LAUNCH[] =
 {
   "cuEventRecord"
 };

 static const char*       FTABLE_CUDA_STREAM_WAIT[]  =
 {
   "cuStreamWaitEvent"
 };

 static const char*       FTABLE_CUDA_WAITSTATE[]    =
 {
   "__WaitState__"
 };

 static const char*       FTABLE_MPI_RECV[]          =
 {
   "MPI_Recv"
 };

 static const char*       FTABLE_MPI_SEND[]          =
 {
   "MPI_Send"
 };

 static const char*       FTABLE_MPI_WAIT[]          =
 {
   "MPI_Wait"
 };

 static const char*       FTABLE_MPI_ASYNC[]         =
 {
   "MPI_Isend",
   "MPI_Irecv"
 };

 static const char*       FTABLE_MPI_COLL[]          =
 {
   "MPI_Barrier",
   "MPI_Allreduce",
   "MPI_Allgather",
   "MPI_Finalize",
   "MPI_Init"
 };

 static const char*       FTABLE_MPI_ONETOALL[]      =
 {
   "MPI_Scatter",
   "MPI_Bcast"
 };

 static const char*       FTABLE_MPI_ALLTOONE[]      =
 {
   "MPI_Gather",
   "MPI_Reduce",
 };

 static const char*       FTABLE_MPI_SENDRECV[]      =
 {
   "MPI_Sendrecv"
 };

 static const char*       FTABLE_MPI_MISC[]          =
 {

 };

 static const size_t      fTableEntriesCUDA = 9;
 static const FTableEntry fTableCUDA[fTableEntriesCUDA] =
 {
   { CUDA_COLLSYNC, 7, FTABLE_CUDA_COLL_SYNC },
   { CUDA_SYNC, 1, FTABLE_CUDA_SYNC },
   { CUDA_QUERY, 1, FTABLE_CUDA_QUERY },
   { CUDA_KERNEL_LAUNCH, 5, FTABLE_CUDA_LAUNCH },
   { CUDA_EV_QUERY, 1, FTABLE_CUDA_EVENT_QUERY },
   { CUDA_EV_SYNC, 1, FTABLE_CUDA_EVENT_SYNC },
   { CUDA_EV_LAUNCH, 1, FTABLE_CUDA_EVENT_LAUNCH },
   { CUDA_STREAMWAIT, 1, FTABLE_CUDA_STREAM_WAIT },
   { CUDA_WAITSTATE, 1, FTABLE_CUDA_WAITSTATE }
 };

 static const size_t      fTableEntriesMPI = 9;
 static const FTableEntry fTableMPI[fTableEntriesMPI] =
 {
   { MPI_RECV, 1, FTABLE_MPI_RECV },
   { MPI_SEND, 1, FTABLE_MPI_SEND },
   { MPI_WAIT, 1, FTABLE_MPI_WAIT },
   { MPI_COLL, 5, FTABLE_MPI_COLL },
   { MPI_ONETOALL, 2, FTABLE_MPI_ONETOALL },
   { MPI_ALLTOONE, 2, FTABLE_MPI_ALLTOONE },
   { MPI_SENDRECV, 1, FTABLE_MPI_SENDRECV },
   { MPI_MISC, 0, FTABLE_MPI_MISC },
   { MPI_WAITSTATE, 1, FTABLE_CUDA_WAITSTATE }
 };

 static const size_t      fTableEntriesMPIAsync = 1;
 static const FTableEntry fTableMPIAsync[fTableEntriesMPIAsync] =
 {
   { MPI_MISC, 2, FTABLE_MPI_ASYNC }
 };

 class FunctionTable
 {
   public:
     typedef std::vector< uint64_t > FunctionIdList;

     FunctionTable( ) :
       lastFunctionId( INVALID_ID )
     {

     }

     const char*
     getName( uint64_t id )
     {
       std::map< uint64_t, std::string >::iterator iter = functionNameMap.find(
         id );
       if ( iter != functionNameMap.end( ) )
       {
         return iter->second.c_str( );
       }
       else
       {
         return "__unknown__";
       }
     }

     static bool
     getAPIFunctionType( const char* name, FunctionDescriptor* descr,
                         bool deviceStream, bool deviceNullStream,
                         bool ignoreAsyncMpi )
     {
       descr->paradigm = PARADIGM_CPU;
       descr->type     = 0;

       bool set = false;

       for ( size_t i = 0; i < fTableEntriesMPIAsync; ++i )
       {
         FTableEntry entry = fTableMPIAsync[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             if ( ignoreAsyncMpi )
             {
               descr->paradigm = PARADIGM_CPU;
               descr->type     = MISC_CPU;
               return false;
             }
             else
             { throw RTException( "Asynchronous MPI functions are not supported (%s).", name );
             }
           }
         }
       }

       for ( size_t i = 0; i < fTableEntriesCUDA; ++i )
       {
         FTableEntry entry = fTableCUDA[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             descr->paradigm = PARADIGM_CUDA;
             descr->type     = entry.type;
             set = true;
             /* return true; */
           }
         }
       }

       for ( size_t i = 0; i < fTableEntriesMPI; ++i )
       {
         FTableEntry entry = fTableMPI[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             descr->paradigm = PARADIGM_MPI;
             descr->type     = entry.type;
             set = true;
             /* return true; */
           }
         }
       }

       if ( set )
       {
         switch ( descr->paradigm )
         {
           case PARADIGM_CUDA:
             switch ( descr->type )
             {
               case CUDA_COLLSYNC:
                 descr->type = CUDA_COLLSYNC | CUDA_SYNC;
                 return true;

               case CUDA_SYNC:
               case CUDA_KERNEL_LAUNCH:
               case CUDA_EV_LAUNCH:
               case CUDA_EV_SYNC:
               case CUDA_QUERY:
               case CUDA_EV_QUERY:
               case CUDA_STREAMWAIT:
                 return true;
             }

           case PARADIGM_MPI:
             switch ( descr->type )
             {
               case MPI_COLL:
               case MPI_ONETOALL:
               case MPI_ALLTOONE:
               case MPI_WAIT:
               case MPI_SENDRECV:
               case MPI_RECV:
               case MPI_SEND:
               case MPI_MISC:
                 return true;
             }

           default:
             break;
         }
       }
       /* not an MPI or CUDA API function */

       if ( strstr( name, "!$omp" ) )
       {
         descr->paradigm = PARADIGM_OMP;
         if ( strstr( name, "barrier" ) )
         {
           descr->type = OMP_SYNC;
         }
         else
         {
           if ( strstr( name, "target " ) || strstr( name, "targetmap " ) )
           {
             descr->type = OMP_TARGET_OFFLOAD;
           }
           else
           {
             if ( strstr( name, "offloading flush" ) )
             {
               descr->type = OMP_TARGET_FLUSH;
             }
             else
             {
               descr->type = OMP_COMPUTE;
             }
           }
         }
         return true;
       }

       if ( strstr( name, OTF2_OMP_FORKJOIN_INTERNAL ) )
       {
         descr->type     = OMP_FORKJOIN;
         descr->paradigm = PARADIGM_OMP;
         return true;
       }
       /* not an OpenMP function */

       /* kernel ? */
       if ( deviceNullStream )
       {
         descr->type     = ( CUDA_KERNEL | CUDA_SYNC | CUDA_COLLSYNC );
         descr->paradigm = PARADIGM_CUDA;
         return true;
       }

       if ( deviceStream )
       {
         descr->type     = CUDA_KERNEL;
         descr->paradigm = PARADIGM_CUDA;
         return true;
       }

       /* anything else */
       descr->paradigm = PARADIGM_CPU;
       descr->type     = MISC_CPU;
       return false;
     }

   private:
     std::map< uint64_t, std::string > functionNameMap;
     FunctionIdList hostFunctions;
     FunctionIdList kernels;
     uint64_t invalidId;
     uint64_t lastFunctionId;
 };

}
