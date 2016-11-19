/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2016,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 * What this file does:
 * This file matches the name of a read event to an event type & paradigm.
 * - mark CPU functions
 * - mark async MPI communication
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
#include "Parser.hpp"

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
   Paradigm   paradigm;
   int        functionType;
   RecordType recordType;
 } FunctionDescriptor;

 ///////////////// CUDA functions ////////////////
 static const char* FTABLE_CUDA_COLL_SYNC[] =
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

 static const char* FTABLE_CUDA_SYNC[]         =
 {
   "cuStreamSynchronize",
 };

 static const char* FTABLE_CUDA_QUERY[]        =
 {
   "cuStreamQuery"
 };

 static const char* FTABLE_CUDA_LAUNCH[]       =
 {
   "cuLaunch",
   "cuLaunchGrid",
   "cuLaunchGridAsync",
   "cuLaunchKernel",

   "cudaLaunch"
 };

 static const char* FTABLE_CUDA_EVENT_QUERY[]  =
 {
   "cuEventQuery"
 };

 static const char* FTABLE_CUDA_EVENT_SYNC[]   =
 {
   "cuEventSynchronize"
 };

 static const char* FTABLE_CUDA_EVENT_LAUNCH[] =
 {
   "cuEventRecord"
 };

 static const char* FTABLE_CUDA_STREAM_WAIT[]  =
 {
   "cuStreamWaitEvent"
 };

 static const char* FTABLE_GPU_WAITSTATE[]     =
 {
   "__WaitState__"
 };
 
  ///////////////// OpenCL functions ////////////////
 static const char* FTABLE_OPENCL_QUEUE_SYNC[] =
 {
   "clFinish"
 };
 
 static const char* FTABLE_OPENCL_ENQUEUE_BUFFER[] =
 {
   "clEnqueueReadBuffer",
   "clEnqueueWriteBuffer"
 };
 
 static const char* FTABLE_OPENCL_ENQUEUE[]       =
 {
   "clEnqueueNDRangeKernel"
 };
 
 static const char* FTABLE_OPENCL_EVENT_QUERY[]  =
 {
   "clGetEventInfo"
 };
 
  static const char* FTABLE_OPENCL_EVENT_SYNC[]   =
 {
   "clWaitForEvents"
 };

  ///////////////// MPI functions ////////////////
 static const char* FTABLE_MPI_INIT[]          =
 {
   "MPI_Init",
   "MPI_Init_thread"
 };
 
 static const char* FTABLE_MPI_FINALIZE[]      =
 {
   "MPI_Finalize"
 };
 
 static const char* FTABLE_MPI_RECV[]          =
 {
   "MPI_Recv"
 };

 static const char* FTABLE_MPI_SEND[]          =
 {
   "MPI_Send",
   "MPI_Ssend",
   "MPI_Bsend",
   "MPI_Rsend"
 };

 static const char* FTABLE_MPI_WAIT[]          =
 {
   "MPI_Wait"
 };

 static const char* FTABLE_MPI_WAITALL[]       =
 {
   "MPI_Waitall"
 };

 static const char* FTABLE_MPI_ISEND[]         =
 {
   "MPI_Isend",
   "MPI_Ibsend"
 };

 static const char* FTABLE_MPI_IRECV[]         =
 {
   "MPI_Irecv"
 };

 static const char* FTABLE_MPI_COLL[]          =
 {
   "MPI_Barrier",
   "MPI_Allreduce",
   "MPI_Allgather",
   "MPI_Allgatherv",
   "MPI_Alltoall",
   "MPI_Alltoallv",
   "MPI_Reduce_scatter",
   "MPI_Reduce",  // allToOne
   "MPI_Gather",  //allToOne
   "MPI_Scatter", // oneToAll
   "MPI_Bcast"    // oneToAll
 };

 /* oneToAll rule might be broken, hence use collective rule
 static const char* FTABLE_MPI_ONETOALL[]      =
 {
   "MPI_Scatter",
   "MPI_Bcast"
 };*/

 /* allToOne rule is broken, hence use collective rule
 static const char* FTABLE_MPI_ALLTOONE[]      =
 {
   "MPI_Gather",
   "MPI_Reduce"
 };*/

 static const char* FTABLE_MPI_SENDRECV[]      =
 {
   "MPI_Sendrecv"
 };

 static const char* FTABLE_MPI_MISC[]          =
 {
   //"MPI_Bsend", "MPI_Cancel", "MPI_Probe"
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
   { CUDA_WAITSTATE, 1, FTABLE_GPU_WAITSTATE }
 };
 
 static const size_t      fTableEntriesOpenCL = 6;
 static const FTableEntry fTableOpenCL[fTableEntriesOpenCL] =
 {
   { OCL_SYNC_QUEUE, 1, FTABLE_OPENCL_QUEUE_SYNC },
   { OCL_ENQUEUE_BUFFER, 2, FTABLE_OPENCL_ENQUEUE_BUFFER },
   { OCL_ENQUEUE_KERNEL, 1, FTABLE_OPENCL_ENQUEUE },
   { OCL_QUERY_EVENT, 1, FTABLE_OPENCL_EVENT_QUERY },
   { OCL_SYNC_EVENT, 1, FTABLE_OPENCL_EVENT_SYNC },
   { OCL_WAITSTATE, 1, FTABLE_GPU_WAITSTATE }
 };

 static const size_t      fTableEntriesMPI = 8;
 static const FTableEntry fTableMPI[fTableEntriesMPI] =
 {
   { MPI_INIT, 2, FTABLE_MPI_INIT },
   { MPI_FINALIZE, 1, FTABLE_MPI_FINALIZE },
   { MPI_RECV, 1, FTABLE_MPI_RECV },
   { MPI_SEND, 4, FTABLE_MPI_SEND },
   { MPI_COLL, 11, FTABLE_MPI_COLL },
//   { MPI_ONETOALL, 2, FTABLE_MPI_ONETOALL }, // oneToAll rule might be broken
//   { MPI_ALLTOONE, 0, FTABLE_MPI_ALLTOONE }, // allToOne rule is broken
   { MPI_SENDRECV, 1, FTABLE_MPI_SENDRECV },
   { MPI_MISC, 0, FTABLE_MPI_MISC },
   { MPI_WAITSTATE, 1, FTABLE_GPU_WAITSTATE }
 };

 static const size_t      fTableEntriesMPIAsync = 4;
 static const FTableEntry fTableMPIAsync[fTableEntriesMPIAsync] =
 {
   { MPI_WAIT, 1, FTABLE_MPI_WAIT },
   { MPI_WAITALL, 1, FTABLE_MPI_WAITALL },
   { MPI_IRECV, 1, FTABLE_MPI_IRECV },
   { MPI_ISEND, 2, FTABLE_MPI_ISEND }
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

     /**
      * This function determines the type of the event. The record type has to
      * be already set to get correct results (descr->recordType).
      * 
      * @param name
      * @param descr the function descriptor (the record type must be set)
      * @param deviceStream
      * @param deviceNullStream
      * @return true, if it maps to an internal node
      */
     static bool
     getAPIFunctionType( const char* name, FunctionDescriptor* descr,
                         bool deviceStream, bool deviceNullStream )
     {
       descr->paradigm     = PARADIGM_CPU;
       descr->functionType = 0;

       bool set = false; // remember if paradigm and function type has been set

       // check for non-blocking MPI communication
       for ( size_t i = 0; i < fTableEntriesMPIAsync; ++i )
       {
         FTableEntry entry = fTableMPIAsync[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           // if we found a non-blocking MPI event
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             if ( Parser::getInstance().getProgramOptions().ignoreAsyncMpi )
             {
               descr->paradigm     = PARADIGM_CPU;
               descr->functionType = MISC_CPU;
               return false;
             }
             else
             {
               /*
               // the enter events of non-blocking MPI operations are currently 
               // not associated with information
               if( entry.type == MPI_ISEND || entry.type == MPI_IRECV ||
                   entry.type == MPI_WAIT  || entry.type == MPI_WAITALL )
               {
                 if( descr->recordType == RECORD_ENTER )
                 {
                   descr->paradigm     = PARADIGM_CPU;
                   descr->functionType = MISC_CPU;
                   return false;
                 }
                 else
                 {
                   descr->recordType = RECORD_SINGLE;
                 }
               }

               */
               descr->paradigm     = PARADIGM_MPI;
               descr->functionType = entry.type;
               return true;
             }
           }
         }
       }

       // check for CUDA functions
       for ( size_t i = 0; i < fTableEntriesCUDA; ++i )
       {
         FTableEntry entry = fTableCUDA[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           // if we found a CUDA function
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             if ( Parser::getInstance().getProgramOptions().ignoreCUDA )
             {
               descr->paradigm     = PARADIGM_CPU;
               descr->functionType = MISC_CPU;
               return false;
             }
             
             /* CUDA event record enter nodes do not carry any additional information
             if( entry.type == CUDA_EV_LAUNCH )
             {
               if( descr->recordType == RECORD_ENTER )
               {
                 descr->paradigm     = PARADIGM_CPU;
                 descr->functionType = MISC_CPU;
                 return false;
               }
               else
               {
                 descr->recordType = RECORD_SINGLE;
               }
             }*/
             
             descr->paradigm     = PARADIGM_CUDA;
             descr->functionType = entry.type;
             set = true;
           }
         }
       }
       
       for ( size_t i = 0; i < fTableEntriesOpenCL; ++i )
       {
         FTableEntry entry = fTableOpenCL[i];
         for ( size_t j = 0; j < entry.numEntries; ++j )
         {
           if ( strcmp( entry.table[j], name ) == 0 )
           {
             descr->paradigm     = PARADIGM_OCL;
             descr->functionType = entry.type;
             set = true;
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
             descr->paradigm     = PARADIGM_MPI;
             descr->functionType = entry.type;
             set = true;
           }
         }
       }

       if ( set )
       {
         switch ( descr->paradigm )
         {
           case PARADIGM_CUDA:
             switch ( descr->functionType )
             {
               case CUDA_COLLSYNC:
                 descr->functionType = CUDA_COLLSYNC | CUDA_SYNC;
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
             
           case PARADIGM_OCL:
             switch ( descr->functionType )
             {
               case OCL_SYNC_QUEUE:
               case OCL_SYNC_EVENT:
               case OCL_QUERY_EVENT:
               case OCL_ENQUEUE_KERNEL:
               case OCL_ENQUEUE_BUFFER:
                 return true;
             }

           // only blocking MPI (non-blocking is handled separately)
           case PARADIGM_MPI:
             switch ( descr->functionType )
             {
               case MPI_INIT:
               case MPI_FINALIZE:
                 descr->functionType |= MPI_ALLRANKS; // these two are always executed on all ranks
                 descr->functionType |= MPI_COLL;     // these two are collectives
               case MPI_COLL:
               case MPI_ONETOALL:
               case MPI_ALLTOONE:
               case MPI_SENDRECV:
               case MPI_RECV:
               case MPI_SEND:
                 descr->functionType |= MPI_BLOCKING;
               case MPI_MISC:
                 return true;
             }

           default:
             break;
         }
       }
       // neither an MPI nor CUDA API nor OpenCL API function

       // check if an OpenMP instrumented region
       if ( strstr( name, "!$omp" ) )
       {
         descr->paradigm = PARADIGM_OMP;
         
         if ( strstr( name, "barrier" ) )
         {
           descr->functionType = OMP_SYNC;
         }
         else // not a barrier
         {
           if ( strstr( name, "target " ) || strstr( name, "targetmap " ) )
           {
             descr->functionType = OMP_TARGET_OFFLOAD;
           }
           else 
           {
             if ( strstr( name, "offloading flush" ) )
             {
               descr->functionType = OMP_TARGET_FLUSH;
             }
             else
             {
               if( strstr( name, "parallel" ) )
               {
                 descr->functionType = OMP_PARALLEL;
               }
               else
               {
                 descr->functionType = OMP_MISC;
                 descr->paradigm = PARADIGM_CPU;
                 return false;
               }
             }
           }
         }
         return true;
       }
       
       // if it is an OpenMP fork/join event
       if ( strstr( name, OTF2_OMP_FORKJOIN_INTERNAL ) )
       {
         descr->functionType = OMP_FORKJOIN;
         descr->paradigm = PARADIGM_OMP;
         return true;
       }
       /* not an OpenMP function */

       /* kernel ? */
       if ( deviceNullStream )
       {
         descr->functionType = ( CUDA_KERNEL | CUDA_SYNC | CUDA_COLLSYNC );
         descr->paradigm = PARADIGM_CUDA;
         return true;
       }

       if ( deviceStream )
       {
         // TODO: distinguish by function group:
         
         // if name starts with '$' it is an OpenCL kernel
         if( name[0] == '$' )
         {
           descr->functionType = OCL_KERNEL;
           descr->paradigm     = PARADIGM_OCL;
         }
         else
         {
           descr->functionType = CUDA_KERNEL;
           descr->paradigm     = PARADIGM_CUDA;
         }
         return true;
       }

       /* anything else */
       descr->paradigm     = PARADIGM_CPU;
       descr->functionType = MISC_CPU;
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
