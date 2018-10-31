/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2018,
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
 static const char* FTABLE_CUDA_BLOCKING_COMM[] =
 {
   "cuMemcpyDtoD",
   "cuMemcpyDtoH",
   "cuMemcpyDtoH_v2",
   "cuMemcpyHtoD",
   "cuMemcpyHtoD_v2"
 };
 
 static const char* FTABLE_CUDA_COLL_SYNC[] =
 {
   "cuCtxSynchronize",
   "cudaSynchronize",
     
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
   "cuMemsetD2D32"
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
 
 static const char* FTABLE_CUDA_ASYNC_MEMCPY[] =
 {
   "cuMemcpyDtoHAsync_v2",
   "cuMemcpyHtoDAsync_v2"
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
   "MPI_Gather",  // allToOne
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

 //static const char* FTABLE_MPI_MISC[]          =
 //{
   //"MPI_Bsend", "MPI_Cancel", "MPI_Probe"
 //};

 static const size_t      fTableEntriesCUDA = 11;
 static const FTableEntry fTableCUDA[ fTableEntriesCUDA ] =
 {
   { OFLD_WAIT_ALL, 2, FTABLE_CUDA_COLL_SYNC },
   { OFLD_WAIT, 1, FTABLE_CUDA_SYNC },
   { OFLD_QUERY, 1, FTABLE_CUDA_QUERY },
   { OFLD_ENQUEUE_KERNEL, 5, FTABLE_CUDA_LAUNCH },
   { OFLD_QUERY_EVT, 1, FTABLE_CUDA_EVENT_QUERY },
   { OFLD_WAIT_EVT, 1, FTABLE_CUDA_EVENT_SYNC },
   { OFLD_ENQUEUE_EVT, 1, FTABLE_CUDA_EVENT_LAUNCH },
   { OFLD_ENQUEUE_WAIT, 1, FTABLE_CUDA_STREAM_WAIT },
   { OFLD_WAITSTATE, 1, FTABLE_GPU_WAITSTATE },
   { OFLD_BLOCKING_DATA, 5, FTABLE_CUDA_BLOCKING_COMM },
   { OFLD_ASYNC_DATA, 2, FTABLE_CUDA_ASYNC_MEMCPY }
 };
 
 static const size_t      fTableEntriesOpenCL = 6;
 static const FTableEntry fTableOpenCL[ fTableEntriesOpenCL ] =
 {
   { OFLD_WAIT_QUEUE, 1, FTABLE_OPENCL_QUEUE_SYNC },
   { OFLD_ENQUEUE_DATA, 2, FTABLE_OPENCL_ENQUEUE_BUFFER },
   { OFLD_ENQUEUE_KERNEL, 1, FTABLE_OPENCL_ENQUEUE },
   { OFLD_QUERY_EVT, 1, FTABLE_OPENCL_EVENT_QUERY },
   { OFLD_WAIT_EVT, 1, FTABLE_OPENCL_EVENT_SYNC },
   { OFLD_WAITSTATE, 1, FTABLE_GPU_WAITSTATE }
 };

 static const size_t      fTableEntriesMPI = 7;
 static const FTableEntry fTableMPI[fTableEntriesMPI] =
 {
   { MPI_INIT, 2, FTABLE_MPI_INIT },
   { MPI_FINALIZE, 1, FTABLE_MPI_FINALIZE },
   { MPI_RECV, 1, FTABLE_MPI_RECV },
   { MPI_SEND, 4, FTABLE_MPI_SEND },
   { MPI_COLLECTIVE, 11, FTABLE_MPI_COLL },
//   { MPI_ONETOALL, 2, FTABLE_MPI_ONETOALL }, // oneToAll rule might be broken
//   { MPI_ALLTOONE, 0, FTABLE_MPI_ALLTOONE }, // allToOne rule is broken
   { MPI_SENDRECV, 1, FTABLE_MPI_SENDRECV },
//   { MPI_MISC, 0, FTABLE_MPI_MISC },
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
      * @param descr the function descriptor (the record type must be set)
      * @param name name of the region
      * @param paradigm the OTF2 paradigm
      * @param deviceStream does the event occur on a device stream
      * @param deviceNullStream do we have only the device null stream
      * @param ignoreMPI ignore MPI events
      * 
      * @return true, if it maps to an internal node, otherwise false
      */
     static bool
     getAPIFunctionType( FunctionDescriptor* descr, const char* name, 
                         OTF2_Paradigm paradigm,
                         bool deviceStream, bool deviceNullStreamOnly,
                         bool ignoreMPI )
     {
       if( name == NULL || descr == NULL )
       {
         return false;
       }
       
       descr->paradigm     = PARADIGM_CPU;
       descr->functionType = 0;
       
       // do not check function type for user and compiler instrumented regions
       // but keep offloading kernels
       if( !deviceStream && ( paradigm == OTF2_PARADIGM_USER || 
                              paradigm == OTF2_PARADIGM_COMPILER ) )
       {
         descr->functionType = MISC_CPU;
         return false;
       }

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
             if ( Parser::getInstance().getProgramOptions().ignoreAsyncMpi || 
                  ignoreMPI )
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
             if ( Parser::ignoreOffload() )
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
             if ( Parser::ignoreOffload() )
             {
               descr->paradigm     = PARADIGM_CPU;
               descr->functionType = MISC_CPU;
               return false;
             }
             
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
             if( ignoreMPI )
             {
               descr->paradigm     = PARADIGM_CPU;
               descr->functionType = MISC_CPU;
               return false;
             }
             
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
             
             // change the paradigm to offload
             //descr->paradigm = PARADIGM_OFFLOAD;
             
             switch ( descr->functionType )
             {
               case OFLD_BLOCKING_DATA:
                 descr->functionType |= OFLD_WAIT_ALL | OFLD_WAIT | OFLD_ENQUEUE_DATA;
                 return true;
                 
               case OFLD_WAIT_ALL:
                 descr->functionType |= OFLD_WAIT;
                 return true;
                 
               case OFLD_ASYNC_DATA:
                 if( deviceNullStreamOnly )
                 {
                   descr->functionType |= OFLD_WAIT_ALL | OFLD_WAIT | OFLD_BLOCKING_DATA | OFLD_ENQUEUE_DATA;
                   return true;
                 }
                 else
                 {
                   return false;
                 }
                 
               case OFLD_WAIT:
               case OFLD_ENQUEUE_KERNEL:
               case OFLD_ENQUEUE_EVT:
               case OFLD_WAIT_EVT:
               case OFLD_QUERY:
               case OFLD_QUERY_EVT:
               case OFLD_ENQUEUE_WAIT:
                 return true;
             }
             
             break;
             
           case PARADIGM_OCL:
             // change the paradigm to offload
             //descr->paradigm = PARADIGM_OFFLOAD;
             
             switch ( descr->functionType )
             {
               case OFLD_WAIT_QUEUE:
               case OFLD_WAIT_EVT:
                 descr->functionType |= OFLD_WAIT;
               case OFLD_QUERY_EVT:
               case OFLD_ENQUEUE_KERNEL:
               case OFLD_ENQUEUE_DATA:
                 return true;
             }
             
             break;

           // only blocking MPI (non-blocking is handled separately)
           case PARADIGM_MPI:
             switch ( descr->functionType )
             {
               case MPI_INIT:
               case MPI_FINALIZE:
                 descr->functionType |= MPI_ALLRANKS;   // these two are always executed on all ranks
                 descr->functionType |= MPI_COLLECTIVE; // these two are collectives
               case MPI_COLLECTIVE:
               case MPI_ONETOALL:
               case MPI_ALLTOONE:
               case MPI_SENDRECV:
               case MPI_RECV:
               case MPI_SEND:
                 descr->functionType |= MPI_BLOCKING;
               case MPI_MISC:
                 return true;
             }
             
             break;

           default:
             break;
         }
       }
       // neither an MPI nor CUDA API nor OpenCL API function

       // check if an OpenMP instrumented region
       if( strncmp( name, "!$omp", 5 ) == 0 ) // if( strstr( name, "!$omp" ) )
       {
         descr->paradigm = PARADIGM_OMP;
         
         /*if ( strstr( name+5, "wait_barrier" ) ) // exclude wait_barrier
         {
           descr->functionType = OMP_MISC;
           descr->paradigm = PARADIGM_CPU;
           return false;
         }
         else */
         if ( strstr( name+6, "barrier" ) ) // this includes wait_barrier
         {
           descr->functionType = OMP_SYNC;
         }
         else // not a barrier
         {
           // target regions (ignore target data regions)
           if ( strstr( name+6, "target" ) && strstr( name+13, "data" ) == NULL )
           {
             descr->functionType = OMP_TARGET;
           }
           else if ( strstr( name+6, "offloading flush" ) )
           {
             descr->functionType = OMP_TARGET_FLUSH;
           }
           else
           {
             // threads of a parallel region start with 
             // OPARI2: parallel begin event || OMPT: implicit task begin event
             if( strstr( name+6, "parallel" ) )
             {
               descr->functionType = OMP_PARALLEL;
             }
             else if( strstr( name+6, "implicit" ) && strstr( name+14, "task" ) )
             {
               descr->functionType = OMP_IMPLICIT_TASK;
             }
             else
             {
               descr->functionType = OMP_MISC;
               descr->paradigm = PARADIGM_CPU;
               return false;
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
       if ( deviceStream )
       {
         // TODO: distinguish by function group:
         
         descr->functionType = OFLD_TASK_KERNEL;
         
         // if name starts with '$' it is an OpenCL kernel
         if( name[0] == '$' )
         {
           descr->paradigm = PARADIGM_OCL;
         }
         else
         {
           descr->paradigm = PARADIGM_CUDA;
         }
         return true;
       }

       // anything else
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
