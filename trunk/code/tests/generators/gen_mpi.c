/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */ 

#include <mpi.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#define BUF_SIZE (1024*1024)
float buffer[BUF_SIZE];

float mpi_buf;

#define LOW_LOAD 10
#define HIGH_LOAD 100

int mpi_size  = 1;
int mpi_rank  = 0;
int prev_rank = 0;
int next_rank = 0;
int last_rank = 0;
const int root_rank = 0;

void foo(const int load)
{
  int i;
  for (i = 0; i < load; ++i)
  {
    int j;
    for (j = 0; j < BUF_SIZE; ++j)
    {
      buffer[j] += sin((double)j);
    }
  }
}

void barrier()
{
  foo((mpi_rank == last_rank) ? HIGH_LOAD : LOW_LOAD);
  printf("%d: barrier\n", mpi_rank);
  MPI_Barrier(MPI_COMM_WORLD);
}

void late_sender()
{
  if (mpi_rank == root_rank)
  {
    foo(HIGH_LOAD);
    printf("%d: send\n", mpi_rank);
    MPI_Send(&mpi_buf, 1, MPI_FLOAT, next_rank, 0, MPI_COMM_WORLD);
  } else
  {
    foo(LOW_LOAD);
  }
  
  if (mpi_rank > root_rank && mpi_rank < last_rank)
  {
    MPI_Status status;
    printf("%d: recv\n", mpi_rank);
    MPI_Recv(&mpi_buf, 1, MPI_FLOAT, prev_rank, 0, MPI_COMM_WORLD, &status);
    
    foo(LOW_LOAD);
    
    printf("%d: send\n", mpi_rank);
    MPI_Send(&mpi_buf, 1, MPI_FLOAT, next_rank, 0, MPI_COMM_WORLD);
  }
  
  if (mpi_rank == last_rank)
  {
    MPI_Status status;
    printf("%d: recv\n", mpi_rank);
    MPI_Recv(&mpi_buf, 1, MPI_FLOAT, prev_rank, 0, MPI_COMM_WORLD, &status);
  }
}

/*
 * Open waits for Isend on root 
 */
void isend_recv_root_to_all()
{
  // the root rank sends non-blocking messages to all other ranks
  if (mpi_rank == root_rank)
  {
    int r = 0;
    for(r = root_rank + 1; r < mpi_size; r++)
    {
      MPI_Request send_request;
      printf("0: MPI_Isend\n");
      MPI_Isend(&mpi_buf, 1, MPI_FLOAT, r, 0, MPI_COMM_WORLD, &send_request);
    }
  }else
  {
    MPI_Status status;
    printf("%d: MPI_Recv\n", mpi_rank);
    MPI_Recv(&mpi_buf, 1, MPI_FLOAT, root_rank, 0, MPI_COMM_WORLD, &status);
  }
}

void irecv_send_ring()
{
  int buffer[1000], buffer2[1000];
  MPI_Request request;
  MPI_Status status;

  int i = 0;
  for( i = 0; i < 1; i++ )
  {
    MPI_Irecv(buffer, 1000, MPI_INT, prev_rank, 123, MPI_COMM_WORLD, &request);
    MPI_Send(buffer2, 1000, MPI_INT, next_rank, 123, MPI_COMM_WORLD);
    MPI_Wait(&request, &status);
  }
}

void isend_irecv_wait()
{
  const int messages = 3;
  
  // allocate message buffers
  float *buffer_irecv = (float *) malloc( sizeof(float) * messages );
  float *buffer_isend = (float *) malloc( sizeof(float) * messages );
  
  MPI_Request *send_request = (MPI_Request *) malloc( sizeof(MPI_Request) * messages );
  MPI_Request *recv_request = (MPI_Request *) malloc( sizeof(MPI_Request) * messages );
  
  MPI_Status *status = (MPI_Status *) malloc( sizeof(MPI_Status) * messages );

  int i = 0;
  
  // invoke non-blocking messages
  for( i = 0; i < messages; i++ )
  {
    MPI_Irecv(&buffer_irecv[i], 1, MPI_FLOAT, prev_rank, 123, MPI_COMM_WORLD, &recv_request[i]);
    MPI_Isend(&buffer_isend[i], 1, MPI_FLOAT, next_rank, 123, MPI_COMM_WORLD, &send_request[i]);
  } 
  
  // wait for non-blocking messages
  for( i = 0; i < messages; i++ )
  {
    MPI_Wait(&recv_request[i], &status[i]);
    MPI_Wait(&send_request[i], &status[i]);
  }
}

void test()
{
  // the root rank sends non-blocking messages to all other ranks
  if (mpi_rank == root_rank)
  {
    float mpi_buf_recv[10];
    
    int r = 0;
    for(r = root_rank + 1; r < mpi_size; r++)
    {
      MPI_Request send_request, recv_request;
      printf("0: MPI_Isend\n");
      MPI_Isend(&mpi_buf, 1, MPI_FLOAT, r, 0, MPI_COMM_WORLD, &send_request);
      printf("0: MPI_Irecv\n");
      MPI_Irecv(mpi_buf_recv, 1, MPI_FLOAT, r, 0, MPI_COMM_WORLD, &recv_request);
      
        int flag = 0;
        MPI_Status   status;
        MPI_Test(&send_request, &flag, &status);
        
        if(status.MPI_ERROR == MPI_SUCCESS)
            printf("ISendRule: sendRequest MPI_SUCCESS %d, flag=%d\n", status.MPI_ERROR, flag);
        else if(status.MPI_ERROR == MPI_ERR_REQUEST)
            printf("ISendRule: sendRequest MPI_ERR_REQUEST\n");
        else if(status.MPI_ERROR == MPI_ERR_ARG)
            printf("ISendRule: sendRequest MPI_ERR_ARG\n");
        
        MPI_Test(&recv_request, &flag, &status);
        
        if(status.MPI_ERROR == MPI_SUCCESS)
            printf("ISendRule: recvRequest MPI_SUCCESS %d, flag=%d\n", status.MPI_ERROR, flag);
        else if(status.MPI_ERROR == MPI_ERR_REQUEST)
            printf("ISendRule: recvRequest MPI_ERR_REQUEST\n");
        else if(status.MPI_ERROR == MPI_ERR_ARG)
            printf("ISendRule: recvRequest MPI_ERR_ARG\n");
    }
  }else
  {
    MPI_Status status;
    float buffer_send;
    
    printf("%d: MPI_Recv\n", mpi_rank);
    MPI_Recv(&mpi_buf, 1, MPI_FLOAT, root_rank, 0, MPI_COMM_WORLD, &status);
    printf("%d: MPI_Recv\n", mpi_rank);
    MPI_Send(&buffer_send, 1, MPI_FLOAT, root_rank, 0, MPI_COMM_WORLD);
  }
  
  // gather all information from all processes
  float sendall_buf;
  float *recvall_buf = (float *) malloc(sizeof(float) * mpi_size);
  MPI_Allgather( &sendall_buf, 1, MPI_FLOAT, recvall_buf, 1, MPI_FLOAT, MPI_COMM_WORLD );
}

int main(int argc, char **argv)
{
  MPI_Init(&argc, &argv);
  
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  
  if (mpi_size > 1)
  {
    if (mpi_rank == root_rank)
      prev_rank = mpi_size - 1;
    else
      prev_rank = mpi_rank - 1;
    next_rank = (mpi_rank + 1) % mpi_size;
    last_rank = mpi_size - 1;
    
    late_sender();

    isend_recv_root_to_all();
    
    barrier();
    
    irecv_send_ring();
    
    isend_irecv_wait();
    
    //test();
  }
  
  MPI_Finalize();
  return 0;
}
