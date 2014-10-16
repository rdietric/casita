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

    barrier();
  }
  
  MPI_Finalize();
  return 0;
}
