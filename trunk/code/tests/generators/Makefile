SCOREP = scorep
CC     = mpicc
NVCC   = nvcc

C_FLAGS  = -g
LD_FLAGS = -lmpi

all: gen_mpi

gen_mpi: gen_mpi.c
	$(SCOREP) $(CC) $(C_FLAGS) $(LD_FLAGS) $+ -o $@

run:
	mpirun -n 2 ./gen_mpi

clean:
	rm gen_mpi
	rm -rf scorep-*
