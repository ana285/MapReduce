from mpi4py import MPI
import shutil
import os
import dispatcher as master
import worker as worker

path_file = "mapping"

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    p = comm.Get_size()
    print('--- MAPPING phase started ---')
    if rank == 0:
        print('I am the coordinator with rank ', rank)
        coordinatorMapJob(comm, p)
    else:
        print('I am the worker ', rank)
        workerMapJob(rank, comm)

    print("--- REDUCING phase started ---")
    if rank == 0:
        master.reducer(comm, p)
        print("Master finished\n")
    else:
        worker.reducer(comm)
        print("Worker " + str(rank) + " FINISHED\n")


def coordinatorMapJob(comm, p):
    print('Coordinator job started')
    try:
        shutil.rmtree("mapping")
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
    try:
        os.mkdir(path_file)
    except OSError:
        print("Creation of the directory %s failed" % path_file)
    else:
        print("Successfully created the directory %s " % path_file)
    master.mapper(comm, p)
    print("MASTER FINISH\n")


def workerMapJob(rank, comm):
    print('Worker job started for ', rank)
    worker.mapper(comm)
    print("Worker " + str(rank) + " FINISHED\n")



if __name__ == "__main__":
    main()