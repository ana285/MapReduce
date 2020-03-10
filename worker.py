from base64 import b16encode, b16decode
import os
from mpi4py import MPI

ROOT = 0
MAP = 1
REDUCE = 2
main_path_file = "mapping"


def create_mapping(dir_name, new_dir):
    path_file = dir_name+"/"+new_dir
    # os.mkdir(path_file)
    try:
        os.mkdir(path_file)
    except OSError:
        print("Creation of the directory %s failed" % path_file)
    else:
        print("Successfully created the directory %s " % path_file)


def mapper(comm):
    my_rank = comm.Get_rank()
    data = comm.recv(source=ROOT, tag=MAP)
    while data != "empty":
        print("slave " + str(my_rank) + " received values\n")

        key = data[0]
        value = data[1]
        encoded_key = b16encode(key.encode()).decode()
        for val in value:
            encoded_val = b16encode(val.encode()).decode()
            create_mapping(main_path_file, str(encoded_key) + "#" + str(encoded_val))
        print("starting slave send")
        comm.send(1, dest=ROOT, tag=MAP)
        print("end slave send")
        data = comm.recv(source=ROOT, tag=MAP)


def reducer(comm):
    my_rank = comm.Get_rank()
    key = comm.recv(source=ROOT, tag=REDUCE)
    while key != "empty":
        print("slave " + str(my_rank) + " received value " + key + "\n")
        list_values = []
        for o in os.listdir(main_path_file):
            if os.path.isdir(os.path.join(main_path_file, o)):
                temp_name = o.split("#")
                first_key = temp_name[0]
                end_key = temp_name[-1]
                if first_key == key:
                    decrypted_end_key = b16decode(end_key).decode()
                    list_values.append(decrypted_end_key)
        decrypted_first_key = b16decode(key).decode()
        print("starting slave send")
        print([decrypted_first_key, list_values])
        comm.send([decrypted_first_key, list_values], dest=ROOT, tag=REDUCE)
        print("end slave send")
        key = comm.recv(source=ROOT, tag=REDUCE)
