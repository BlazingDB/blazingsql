import errno
import logging
import os
import time
import socket

# Util functions 

def checkSocket(socketNum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socket_free = False
    try:
        s.bind(("127.0.0.1", socketNum))
        socket_free = True
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            socket_free = False
        else:
            # something else raised the socket.error exception
            print("ERROR: Something happened when checking socket " + str(socketNum))
    s.close()
    return socket_free


def get_uri_values(files, partitions, base_folder):
    if base_folder[-1] != "/":
        base_folder = base_folder + "/"

    uri_values = []
    for file in files:
        file_dir = os.path.dirname(file.decode())
        partition_name = file_dir.replace(base_folder, "")
        if partition_name in partitions:
            uri_values.append(partitions[partition_name])
        else:
            print("ERROR: Could not get partition values for file: " + file.decode())
    return uri_values


def resolve_relative_path(files):
    files_out = []
    for file in files:
        if isinstance(file, str):
            # if its an abolute path or fs path
            if (
                file.startswith("/")
                | file.startswith("hdfs://")
                | file.startswith("s3://")
                | file.startswith("gs://")
            ):
                files_out.append(file)
            else:  # if its not, lets see if its a relative path we can access
                abs_file = os.path.abspath(os.path.join(os.getcwd(), file))
                if os.path.exists(abs_file):
                    files_out.append(abs_file)
                # if its not, lets just leave it and see if somehow
                # the engine can access it
                else:
                    files_out.append(file)
        else:  # we are assuming all are string. If not, lets just return
            return files
    return files_out


def distributed_initialize_server_directory(client, dir_path):

    # We are going to differentiate the two cases. When path is absolute,
    # we do the logging folder creation only once per host (server).
    # When path is relative, we have to group the workers according
    # to whether they have the same current working directory,
    # so, a unique folder will be created for each sub common cwd set.

    all_items = client.scheduler_info()["workers"].items()

    is_absolute_path = os.path.isabs(dir_path)

    if is_absolute_path:
        # Let's group the workers by host_name
        host_worker_dict = {}
        for worker, worker_info in all_items:
            host_name = worker.split(":")[0]
            if host_name not in host_worker_dict.keys():
                host_worker_dict[host_name] = [worker]
            else:
                host_worker_dict[host_name].append(worker)

        dask_futures = []
        for host_name, worker_list in host_worker_dict.items():
            dask_futures.append(
                client.submit(
                    initialize_server_directory,
                    dir_path,
                    workers=[worker_list[0]],
                    pure=False,
                )
            )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                logging.info("Directory already exists")
    else:
        # Let's get the current working directory of all workers
        dask_futures = []
        for worker, worker_info in all_items:
            dask_futures.append(
                client.submit(get_current_directory_path, workers=[worker], pure=False)
            )

        current_working_dirs = client.gather(dask_futures)

        # Let's group the workers by host_name and by common cwd
        host_worker_dict = {}
        for worker_key, cwd in zip(all_items, current_working_dirs):
            worker = worker_key[0]
            host_name = worker.split(":")[0]
            if host_name not in host_worker_dict.keys():
                host_worker_dict[host_name] = {cwd: [worker]}
            else:
                if cwd not in host_worker_dict[host_name].keys():
                    host_worker_dict[host_name][cwd] = [worker]
                else:
                    host_worker_dict[host_name][cwd].append(worker)

        dask_futures = []
        for host_name, common_current_work in host_worker_dict.items():
            for cwd, worker_list in common_current_work.items():
                dask_futures.append(
                    client.submit(
                        initialize_server_directory,
                        dir_path,
                        workers=[worker_list[0]],
                        pure=False,
                    )
                )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                logging.info("Directory already exists")


def initialize_server_directory(dir_path):
    if not os.path.exists(dir_path):
        try:
            os.mkdir(dir_path)
        except OSError as error:
            logging.error("Could not create directory: " + error)
            raise
        return True
    else:
        return False


def get_current_directory_path():
    return os.getcwd()


# Delete all generated (older than 1 hour) orc files
def remove_orc_files_from_disk(data_dir):
    if os.path.isfile(data_dir):  # only if data_dir exists
        all_files = os.listdir(data_dir)
        current_time = time.time()
        for file in all_files:
            if ".blazing-temp" in file:
                full_path_file = data_dir + "/" + file
                creation_time = os.path.getctime(full_path_file)
                if (current_time - creation_time) // (1 * 60 * 60) >= 1:
                    os.remove(full_path_file)
