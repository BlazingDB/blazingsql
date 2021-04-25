import copy
import os

from blazingsql import BlazingContext, DataType
from dask.distributed import Client
from dask_cuda import LocalCUDACluster
from dask_cuda.utils import CPUAffinity, get_cpu_affinity

from Configuration import Settings as Settings
from DataBase.createSchema import get_extension

test_name_delimiter = " $$0_0$$ "


# NOTE this function will help to chosse the final distributed query
# if nRals will output the single_node_sql, else will output distrubuted_sql
def dquery(nRals, single_node_sql, distrubuted_sql):
    if int(nRals) == 1:
        return single_node_sql
    else:
        return distrubuted_sql


def test_name(queryType, fileSchemaType):
    ext = get_extension(fileSchemaType)
    tname = "%s%s%s" % (queryType, test_name_delimiter, ext)
    return tname


def skip_test(dask_client, nRals, fileSchemaType, queryType):
    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
    executionMode = Settings.data['RunSettings']['executionMode']

    if fileSchemaType == DataType.DASK_CUDF:
        # Skipping combination DASK_CUDF and testsWithNulls="true"
        # due to https://github.com/rapidsai/cudf/issues/7572
        if dask_client is None or testsWithNulls == "true":
            return True

    if fileSchemaType == DataType.CUDF:
        # TODO dask tests percy kharoly c.gonzales
        # skip gdf test when we are about to run tests
        # on gdf tables with nRals>1
        skip = False
        if int(nRals) > 1:
            if dask_client is None or testsWithNulls == "true":
                return True

        return skip

    if executionMode == 'gpuci':
        if  fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL]:
            return True

    return False


class LocalBlazingSQLCluster(LocalCUDACluster):
    def __init__(self, ngpus, *args, **kwargs):
        self._worker_count = -1
        self._ngpus = ngpus
        self._gpu_count = -1
        super().__init__(*args, **kwargs)

    @property
    def worker_count(self):
        """ Returns the id for a next worker

        To improve this method use a python generator

        NOTE: The worker id must be a string. So this method (or the generator)
        could produce hashes.
        """
        self._worker_count += 1
        return self._worker_count

    @property
    def gpu_count(self):
        self._gpu_count += 1
        if self._ngpus <= self._gpu_count:
            self._gpu_count = 0
        return self._gpu_count

    def new_worker_spec(self):
        """ Returns the info for a next worker

        This method is related with `cluster.scale`
        used to change the number of worker in the cluster
        """
        worker_count = self.worker_count
        gpu_count = self.gpu_count
        spec = copy.deepcopy(self.new_spec)
        spec["options"].update(
            {
                "env": {"CUDA_VISIBLE_DEVICES": str(gpu_count)},
                "plugins": {CPUAffinity(get_cpu_affinity(gpu_count))},
            }
        )
        return {worker_count: spec}


def try_to_get_dask_client(n_workers, n_gpus, iface):
    daskConnection = Settings.data["TestSettings"].get("daskConnection")
    if (daskConnection is not None) or (type(daskConnection) is not str):
        if n_gpus < 1:
            raise ValueError("n gpus must be at least 1")
        try:
            if daskConnection == "local":
                cluster = LocalBlazingSQLCluster(n_gpus, n_workers=n_workers)
                # cluster = LocalBlazingSQLCluster(n_gpus,  n_workers=n_workers,
                #   interface=iface,
                #    protocol="ucx",
                #    enable_tcp_over_ucx=True,
                #    enable_infiniband=False,
                #    enable_nvlink=False,
                #    # asynchronous=True,
                #)
                return Client(cluster)
            else:
                if daskConnection.endswith('.json'): #assuming a scheduler file
                    return Client(scheduler_file=daskConnection)
                else:
                    return Client(daskConnection)
        except Exception as e:
            # TODO: exceptions from cluster creation or dask connection
            raise EnvironmentError(
                "WARNING: Could not connect to dask: "
                + daskConnection
                + " ... error was "
                + str(e)
            )
    raise ValueError("ERROR: Bad dask connection '%s'" % daskConnection)


def init_context(useProgressBar: bool = False, config_options={"ENABLE_GENERAL_ENGINE_LOGS": True,
                                 "ENABLE_COMMS_LOGS": True,
                                 "ENABLE_TASK_LOGS": True,
                                 "ENABLE_OTHER_ENGINE_LOGS": True}):
    bc = None
    dask_client = None
    nRals = int(Settings.data["RunSettings"]["nRals"])
    nGpus = int(Settings.data["RunSettings"]["nGPUs"])
    if nRals == 1:
        bc = BlazingContext(
            config_options=config_options,
            enable_progress_bar=True
        )
    else:
        os.chdir(Settings.data["TestSettings"]["logDirectory"])
        iface = Settings.data["RunSettings"]["networkInterface"]
        dask_client = try_to_get_dask_client(nRals, nGpus, iface)
        if dask_client is not None:
            dask_conn = Settings.data["TestSettings"]["daskConnection"]

            # print("Using dask: " + dask_conn)
            # if "local" != dask_conn:

            bc = BlazingContext(
                dask_client=dask_client,
                network_interface=iface,
                # pool=True,
                # initial_pool_size=300000000,
                allocator="default",
                config_options=config_options,
                enable_progress_bar=True
            )
        else:
            # Fallback: could not found a valid dask server
            bc = BlazingContext(
                config_options=config_options,
                enable_progress_bar=True)

    return (bc, dask_client)
