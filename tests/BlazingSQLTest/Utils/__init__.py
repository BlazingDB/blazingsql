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
    if fileSchemaType == DataType.DASK_CUDF:
        if dask_client is None:
            return True

    if fileSchemaType == DataType.CUDF:
        # TODO dask tests percy kharoly c.gonzales
        # skip gdf test when we are about to run tests
        # on gdf tables with nRals>1
        skip = False
        if int(nRals) > 1:
            if dask_client is None:
                return True

        return skip

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


def try_to_get_dask_client(n_workers, n_gpus):
    daskConnection = Settings.data["TestSettings"].get("daskConnection")
    if (daskConnection is not None) or (type(daskConnection) is not str):
        if n_gpus < 1:
            raise ValueError("n gpus must be at least 1")
        try:
            if daskConnection == "local":
                cluster = LocalBlazingSQLCluster(n_gpus, n_workers=n_workers)
                return Client(cluster)
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


def init_context():
    bc = None
    dask_client = None
    nRals = int(Settings.data["RunSettings"]["nRals"])
    nGpus = int(Settings.data["RunSettings"]["nGPUs"])
    if nRals == 1:
        bc = BlazingContext()
    else:
        os.chdir(Settings.data["TestSettings"]["logDirectory"])
        dask_client = try_to_get_dask_client(nRals, nGpus)
        if dask_client is not None:
            dask_conn = Settings.data["TestSettings"]["daskConnection"]
            iface = Settings.data["RunSettings"]["networkInterface"]
            # print("Using dask: " + dask_conn)
            # if "local" != dask_conn:

            bc = BlazingContext(
                dask_client=dask_client,
                network_interface=iface,
                pool=True,
                initial_pool_size=300000000,
                allocator="default",
            )
        else:
            # Fallback: could not found a valid dask server
            bc = BlazingContext()
    return (bc, dask_client)
