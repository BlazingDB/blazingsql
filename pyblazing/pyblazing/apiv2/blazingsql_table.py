import cio
import cudf
import dask_cudf
import pyarrow

from pyblazing.apiv2 import DataType
from pyblazing.apiv2.utilities import getNodePartitionKeys


class BlazingTable(object):
    def __init__(
        self,
        name,
        input,
        fileType,
        files=None,
        datasource=[],
        calcite_to_file_indices=None,
        args={},
        convert_gdf_to_dask=False,
        convert_gdf_to_dask_partitions=1,
        client=None,
        uri_values=[],
        in_file=[],
        force_conversion=False,
        metadata=None,
        row_groups_ids=[],
    ):
        # row_groups_ids, vector<vector<int>> one vector
        # of row_groups per file
        self.name = name
        self.fileType = fileType
        if fileType == DataType.ARROW:
            if force_conversion:
                # converts to cudf for querying
                self.input = cudf.DataFrame.from_arrow(input)
                self.fileType = DataType.CUDF
            else:
                self.input = cudf.DataFrame.from_arrow(input.schema.empty_table())
                self.arrow_table = input
        else:
            self.input = input

        self.calcite_to_file_indices = calcite_to_file_indices
        self.files = files

        self.datasource = datasource

        self.args = args
        if self.fileType == DataType.CUDF or self.fileType == DataType.DASK_CUDF:
            if convert_gdf_to_dask and isinstance(self.input, cudf.DataFrame):
                self.input = dask_cudf.from_cudf(
                    self.input, npartitions=convert_gdf_to_dask_partitions
                )
            if isinstance(self.input, dask_cudf.core.DataFrame):
                self.input = self.input.persist()
        self.uri_values = uri_values
        self.in_file = in_file

        # slices, this is computed in create table,
        # and then reused in sql method
        self.slices = None
        # metadata, this is computed in create table, after call get_metadata
        self.metadata = metadata
        # row_groups_ids, vector<vector<int>> one vector of
        # row_groups per file
        self.row_groups_ids = row_groups_ids
        # a pair of values with the startIndex and batchSize
        # info for each slice
        self.offset = (0, 0)

        self.column_names = []
        self.column_types = []

        if self.fileType == DataType.CUDF:
            self.column_names = [x for x in self.input._data.keys()]
            data_values = self.input._data.values()
            self.column_types = [cio.np_to_cudf_types_int(x.dtype) for x in data_values]
        elif self.fileType == DataType.DASK_CUDF:
            self.column_names = [x for x in input.columns]
            self.column_types = [cio.np_to_cudf_types_int(x) for x in input.dtypes]

        # file_column_names are usually the same as column_names, except
        # for when in a hive table the column names defined by the hive schema
        # are different that the names in actual files
        self.file_column_names = self.column_names

    def has_metadata(self):
        if isinstance(self.metadata, dask_cudf.core.DataFrame):
            return not self.metadata.compute().empty
        if self.metadata is not None:
            return not self.metadata.empty
        return False

    def filterAndRemapColumns(self, tableColumns):
        # only used for arrow
        new_table = self.arrow_table

        columns = []
        names = []
        i = 0
        for column in new_table.itercolumns():
            for index in tableColumns:
                if i == index:
                    names.append(self.arrow_table.field(i).name)
                    columns.append(column)
            i = i + 1
        new_table = pyarrow.Table.from_arrays(columns, names=names)
        new_table = BlazingTable(
            self.name, new_table, DataType.ARROW, force_conversion=True
        )

        return new_table

    def convertForQuery(self):
        return BlazingTable(
            self.name, self.arrow_table, DataType.ARROW, force_conversion=True
        )

    # until this is implemented we cant do self join with arrow tables
    #    def unionColumns(self,otherTable):

    def getDaskDataFrameKeySlices(self, nodes, client):
        import copy

        nodeFilesList = []
        partition_keys_mapping = getNodePartitionKeys(self.input, client)
        for node in nodes:
            # here we are making a shallow copy of the table and getting rid
            # of the reference for the dask.cudf.DataFrame since we dont want
            # to send that to dask wokers. You dont want to send a distributed
            # object to individual workers
            table = copy.copy(self)
            if node["worker"] in partition_keys_mapping:
                table.partition_keys = partition_keys_mapping[node["worker"]]
                table.input = []
            else:
                table.input = [table.input._meta]
                table.partition_keys = []
            nodeFilesList.append(table)

        return nodeFilesList

    def getSlices(self, numSlices):
        nodeFilesList = []
        if self.files is None:
            for i in range(0, numSlices):
                nodeFilesList.append(BlazingTable(self.name, self.input, self.fileType))
            return nodeFilesList
        remaining = len(self.files)
        startIndex = 0
        for i in range(0, numSlices):
            batchSize = int(remaining / (numSlices - i))
            tempFiles = self.files[startIndex : startIndex + batchSize]
            uri_values = self.uri_values[startIndex : startIndex + batchSize]

            slice_row_groups_ids = []
            if self.row_groups_ids is not None:
                slice_row_groups_ids = self.row_groups_ids[
                    startIndex : startIndex + batchSize
                ]

            bt = BlazingTable(
                self.name,
                self.input,
                self.fileType,
                files=tempFiles,
                calcite_to_file_indices=self.calcite_to_file_indices,
                uri_values=uri_values,
                args=self.args,
                row_groups_ids=slice_row_groups_ids,
                in_file=self.in_file,
            )
            bt.offset = (startIndex, batchSize)
            bt.column_names = self.column_names
            bt.file_column_names = self.file_column_names
            bt.column_types = self.column_types
            nodeFilesList.append(bt)

            startIndex = startIndex + batchSize
            remaining = remaining - batchSize

        return nodeFilesList
