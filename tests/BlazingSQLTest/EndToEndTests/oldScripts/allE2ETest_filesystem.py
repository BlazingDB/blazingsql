from Configuration import Settings as Settings
from DataBase import createSchema as createSchema
from EndToEndTests import (
    csvFromHdfsTest,
    csvFromLocalTest,
    csvFromS3Test,
    parquetFromHdfsTest,
    parquetFromLocalTest,
    parquetFromS3Test,
)
from pydrill.client import PyDrill
from Runner import runTest
from Utils import Execution


def main():
    print("**init end2end**")

    Execution.getArgs()

    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]

    # Create Table Drill -----------------------------------------
    drill = PyDrill(host="localhost", port=8047)
    createSchema.init_drill_schema(drill, dir_data_file)

    # Sólo pasan todos los test con 100Mb
    csvFromLocalTest.main(drill, dir_data_file)

    csvFromS3Test.main(
        drill, dir_data_file
    )  # AttributeError: 'NoneType' object has no attribute '_cols'

    # vector::_M_range_check: __n
    # (which is 18446744073709551615) >= this->size() (which is 2)
    csvFromHdfsTest.main(
        drill, dir_data_file
    )

    parquetFromLocalTest.main(
        drill, dir_data_file
    )  # Sólo pasan todos los test con 100Mb

    # Pasan todos los test con 100Mb, con multiples archivos para
    # una tabla no porque no se carga bien todos los archivos.
    parquetFromS3Test.main(
        drill, dir_data_file
    )

    parquetFromHdfsTest.main(
        drill, dir_data_file
    )  # Se queda pensando en la lectura de data

    runTest.save_log()

    for i in range(0, len(Settings.memory_list)):
        print(
            Settings.memory_list[i].name
            + ":"
            + "   Start Mem: "
            + str(Settings.memory_list[i].start_mem)
            + "   End Mem: "
            + str(Settings.memory_list[i].end_mem)
            + "   Diff: "
            + str(Settings.memory_list[i].delta)
        )


if __name__ == "__main__":
    main()
