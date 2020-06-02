from pydrill.client import PyDrill
from DataBase import createSchema as createSchema
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution

from EndToEndTests import parquetFromLocalTest
from EndToEndTests import csvFromS3Test
from EndToEndTests import parquetFromS3Test
from EndToEndTests import parquetFromHdfsTest
from EndToEndTests import csvFromHdfsTest
from EndToEndTests import csvFromLocalTest
from Utils import gpuMemory
from pynvml import *

def main():
    print('**init end2end**')
    
    Execution.getArgs()
    
    dir_data_file = Settings.data['TestSettings']['dataDirectory']
    
    # Create Table Drill ------------------------------------------------------------------------------------------------------
    drill = PyDrill(host = 'localhost', port = 8047)
    createSchema.init_drill_schema(drill, dir_data_file)

    csvFromLocalTest.main(drill, dir_data_file) #Sólo pasan todos los test con 100Mb
    
    csvFromS3Test.main(drill, dir_data_file) # AttributeError: 'NoneType' object has no attribute '_cols'
     
    csvFromHdfsTest.main(drill, dir_data_file) #vector::_M_range_check: __n (which is 18446744073709551615) >= this->size() (which is 2)
    
    parquetFromLocalTest.main(drill, dir_data_file) #Sólo pasan todos los test con 100Mb
     
    parquetFromS3Test.main(drill, dir_data_file) #Pasan todos los test con 100Mb, con multiples archivos para una tabla no porque no se carga bien todos los archivos.
     
    parquetFromHdfsTest.main(drill, dir_data_file) # Se queda pensando en la lectura de data

    runTest.save_log()
    
    for i in range(0, len(Settings.memory_list)):
        print(Settings.memory_list[i].name + ":" + 
              "   Start Mem: " + str(Settings.memory_list[i].start_mem) +
              "   End Mem: " + str(Settings.memory_list[i].end_mem) + 
              "   Diff: " + str(Settings.memory_list[i].delta))

if __name__ == '__main__':
    main()
    
    