#ifndef BLAZING_RAL_DATA_TYPE_H_
#define BLAZING_RAL_DATA_TYPE_H_

namespace ral {
namespace io {

typedef enum { UNDEFINED = 999, PARQUET = 0, ORC = 1, CSV = 2, JSON = 3, CUDF = 4, DASK_CUDF = 5, ARROW = 6, MYSQL = 7 } DataType;

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_DATA_TYPE_H_ */
