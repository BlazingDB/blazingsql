from datetime import datetime

import numpy as np
import cudf

current_date = datetime.now().date()

expected = cudf.DataFrame({
  'current_date':[np.datetime64(current_date, 'ns')] * 5, 
  'o_orderkey': [1.0, 2.0, 3.0, 4.0, 5.0]
}).to_pandas()

