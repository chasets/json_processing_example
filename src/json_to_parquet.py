# Author: Tim Chase
# Date:   July 30, 2019
# Desc:   Convert json input files to parquet. See https://github.com/chasets/json_processing_example

import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



recs = json.load(open('data/original.json'))['records']
df = pd.DataFrame(recs)
df.set_index('ts', inplace=True)

pq.write_table(table, 'output/original.parquet', flavor='spark', coerce_timestamps='ms')
