import pandas as pd
from fastparquet import write

def write_to_parquet(df, path):
  print('Writing parquet data', path)
  df.to_parquet(path, compression = 'gzip')
