import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#csv_file = '/Users/angelikiandreadi/Downloads/100M.csv'
#parquet_file = '/Users/angelikiandreadi/Downloads/100M.parquet'

#csv_file = '/Users/angelikiandreadi/Downloads/25M.csv'
#parquet_file = '/Users/angelikiandreadi/Downloads/25M.parquet'

csv_file = '/Users/angelikiandreadi/Downloads/5M.csv'
parquet_file = '/Users/angelikiandreadi/Downloads/5M.parquet'

chunk_size = 1_000_000  

reader = pd.read_csv(csv_file, chunksize=chunk_size)

writer = None

for chunk in reader:
    table = pa.Table.from_pandas(chunk)
    if writer is None:
        writer = pq.ParquetWriter(parquet_file, table.schema)
    
    writer.write_table(table)

if writer:
    writer.close()