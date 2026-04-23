import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



csv_file = '/Users/angelikiandreadi/Downloads/100M.csv'
parquet_file = '/Users/angelikiandreadi/Downloads/100M.parquet'
chunk_size = 1_000_000  # Process 1 million rows at a time

# Create a CSV reader object
reader = pd.read_csv(csv_file, chunksize=chunk_size)

writer = None

for chunk in reader:
    # Convert Pandas chunk to PyArrow Table
    table = pa.Table.from_pandas(chunk)
    
    # On the first chunk, initialize the Parquet writer
    if writer is None:
        writer = pq.ParquetWriter(parquet_file, table.schema)
    
    writer.write_table(table)

# Close the writer to finalize the file
if writer:
    writer.close()