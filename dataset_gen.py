import pandas as pd
import numpy as np
import os

sizes = [5000000, 25000000, 100000000]

regions = ["North America", "South America", "Europe", "Asia", " Africa"]
events = ["view", "click", "buy", "login", "logout"] 

def generate_dataset(size):
    #os.makedirs("data", exist_ok=True)

    label = f"{size // 1_000_000}M"
    output_file = f"/Users/angelikiandreadi/Downloads/{label}.csv" # must be adapted

    chunk_size = 100000 # at first attempted to generate line by line but took reaaaally long

    for i in range (0, size, chunk_size):
        curr_batch = min(chunk_size, size-i)

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(np.random.randint(874747384839, 2364737473857, size=curr_batch)),
            "used_id": np.random.randint(100000, 999999, size=curr_batch), # so that they have a consistent id lenght
            "region": np.random.choice(regions, size=curr_batch),
            "event_type": np.random.choice(events, size=curr_batch),
            "value": np.random.uniform(10, 500, size=curr_batch) # price in this context
        })
 
        df.to_csv(output_file, mode="a", index=False, header=(i==0))

for size in sizes:
    generate_dataset(size)


