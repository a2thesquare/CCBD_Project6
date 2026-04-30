import pandas as pd
import numpy as np
from pathlib import Path

sizes = {"S": 5_000_000, "M": 25_000_000, "L": 100_000_000}

regions     = ["North America", "South America", "Europe", "Asia", "Africa"]
event_types = ["view", "click", "buy", "login", "logout"]

output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)

chunk_size = 100_000 # at first attempted to generate line by line but took reaaaally long

ts_start = int(pd.Timestamp("2020-01-01").timestamp()) # changed the timestamps to make more sense
ts_end   = int(pd.Timestamp("2024-12-31").timestamp())


def generate_dataset(label, size):
    np.random.seed(42)
    output_file = output_dir / f"{label}.csv"

    for i in range(0, size, chunk_size):
        batch = min(chunk_size, size - i)

        df = pd.DataFrame({
            "ts":         pd.to_datetime(np.random.randint(ts_start, ts_end, size=batch), unit="s"),
            "user_id":    np.random.randint(100_000, 999_999, size=batch), # so that they have a consistent id lenght
            "region":     np.random.choice(regions, size=batch),
            "event_type": np.random.choice(event_types, size=batch),
            "value":      np.random.uniform(10, 500, size=batch).round(2), # price in this context
        })

        df.to_csv(output_file, mode="a", index=False, header=(i == 0))

    mb = output_file.stat().st_size / 1e6
    print(f"DONE {label}: {size:,} rows → {output_file} ({mb:.0f} MB)")


for label, size in sizes.items():
    generate_dataset(label, size)