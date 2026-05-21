# Angeliki Andreadi, Keenan Hardy
import pandas as pd
import numpy as np
from pathlib import Path

# Dataset sizes used by the benchmark: small, medium, and large.
sizes = {"S": 5_000_000, "M": 25_000_000, "L": 100_000_000}

# Possible values for the categorical columns in the fake event data.
regions     = ["North America", "South America", "Europe", "Asia", "Africa"]
event_types = ["view", "click", "buy", "login", "logout"]

# Raw CSV files are saved here. The upload script reads from this folder later.
output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)

chunk_size = 100_000  # write rows in batches because generating line by line is too slow

# Use random timestamps between 2020 and 2024 so the query can filter by year.
ts_start = int(pd.Timestamp("2020-01-01").timestamp())
ts_end   = int(pd.Timestamp("2024-12-31").timestamp())


def generate_dataset(label, size):
    # Fixed seed makes the random data repeatable when the script is run again.
    np.random.seed(42)
    output_file = output_dir / f"{label}.csv"

    # Deletes data if previously there.
    if output_file.exists():
        output_file.unlink()

    # Generate and append one chunk at a time to avoid keeping the full dataset in RAM.
    for i in range(0, size, chunk_size):
        batch = min(chunk_size, size - i)

        # Each row is one fake user event with time, user, location, type, and value.
        df = pd.DataFrame({
            "ts":         pd.to_datetime(np.random.randint(ts_start, ts_end, size=batch), unit="s"),
            "user_id":    np.random.randint(100_000, 999_999, size=batch),  # six-digit IDs look consistent
            "region":     np.random.choice(regions, size=batch),
            "event_type": np.random.choice(event_types, size=batch),
            "value":      np.random.uniform(10, 500, size=batch).round(2),  # pretend price/value for the event
        })

        # Only the first chunk writes the CSV header. Later chunks just append rows.
        df.to_csv(output_file, mode="a", index=False, header=(i == 0))

    mb = output_file.stat().st_size / 1e6
    print(f"DONE {label}: {size:,} rows → {output_file} ({mb:.0f} MB)")


# Build all dataset sizes one after another.
for label, size in sizes.items():
    generate_dataset(label, size)
