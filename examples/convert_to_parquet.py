
import pandas as pd
from pathlib import Path

# Read CSV
data_dir = Path(__file__).parent / "data"
df = pd.read_csv(
    data_dir / "customer_data.csv",
    parse_dates=["event_timestamp"]
)

# Write Parquet
data_dir.mkdir(exist_ok=True)
df.to_parquet(data_dir / "customer_data.parquet", index=False)