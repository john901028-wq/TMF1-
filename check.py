import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import os

print("="*60)
print("MXF + SPY Data Pairing")
print("="*60)

tz_taiwan = pytz.timezone('Asia/Taipei')
tz_est = pytz.timezone('America/New_York')

# Load SPY data
print("\nLoading SPY data...")
spy_df = pd.read_csv('spy_hourly.csv')
print(f"OK - {len(spy_df)} SPY rows")
print(spy_df.head())

# Load MXF data
print("\nLoading MXF data...")
if os.path.exists('mxf_1h_data.csv'):
    mxf_df = pd.read_csv('mxf_1h_data.csv')
    print(f"OK - {len(mxf_df)} MXF rows")
    print(mxf_df.head())
else:
    print("No MXF file found")

print("\n" + "="*60)
print("Done")
print("="*60)
