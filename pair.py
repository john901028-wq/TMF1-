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
spy_df = pd.read_csv('spy_hourly.csv', index_col=0, parse_dates=True)
spy_df.index = spy_df.index.tz_localize(tz_est) if spy_df.index.tz is None else spy_df.index
print(f"OK - {len(spy_df)} SPY rows")

# Create or load MXF data
mxf_file = 'mxf_1h_data.csv'

if os.path.exists(mxf_file):
    print(f"\nLoading MXF from {mxf_file}...")
    mxf_df = pd.read_csv(mxf_file)
    mxf_df['timestamp'] = pd.to_datetime(mxf_df['timestamp'])
else:
    print("\nCreating sample MXF data...")
    
    dates = []
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    
    price = 32000
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    current_date = start_date
    while current_date < end_date:
        for hour in range(8):
            price = price + np.random.randn() * 50
            
            night_time = current_date.replace(hour=21, minute=30) + timedelta(hours=hour)
            
            dates.append(night_time)
            opens.append(price)
            highs.append(price + abs(np.random.randn()) * 30)
            lows.append(price - abs(np.random.randn()) * 30)
            closes.append(price + np.random.randn() * 20)
            volumes.append(np.random.randint(100, 5000))
        
        current_date += timedelta(days=1)
    
    mxf_df = pd.DataFrame({
        'timestamp': dates,
        'open': opens,
        'high': highs,
        'low': lows,
        'close': closes,
        'volume': volumes
    })
    
    mxf_df.to_csv('mxf_sample.csv', index=False)
    print(f"Created sample MXF: mxf_sample.csv ({len(mxf_df)} rows)")

print(f"OK - {len(mxf_df)} MXF rows")

# Localize MXF to Taiwan timezone
mxf_df['timestamp'] = mxf_df['timestamp'].dt.tz_localize(tz_taiwan) if mxf_df['timestamp'].dt.tz is None else mxf_df['timestamp']
mxf_df['timestamp_utc'] = mxf_df['timestamp'].dt.tz_convert('UTC')

# Align data
print("\nAligning MXF (night) and SPY (day)...")

paired_data = []

for idx, mxf_row in mxf_df.iterrows():
    mxf_time_utc = mxf_row['timestamp_utc']
    
    spy_times = pd.to_datetime(spy_df.index)
    time_diff = (spy_times - mxf_time_utc).abs()
    closest_idx = time_diff.argmin()
    
    if time_diff[closest_idx].total_seconds() < 3600:
        spy_row = spy_df.iloc[closest_idx]
        
        paired_data.append({
            'mxf_time_tw': mxf_row['timestamp'],
            'mxf_open': mxf_row['open'],
            'mxf_high': mxf_row['high'],
            'mxf_low': mxf_row['low'],
            'mxf_close': mxf_row['close'],
            'mxf_volume': mxf_row['volume'],
            'spy_time': spy_row.name,
            'spy_close': spy_row['Close'],
            'spy_high': spy_row['High'],
            'spy_low': spy_row['Low'],
            'spy_open': spy_row['Open'],
            'spy_volume': spy_row['Volume']
        })

if paired_data:
    paired_df = pd.DataFrame(paired_data)
    paired_df.to_csv('mxf_spy_paired.csv', index=False)
    print(f"OK - {len(paired_df)} paired records")
    print("\nSample:")
    print(paired_df.head())
else:
    print("No matches found")

print("\n" + "="*60)
print("Files created:")
print("  - spy_hourly.csv (existing)")
print("  - mxf_sample.csv (sample data)")
print("  - mxf_spy_paired.csv (paired data)")
print("="*60)
