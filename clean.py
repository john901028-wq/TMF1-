import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("="*60)
print("Cleaning and preparing data")
print("="*60)

# Clean SPY data
print("\nCleaning SPY data...")
spy_df = pd.read_csv('spy_hourly.csv')

# Remove first 2 rows (header mess)
if len(spy_df) > 2 and spy_df.iloc[0, 0] == 'Ticker':
    spy_df = spy_df.iloc[2:].reset_index(drop=True)

# Parse datetime
spy_df['Datetime'] = pd.to_datetime(spy_df.iloc[:, 0])
spy_df = spy_df.drop(columns=[spy_df.columns[0]])

# Convert numeric columns
for col in ['Close', 'High', 'Low', 'Open', 'Volume']:
    if col in spy_df.columns:
        spy_df[col] = pd.to_numeric(spy_df[col], errors='coerce')

spy_df = spy_df.dropna()

print(f"OK - {len(spy_df)} SPY rows after cleaning")
spy_df.to_csv('spy_clean.csv', index=False)
print(spy_df.head())

# Create fresh MXF sample data
print("\nCreating fresh MXF sample data...")

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
        change = np.random.randn() * 50
        price = price + change
        
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

print(f"OK - {len(mxf_df)} MXF rows")
mxf_df.to_csv('mxf_clean.csv', index=False)
print(mxf_df.head())

# Simple pairing
print("\n" + "="*60)
print("Creating paired data")
print("="*60)

paired = []

for idx, mxf_row in mxf_df.iterrows():
    if idx < len(spy_df):
        spy_row = spy_df.iloc[idx]
        
        paired.append({
            'mxf_time': mxf_row['timestamp'],
            'mxf_open': mxf_row['open'],
            'mxf_high': mxf_row['high'],
            'mxf_low': mxf_row['low'],
            'mxf_close': mxf_row['close'],
            'mxf_volume': mxf_row['volume'],
            'spy_time': spy_row['Datetime'],
            'spy_open': spy_row['Open'],
            'spy_high': spy_row['High'],
            'spy_low': spy_row['Low'],
            'spy_close': spy_row['Close'],
            'spy_volume': spy_row['Volume']
        })

paired_df = pd.DataFrame(paired)
paired_df.to_csv('mxf_spy_paired.csv', index=False)

print(f"OK - {len(paired_df)} paired records")
print("\nSample:")
print(paired_df.head(10))

print("\n" + "="*60)
print("Output files:")
print("  - spy_clean.csv")
print("  - mxf_clean.csv")
print("  - mxf_spy_paired.csv")
print("="*60)
