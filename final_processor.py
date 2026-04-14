import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf
import pytz

class MXFSPYDataProcessor:
    def __init__(self):
        self.tz_taiwan = pytz.timezone('Asia/Taipei')
        self.tz_est = pytz.timezone('America/New_York')
        
    def create_sample_mxf_data(self, days=365):
        """Create sample MXF data for testing"""
        print("\n" + "="*60)
        print("Creating sample MXF data...")
        print("="*60)
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            dates = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []
            
            price = 32000
            current_date = start_date
            
            while current_date < end_date:
                for hour in range(8):
                    change = np.random.randn() * 50
                    price = price + change
                    
                    open_price = price
                    high_price = price + abs(np.random.randn()) * 30
                    low_price = price - abs(np.random.randn()) * 30
                    close_price = price + np.random.randn() * 20
                    volume = np.random.randint(100, 5000)
                    
                    night_time = current_date.replace(hour=21, minute=30) + timedelta(hours=hour)
                    
                    dates.append(night_time)
                    opens.append(open_price)
                    highs.append(high_price)
                    lows.append(low_price)
                    closes.append(close_price)
                    volumes.append(volume)
                    
                    price = close_price
                
                current_date += timedelta(days=1)
            
            df = pd.DataFrame({
                'timestamp': dates,
                'open': opens,
                'high': highs,
                'low': lows,
                'close': closes,
                'volume': volumes
            })
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_localize(self.tz_taiwan)
            
            print(f"OK - Created {len(df)} sample data points")
            print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            return df
            
        except Exception as e:
            print(f"Error creating sample data: {e}")
            return None
    
    def load_or_create_mxf_data(self, csv_file="mxf_1h_data.csv"):
        """Load existing MXF CSV or create sample data"""
        import os
        
        if os.path.exists(csv_file):
            try:
                print(f"\nLoading existing data: {csv_file}")
                df = pd.read_csv(csv_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            except Exception as e:
                print(f"Error loading file: {e}")
        
        print(f"\nFile not found: {csv_file}")
        print("Creating sample data for demonstration")
        return self.create_sample_mxf_data()
    
    def fetch_spy_data(self, start_date, end_date):
        """Fetch SPY 1-hour data"""
        print("\n" + "="*60)
        print("Fetching SPY data...")
        print("="*60)
        
        try:
            spy_data = yf.download(
                'SPY',
                start=start_date,
                end=end_date,
                interval='1h',
                progress=False
            )
            
            if spy_data.empty:
                print("Error - No SPY data")
                return None
            
            if isinstance(spy_data.columns, pd.MultiIndex):
                spy_data.columns = spy_data.columns.get_level_values(0)
            
            spy_data.index = spy_data.index.tz_convert(self.tz_est)
            
            print(f"OK - Got {len(spy_data)} SPY data points")
            print(f"Time range: {spy_data.index[0]} to {spy_data.index[-1]}")
            
            return spy_data
            
        except Exception as e:
            print(f"Error fetching SPY: {e}")
            return None
    
    def align_trading_sessions(self, mxf_df, spy_df):
        """
        Align MXF night session and SPY day session
        
        MXF night (Taiwan time UTC+8): 21:30 - 04:30 (8 hours)
        SPY day (US EST/EDT): 09:30 - 16:00 (6.5 hours)
        
        MXF 21:30 (TW) = SPY 08:30 EST (previous day)
        """
        print("\n" + "="*60)
        print("Aligning MXF night session and SPY day session")
        print("="*60)
        
        if mxf_df is None or spy_df is None:
            
