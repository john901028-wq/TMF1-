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
        """创建示例 MXF 数据（用于测试）"""
        print("\n" + "="*60)
        print("📊 创建示例 MXF 数据...")
        print("="*60)
        
        try:
            # 生成日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # MXF 夜盘交易时间 (台湾时间)
            # 冬令时: 21:30 - 次日 04:30
            # 夏令时: 20:30 - 次日 03:30
            dates = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []
            
            # 初始价格
            price = 32000
            
            current_date = start_date
            while current_date < end_date:
                # 每个交易日生成 8 根 1 小时 K 线 (夜盘)
                for hour in range(8):
                    # 随机走势
                    change = np.random.randn() * 50  # 随机变化
                    price = price + change
                    
                    open_price = price
                    high_price = price + abs(np.random.randn()) * 30
                    low_price = price - abs(np.random.randn()) * 30
                    close_price = price + np.random.randn() * 20
                    volume = np.random.randint(100, 5000)
                    
                    # 夜盘时间: 21:30 + hour
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
            
            # 转换为台湾时区
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_localize(self.tz_taiwan)
            
            print(f"✅ 创建了 {len(df)} 条示例数据")
            print(f"📅 时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
            
            return df
            
        except Exception as e:
            print(f"❌ 创建失败: {e}")
            return None
    
    def load_or_create_mxf_data(self, csv_file="mxf_1h_data.csv"):
        """加载现有 MXF CSV，或创建示例数据"""
        import os
        
        if os.path.exists(csv_file):
            try:
                print(f"\n✅ 加载现有数据: {csv_file}")
                df = pd.read_csv(csv_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            except Exception as e:
                print(f"❌ 加载失败: {e}")
        
        print(f"⚠️ 文件不存在: {csv_file}")
        print("📝 将创建示例数据用于演示")
        return self.create_sample_mxf_data()
    
    def fetch_spy_data(self, start_date, end_date):
        """获取 SPY 1小时数据"""
        print("\n" + "="*60)
        print("📈 正在获取 SPY 数据...")
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
                print("❌ SPY 数据为空")
                return None
            
            # 重置 MultiIndex
            if isinstance(spy_data.columns, pd.MultiIndex):
                spy_data.columns = spy_data.columns.get_level_values(0)
            
            # 转换时区到 EST
            spy_data.index = spy_data.index.tz_convert(self.tz_est)
            
            print(f"✅ 获取 {len(spy_data)} 条 SPY 数据")
            print(f"📅 时间范围: {spy_data.index[0]} 到 {spy_data.index[-1]}")
            
            return spy_data
            
        except Exception as e:
            print(f"❌ 获取失败: {e}")
            return None
    
    def align_trading_sessions(self, mxf_df, spy_df):
        """
        对齐 MXF 夜盘和 SPY 日间交易时段
        
        MXF 夜盘 (台湾时间 UTC+8):
        - 21:30 - 04:30 (8 小时)
        
        SPY 日间 (美国 EST/EDT):
        - 09:30 - 16:00 (6.5 小时)
        
        时间对应:
        - MXF 21:30 (台) = SPY 08:30 EST (前一天) = SPY 09:30 EDT (前一天)
        - MXF 04:30 (台) = SPY 15:30 EST (同一天) = SPY 16:30 EDT (同一天)
        
        配对逻辑: MXF 的夜盘对应 SPY 前一天的日间交易
        """
        print("\n" + "="*60)
        print("🔄 对齐 MXF 夜盘 (台湾) 和 SPY 日间 (美国)")
        print("="*60)
        
        if mxf_df is None or spy_df is None:
            print("❌ 数据不完整")
            return None, None
        
        try:
            # MXF: 转换为 UTC 便于比较
            mxf_df_copy = mxf_df.copy()
            if 'timestamp' in mxf_df_copy.columns:
                mxf_df_copy['timestamp'] = pd.to_datetime(mxf_df_copy['timestamp'])
                if
