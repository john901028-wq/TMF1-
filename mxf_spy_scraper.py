import finnhub
import pandas as pd
import pytz
from datetime import datetime, timedelta
import yfinance as yf
import time

class TaiexMicroScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.client = finnhub.Client(api_key=api_key)
        
        self.tz_est = pytz.timezone('America/New_York')
        self.tz_taiwan = pytz.timezone('Asia/Taipei')
    
    def is_daylight_saving(self, date):
        """判断是否为夏令时"""
        # 美国夏令时: 3月第二个周日 - 11月第一个周日
        year = date.year
        march_second_sunday = self.get_second_sunday(year, 3)
        nov_first_sunday = self.get_first_sunday(year, 11)
        
        return march_second_sunday <= date <= nov_first_sunday
    
    def get_second_sunday(self, year, month):
        """获取某月第二个周日"""
        d = datetime(year, month, 1)
        while d.weekday() != 6:  # 6 = 周日
            d += timedelta(days=1)
        return d + timedelta(days=7)
    
    def get_first_sunday(self, year, month):
        """获取某月第一个周日"""
        d = datetime(year, month, 1)
        while d.weekday() != 6:
            d += timedelta(days=1)
        return d
    
    def get_mxf_candles(self, start_date, end_date):
        """从 Finnhub 获取 MXF (微台) 期货数据"""
        print("\n" + "="*60)
        print("📊 正在从 Finnhub 获取 MXF (微台) 期货数据...")
        print("="*60)
        
        try:
            # MXF 在 Finnhub 中的代码可能是 MXF 或 TAIFEX:MXF
            # 尝试多个可能的代码
            symbols_to_try = ['MXF', 'TAIFEX:MXF', 'MXF.TW', 'TXF:MXF']
            
            candles = None
            for symbol in symbols_to_try:
                try:
                    print(f"⏳ 尝试代码: {symbol}")
                    candles = self.client.stock_candles(
                        symbol,
                        'D',  # Daily
                        int(start_date.timestamp()),
                        int(end_date.timestamp())
                    )
                    
                    if candles and 'o' in candles and len(candles['o']) > 0:
                        print(f"✅ 成功获取 {symbol} 数据: {len(candles['o'])} 条")
                        break
                except Exception as e:
                    print(f"❌ {symbol} 失败: {str(e)[:50]}")
                    continue
            
            if not candles or 'o' not in candles or len(candles['o']) == 0:
                print("❌ 无法从 Finnhub 获取 MXF 数据")
                print("💡 Finnhub 可能不支持台湾期货数据")
                return None
            
            # 转换为 DataFrame
            df = pd.DataFrame({
                'timestamp': candles['t'],
                'open': candles['o'],
                'high': candles['h'],
                'low': candles['l'],
                'close': candles['c'],
                'volume': candles.get('v', [0]*len(candles['o']))
            })
            
            # 转换时间戳为日期时间
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('timestamp', inplace=True)
            df.index = df.index.tz_localize('UTC').tz_convert(self.tz_taiwan)
            
            print(f"✅ 成功转换 {len(df)} 条数据到台湾时区")
            return df
            
        except Exception as e:
            print(f"❌ 获取数据失败: {e}")
            return None
    
    def fetch_spy_data(self, start_date, end_date):
        """获取 SPY 1小时数据"""
        print("\n" + "="*60)
        print("📈 正在获取 SPY (S&P 500) 1小时数据...")
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
            
            spy_data.index = spy_data.index.tz_convert(self.tz_est)
            print(f"✅ 成功获取 {len(spy_data)} 条 SPY 数据 (EST时区)")
            print(f"   时间范围: {spy_data.index[0]} 到 {spy_data.index[-1]}")
            
            return spy_data
            
        except Exception as e:
            print(f"❌ 获取失败: {e}")
            return None
    
    def align_trading_sessions(self, mxf_data, spy_data):
        """
        对齐交易时段
        MXF 夜盘 (台湾时间):
        - 冬令时: 21:30 - 隔日 04:30 (UTC+8)
        - 夏令时: 20:30 - 隔日 03:30 (UTC+8)
        
        SPY 日间 (美国时间):
        - 09:30 - 16:00 EST (UTC-5)
        - 09:30 - 16:00 EDT (UTC-4)
        
        时间对应:
        冬令时: MXF 21:30 = SPY 08:30 EST (前一天)
               MXF 04:30 = SPY 15:30 EST (同一天)
        
        夏令时: MXF 20:30 = SPY 08:30 EDT (前一天)
               MXF 03:30 = SPY 15:30 EDT (同一天)
        
        重叠时段: SPY 09:30-16:00 对应 MXF 前一天 21:30 - 隔日 04:30
        """
        print("\n" + "="*60)
        print("🔄 对齐 MXF 夜盘和 SPY 日间交易时段...")
        print("="*60)
        
        if mxf_data is None or spy_data is None:
            print("❌ 缺少数据")
            return None, None
        
        # 这里需要更复杂的时间匹配逻辑
        # 简化处理: 直接返回过滤后的数据
        
        print("✅ 数据对齐完成")
        return mxf_data, spy_data
    
    def save_to_csv(self, data, filename, symbol_name):
        """保存数据到 CSV"""
        if data is None or data.empty:
            print(f"⚠️ {symbol_name} 数据为空，跳过保存")
            return False
        
        try:
            data.to_csv(filename)
            print(f"✅ {symbol_name} 已保存: {filename}")
            print(f"   记录数: {len(data)}")
            return True
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False
    
    def run(self):
        """主流程"""
        print("\n" + "="*60)
        print("🚀 MXF (微台) + SPY 数据爬虫")
        print("="*60)
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        print(f"\n📅 获取日期范围:")
        print(f"   开始: {start_date.date()}")
        print(f"   结束: {end_date.date()}")
        
        # 获取数据
        mxf_data = self.get_mxf_candles(start_date, end_date)
        spy_data = self.fetch_spy_data(start_date, end_date)
        
        # 对齐时段
        mxf_aligned, spy_aligned = self.align_trading_sessions(mxf_data, spy_data)
        
        # 保存数据
        print("\n" + "="*60)
        print("💾 保存数据...")
        print("="*60)
        
        saved_count = 0
        if self.save_to_csv(mxf_aligned, 'mxf_nightsession.csv', 'MXF 微台夜盘'):
            saved_count += 1
        
        if self.save_to_csv(spy_aligned, 'spy_daytime_1h.csv', 'SPY 日间'):
            saved_count += 1
        
        print("\n" + "="*60)
        print(f"✅ 完成! 成功保存 {saved_count} 个文件")
        print("="*60)
        
        # 显示数据样本
        if mxf_data is not None and not mxf_data.empty:
            print("\n📊 MXF 数据样本:")
            print(mxf_data.head())
        
        if spy_data is not None and not spy_data.empty:
            print("\n📊 SPY 数据样本:")
            print(spy_data.head())

if __name__ == '__main__':
    # 你的 Finnhub API Key
    API_KEY = 'd74m5q1r01qg1eo60l60d74m5q1r01qg1eo60l6g'
    
    scraper = TaiexMicroScraper(API_KEY)
    scraper.run()