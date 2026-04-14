import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta, time
import yfinance as yf
import os

class MarketDataFetcher:
    def __init__(self):
        # 时区设置
        self.tz_est = pytz.timezone('America/New_York')
        self.tz_taiwan = pytz.timezone('Asia/Taipei')
        self.tz_utc = pytz.UTC
        
        # 交易时间定义
        self.us_market_open = time(9, 30)    # 美股开盘 09:30 EST
        self.us_market_close = time(16, 0)   # 美股收盘 16:00 EST
        self.tw_market_open = time(8, 45)    # 台股开盘 08:45 CST
        self.tw_market_close = time(13, 45)  # 台股收盘 13:45 CST
    
    def is_trading_day(self, date):
        """检查是否为交易日 (周一到周五)"""
        return date.weekday() < 5
    
    def get_overlapping_hours(self, date):
        """获取当天台股和美股都开盘的交易时段"""
        return True
    
    def fetch_spy_data(self, start_date, end_date):
        """获取 SPY (S&P 500) 1小时数据"""
        print("\n" + "="*60)
        print("📊 正在获取 SPY (美股 S&P 500) 数据...")
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
            
            # 转换时区到 EST
            spy_data.index = spy_data.index.tz_convert(self.tz_est)
            
            print(f"✅ 成功获取 {len(spy_data)} 条 SPY 数据")
            print(f"   时间范围: {spy_data.index[0]} 到 {spy_data.index[-1]}")
            
            return spy_data
            
        except Exception as e:
            print(f"❌ 获取 SPY 数据失败: {e}")
            return None
    
    def fetch_taiwan_stock_data(self, start_date, end_date):
        """获取台股数据"""
        print("\n" + "="*60)
        print("📈 正在获取台股数据...")
        print("="*60)
        
        try:
            tw_data = yf.download(
                '2330.TW',
                start=start_date,
                end=end_date,
                interval='1h',
                progress=False
            )
            
            if tw_data.empty:
                print("⚠️ 无法从 yfinance 获取台股数据")
                return None
            
            tw_data.index = tw_data.index.tz_convert(self.tz_taiwan)
            
            print(f"✅ 成功获取 {len(tw_data)} 条台股数据")
            print(f"   时间范围: {tw_data.index[0]} 到 {tw_data.index[-1]}")
            
            return tw_data
            
        except Exception as e:
            print(f"❌ 获取台股数据失败: {e}")
            return None
    
    def filter_trading_hours(self, spy_data, tw_data):
        """过滤只保留台股和美股都开盘的时段"""
        print("\n" + "="*60)
        print("🔄 过滤台股和美股都开盘的交易时段...")
        print("="*60)
        
        if spy_data is None:
            print("❌ 无 SPY 数据")
            return None, None
        
        spy_data_cst = spy_data.copy()
        spy_data_cst.index = spy_data_cst.index.tz_convert(self.tz_taiwan)
        
        mask_spy = (
            (spy_data_cst.index.time >= self.tw_market_open) &
            (spy_data_cst.index.time <= self.tw_market_close) &
            (spy_data_cst.index.weekday < 5)
        )
        
        spy_filtered = spy_data_cst[mask_spy].copy()
        spy_filtered.index = spy_filtered.index.tz_convert(self.tz_est)
        
        print(f"✅ 过滤后 SPY 数据: {len(spy_filtered)} 条")
        if len(spy_filtered) > 0:
            print(f"   时间范围: {spy_filtered.index[0]} 到 {spy_filtered.index[-1]}")
        
        tw_filtered = None
        if tw_data is not None:
            mask_tw = (
                (tw_data.index.time >= self.tw_market_open) &
                (tw_data.index.time <= self.tw_market_close) &
                (tw_data.index.weekday < 5)
            )
            tw_filtered = tw_data[mask_tw].copy()
            print(f"✅ 过滤后台股数据: {len(tw_filtered)} 条")
        
        return spy_filtered, tw_filtered
    
    def save_to_csv(self, data, filename, symbol_name):
        """保存数据到 CSV"""
        if data is None or data.empty:
            print(f"⚠️ {symbol_name} 数据为空，跳过保存")
            return False
        
        try:
            data.to_csv(filename)
            print(f"✅ {symbol_name} 数据已保存到: {filename}")
            print(f"   记录数: {len(data)}")
            return True
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False
    
    def run(self):
        """主流程"""
        print("\n" + "="*60)
        print("🚀 市场数据爬虫 - 开始运行")
        print("="*60)
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        print(f"\n📅 获取日期范围:")
        print(f"   开始: {start_date.date()}")
        print(f"   结束: {end_date.date()}")
        
        # 获取原始数据
        spy_data = self.fetch_spy_data(start_date, end_date)
        tw_data = self.fetch_taiwan_stock_data(start_date, end_date)
        
        # 过滤交易时段
        spy_filtered, tw_filtered = self.filter_trading_hours(spy_data, tw_data)
        
        # 保存数据
        print("\n" + "="*60)
        print("💾 保存数据...")
        print("="*60)
        
        saved_count = 0
        if self.save_to_csv(spy_filtered, 'spy_1h_trading_hours.csv', 'SPY'):
            saved_count += 1
        
        if self.save_to_csv(tw_filtered, 'taiwan_stock_1h_trading_hours.csv', '台股'):
            saved_count += 1
        
        print("\n" + "="*60)
        print(f"✅ 完成! 成功保存 {saved_count} 个文件")
        print("="*60)
        
        if spy_filtered is not None and not spy_filtered.empty:
            print("\n📊 SPY 数据样本 (前5条):")
            print(spy_filtered.head())
            print("\n最后5条:")
            print(spy_filtered.tail())

if __name__ == '__main__':
    fetcher = MarketDataFetcher()
    fetcher.run()