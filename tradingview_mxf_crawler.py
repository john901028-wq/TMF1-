import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import os

class TradingViewMXFCrawler:
    def __init__(self):
        self.base_url = "https://www.tradingview.com"
        self.symbol = "TAIFEX:MXF1!"
        self.resolution = "60"  # 1 小时
        self.output_file = "mxf_1h_data.csv"
        
    def fetch_mxf_data(self, days_back=365):
        """从 TradingView 爬取 MXF 1小时数据"""
        print("\n" + "="*60)
        print("📊 正在爬取 TradingView MXF 数据...")
        print("="*60)
        
        try:
            # 计算时间戳
            to_timestamp = int(datetime.now().timestamp())
            from_timestamp = int((datetime.now() - timedelta(days=days_back)).timestamp())
            
            print(f"📅 时间范围: {datetime.fromtimestamp(from_timestamp).date()} 到 {datetime.fromtimestamp(to_timestamp).date()}")
            
            # TradingView 的 charting 库 API
            url = "https://www.tradingview.com/api/v1/symbols/TAIFEX:MXF1!/candles"
            
            params = {
                'resolution': self.resolution,
                'from': from_timestamp,
                'to': to_timestamp
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://tw.tradingview.com/chart/',
            }
            
            print("⏳ 正在请求数据...")
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            print(f"📋 HTTP 状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                return self.parse_tradingview_data(data)
            else:
                print(f"⚠️ API 返回状态码 {response.status_code}")
                return self.fetch_alternative_source()
                
        except Exception as e:
            print(f"❌ 请求失败: {e}")
            return self.fetch_alternative_source()
    
    def parse_tradingview_data(self, data):
        """解析 TradingView API 返回的数据"""
        try:
            if not data or 'candles' not in data:
                print("❌ 无 candles 数据")
                return None
            
            candles = data['candles']
            if len(candles) == 0:
                print("❌ 数据为空")
                return None
            
            print(f"✅ 获取 {len(candles)} 条 K 线数据")
            
            # 提取数据
            df = pd.DataFrame({
                'timestamp': [datetime.fromtimestamp(c['time']) for c in candles],
                'open': [c['open'] for c in candles],
                'high': [c['high'] for c in candles],
                'low': [c['low'] for c in candles],
                'close': [c['close'] for c in candles],
                'volume': [c.get('volume', 0) for c in candles]
            })
            
            return df
            
        except Exception as e:
            print(f"❌ 解析失败: {e}")
            return None
    
    def fetch_alternative_source(self):
        """备选方案：尝试其他数据源"""
        print("\n💡 尝试备选数据源...")
        
        try:
            # 备选 URL 列表
            alt_urls = [
                "https://www.tradingview-reuters.com/symbols/TAIFEX:MXF1!/data",
                "https://data.tradingview.com/symbols/TAIFEX:MXF1!/candles",
            ]
            
            for alt_url in alt_urls:
                try:
                    print(f"⏳ 尝试: {alt_url}")
                    response = requests.get(alt_url, timeout=10)
                    if response.status_code == 200:
                        print(f"✅ 响应成功")
                        return response.json()
                except:
                    continue
            
            print("⚠️ 所有数据源均不可用")
            return None
            
        except Exception as e:
            print(f"❌ 备选源失败: {e}")
            return None
    
    def save_to_csv(self, df):
        """保存数据到 CSV"""
        if df is None or df.empty:
            print("❌ 无数据可保存")
            return False
        
        try:
            df.to_csv(self.output_file, index=False)
            print(f"✅ 已保存到: {self.output_file}")
            print(f"📊 数据行数: {len(df)}")
            print(f"📅 时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
            return True
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False
    
    def load_existing_data(self):
        """加载现有数据（用于增量更新）"""
        if os.path.exists(self.output_file):
            try:
                df = pd.read_csv(self.output_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                print(f"✅ 加载现有数据: {len(df)} 行")
                return df
            except Exception as e:
                print(f"⚠️ 加载失败: {e}")
                return None
        return None
    
    def merge_data(self, new_df, existing_df):
        """合并新旧数据（去重）"""
        if existing_df is None:
            return new_df
        
        if new_df is None:
            return existing_df
        
        # 合并数据
        merged = pd.concat([existing_df, new_df], ignore_index=True)
        
        # 按时间排序并去重
        merged['timestamp'] = pd.to_datetime(merged['timestamp'])
        merged = merged.drop_duplicates(subset=['timestamp'], keep='last')
        merged = merged.sort_values('timestamp')
        
        print(f"✅ 合并后数据: {len(merged)} 行")
        return merged
    
    def run(self, days_back=365):
        """主运行流程"""
        print("\n" + "="*60)
        print("🚀 TradingView MXF 爬虫 - 开始运行")
        print(f"⏰ 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # 获取新数据
        new_df = self.fetch_mxf_data(days_back)
        
        if new_df is None or new_df.empty:
            print("⚠️ 无法获取新数据")
            return False
        
        # 加载现有数据
        existing_df = self.load_existing_data()
        
        # 合并数据
        final_df = self.merge_data(new_df, existing_df)
        
        # 保存数据
        success = self.save_to_csv(final_df)
        
        # 显示样本
        if success and final_df is not None and not final_df.empty:
            print("\n📊 数据样本（最近10条）:")
            print(final_df.tail(10))
        
        print("\n" + "="*60)
        print("✅ 爬虫运行完成!")
        print("="*60)
        
        return success

if __name__ == '__main__':
    crawler = TradingViewMXFCrawler()
    crawler.run(days_back=365)