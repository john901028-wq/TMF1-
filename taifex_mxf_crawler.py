import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

class TAIFEXMXFCrawler:
    def __init__(self):
        self.base_url = "https://www.taifex.com.tw/cht/3/downloadDailyFutCSV"
        self.output_file = "mxf_1h_data.csv"
        
    def fetch_mxf_data(self, start_date, end_date):
        """从 TAIFEX 官网爬取 MXF 数据"""
        print("\n" + "="*60)
        print("📊 正在从台湾期交所爬取 MXF 数据...")
        print("="*60)
        
        try:
            # 日期格式: yyyy/mm/dd
            start_str = start_date.strftime('%Y/%m/%d')
            end_str = end_date.strftime('%Y/%m/%d')
            
            print(f"📅 时间范围: {start_str} 到 {end_str}")
            
            params = {
                'queryType': '2',
                'marketCode': '0',
                'commodity_id': 'MXF',  # 微台期货代码
                'queryStartDate': start_str,
                'queryEndDate': end_str
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            print("⏳ 正在请求数据...")
            response = requests.post(self.base_url, data=params, headers=headers, timeout=30)
            
            print(f"📋 HTTP 状态码: {response.status_code}")
            
            if response.status_code == 200:
                # 检查是否是有效的 CSV 数据
                if 'Date' in response.text or '日期' in response.text:
                    print("✅ 成功获取数据")
                    return self.parse_taifex_data(response.text)
                else:
                    print("⚠️ 响应不是有效数据")
                    print(f"📋 响应内容 (前 200 字): {response.text[:200]}")
                    return None
            else:
                print(f"❌ 请求失败: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ 爬取失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def parse_taifex_data(self, csv_content):
        """解析 TAIFEX CSV 数据"""
        try:
            from io import StringIO
            
            # 跳过可能的BOM或其他前缀
            lines = csv_content.strip().split('\n')
            
            # 打印前几行用于调试
            print(f"📋 CSV 前 3 行:")
            for i, line in enumerate(lines[:3]):
                print(f"  {i}: {line[:100]}")
            
            # 读取 CSV
            df = pd.read_csv(StringIO(csv_content), encoding='utf-8-sig')
            
            # 检查列名
            print(f"📋 列名: {list(df.columns)}")
            
            # 根据列名重命名（TAIFEX 的列名可能是中文）
            rename_dict = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if '日期' in col or 'date' in col_lower:
                    rename_dict[col] = 'timestamp'
                elif '開' in col or 'open' in col_lower:
                    rename_dict[col] = 'open'
                elif '高' in col or 'high' in col_lower:
                    rename_dict[col] = 'high'
                elif '低' in col or 'low' in col_lower:
                    rename_dict[col] = 'low'
                elif '收' in col or 'close' in col_lower:
                    rename_dict[col] = 'close'
                elif '成交' in col or 'volume' in col_lower or 'qty' in col_lower:
                    rename_dict[col] = 'volume'
            
            df = df.rename(columns=rename_dict)
            
            # 保留需要的列
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            available_cols = [col for col in required_cols if col in df.columns]
            df = df[available_cols]
            
            # 转换时间戳
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
            # 删除无效行
            df = df.dropna(subset=['timestamp'])
            
            if df.empty:
                print("❌ 数据为空")
                return None
            
            print(f"✅ 解析成功: {len(df)} 行数据")
            print(f"📅 时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
            
            return df
            
        except Exception as e:
            print(f"❌ 解析失败: {e}")
            import traceback
            traceback.print_exc()
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
            return True
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False
    
    def run(self, days_back=365):
        """主运行流程"""
        print("\n" + "="*60)
        print("🚀 TAIFEX MXF 爬虫 - 开始运行")
        print(f"⏰ 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # 获取数据
        df = self.fetch_mxf_data(start_date, end_date)
        
        if df is None or df.empty:
            print("⚠️ 无法获取数据")
            return False
        
        # 保存数据
        success = self.save_to_csv(df)
        
        # 显示样本
        if success and df is not None:
            print("\n📊 数据样本（最近10条）:")
            print(df.tail(10))
        
        print("\n" + "="*60)
        print("✅ 爬虫运行完成!")
        print("="*60)
        
        return success

if __name__ == '__main__':
    crawler = TAIFEXMXFCrawler()
    crawler.run(days_back=365)