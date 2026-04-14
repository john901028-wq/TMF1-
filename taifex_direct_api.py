import requests
import pandas as pd
from datetime import datetime, timedelta
import json

class TAIFEXDirectAPI:
    def __init__(self):
        self.output_file = "mxf_1h_data.csv"
        
    def fetch_mxf_from_api(self, start_date, end_date):
        """直接从期交所 API 获取数据"""
        print("\n" + "="*60)
        print("📊 正在从期交所 API 获取 MXF 数据...")
        print("="*60)
        
        # 尝试多个可能的 API 端点
        apis = [
            # 方案 1: 期交所官方 API
            {
                'url': 'https://www.taifex.com.tw/api/v1/dailyFuturesData',
                'params': {
                    'commodity': 'MXF',
                    'startDate': start_date.strftime('%Y%m%d'),
                    'endDate': end_date.strftime('%Y%m%d')
                }
            },
            # 方案 2: 另一个端点
            {
                'url': 'https://www.taifex.com.tw/cht/3/download',
                'params': {
                    'queryType': 'dayFutures',
                    'commodity': 'MXF',
                    'queryStartDate': start_date.strftime('%Y/%m/%d'),
                    'queryEndDate': end_date.strftime('%Y/%m/%d')
                }
            },
            # 方案 3: CSV 直接下载
            {
                'url': 'https://www.taifex.com.tw/cht/3/downloadDailyFutCSV',
                'params': {
                    'queryType': '2',
                    'commodity': 'MXF',
                    'queryStartDate': start_date.strftime('%Y/%m/%d'),
                    'queryEndDate': end_date.strftime('%Y/%m/%d')
                },
                'method': 'post'
            }
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for api_config in apis:
            try:
                url = api_config['url']
                params = api_config['params']
                method = api_config.get('method', 'get')
                
                print(f"\n⏳ 尝试: {url}")
                print(f"   参数: {params}")
                
                if method == 'post':
                    response = requests.post(url, data=params, headers=headers, timeout=15)
                else:
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                
                print(f"   状态码: {response.status_code}")
                
                if response.status_code == 200:
                    # 检查是否是有效内容
                    if len(response.text) > 100:
                        print(f"✅ 成功! 响应大小: {len(response.text)} 字节")
                        
                        # 尝试作为 CSV 解析
                        try:
                            from io import StringIO
                            df = pd.read_csv(StringIO(response.text), encoding='utf-8-sig')
                            print(f"✅ CSV 解析成功: {len(df)} 行")
                            return df
                        except:
                            print("⚠️ CSV 解析失败，尝试 JSON...")
                            try:
                                data = response.json()
                                print(f"✅ JSON 解析成功")
                                return self.parse_json_data(data)
                            except:
                                print("⚠️ JSON 解析也失败")
                                continue
            
            except Exception as e:
                print(f"❌ 错误: {str(e)[:100]}")
                continue
        
        print("\n⚠️ 所有 API 端点均失败")
        return None
    
    def parse_json_data(self, data):
        """解析 JSON 数据"""
        try:
            if isinstance(data, dict):
                # 尝试从 data 字段提取
                if 'data' in data:
                    records = data['data']
                else:
                    records = list(data.values())
            else:
                records = data
            
            df = pd.DataFrame(records)
            print(f"✅ 解析成功: {len(df)} 行")
            return df
        except Exception as e:
            print(f"❌ 解析失败: {e}")
            return None
    
    def save_to_csv(self, df):
        """保存数据"""
        if df is None or df.empty:
            print("❌ 无数据可保存")
            return False
        
        try:
            df.to_csv(self.output_file, index=False)
            print(f"✅ 已保存: {self.output_file}")
            print(f"📊 数据行数: {len(df)}")
            return True
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False
    
    def run(self, days_back=365):
        """主运行流程"""
        print("\n" + "="*60)
        print("🚀 期交所直接 API 爬虫")
        print(f"⏰ 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"📅 日期范围: {start_date.date()} 到 {end_date.date()}")
        
        df = self.fetch_mxf_from_api(start_date, end_date)
        
        if df is not None and not df.empty:
            success = self.save_to_csv(df)
            if success:
                print("\n📊 数据样本:")
                print(df.head())
            return success
        
        print("\n❌ 无法获取数据")
        return False

if __name__ == '__main__':
    scraper = TAIFEXDirectAPI()
    scraper.run(days_back=365)