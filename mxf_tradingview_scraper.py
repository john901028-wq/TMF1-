import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time
import json
import yfinance as yf

class TradingViewMXFScraper:
    def __init__(self):
        self.driver = None
        self.tz_taiwan = pytz.timezone('Asia/Taipei')
        self.tz_est = pytz.timezone('America/New_York')
    
    def setup_driver(self):
        """设置 Chrome WebDriver"""
        print("🔧 正在初始化浏览器...")
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            
            self.driver = uc.Chrome(options=options)
            print("✅ 浏览器已启动")
            return True
        except Exception as e:
            print(f"❌ 浏览器启动失败: {e}")
            return False
    
    def fetch_mxf_from_tradingview(self):
        """从 TradingView 抓取 MXF 数据"""
        print("\n" + "="*60)
        print("📊 正在从 TradingView 抓取 MXF (微台) 数据...")
        print("="*60)
        
        try:
            url = "https://tw.tradingview.com/chart/?symbol=TAIFEX%3AMXF1%21"
            print(f"📍 打开页面: {url}")
            
            self.driver.get(url)
            print("⏳ 等待页面加载...")
            
            # 等待图表容器加载
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "chart-container"))
            )
            
            print("✅ 页面已加载")
            time.sleep(5)
            
            # 尝试通过 JavaScript 获取图表数据
            script = """
            try {
                // 尝试从 TradingView 的全局对象获取数据
                if (window.tvWidget && window.tvWidget.activeChart()) {
                    const chart = window.tvWidget.activeChart();
                    return {
                        symbol: chart.symbol(),
                        resolution: chart.resolution(),
                        status: 'success'
                    };
                }
                return { status: 'no_widget', message: 'tvWidget not found' };
            } catch(e) {
                return { status: 'error', message: e.toString() };
            }
            """
            
            result = self.driver.execute_script(script)
            print(f"📈 图表信息: {result}")
            
            # TradingView 使用高度动态的数据加载
            # 直接从页面 DOM 提取数据困难，改用替代方案
            print("\n⚠️ TradingView 动态数据提取困难")
            print("💡 建议使用其他数据源...")
            
            return None
            
        except TimeoutException:
            print("❌ 页面加载超时")
            return None
        except Exception as e:
            print(f"❌ 出错: {e}")
            return None
    
    def fetch_mxf_from_taifex_api(self):
        """从台湾期交所 API 获取 MXF 数据"""
        print("\n" + "="*60)
        print("📊 正在从台湾期交所 API 获取 MXF 数据...")
        print("="*60)
        
        try:
            import requests
            
            # 台湾期交所 API 端点 (示例)
            # 实际端点可能需要调整
            url = "https://www.taifex.com.tw/cht/api/minuteSeriesLineChart"
            
            # 参数示例
            params = {
                'queryStartDate': (datetime.now() - timedelta(days=365)).strftime('%Y%m%d'),
                'queryEndDate': datetime.now().strftime('%Y%m%d'),
                'sessionStr': 'MXF',
                'commodity': 'MXF'
            }
            
            print(f"📍 请求 API: {url}")
            print(f"📋 参数: {params}")
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ 成功获取数据")
                
                # 解析数据
                df = self.parse_taifex_data(data)
                return df
            else:
                print(f"❌ API 返回状态码: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ API 请求失败: {e}")
            return None
    
    def parse_taifex_data(self, data):
        """解析期交所数据"""
        try:
            # 这里需要根据实际的 API 响应格式进行解析
            # 示例结构（需要验证）
            
            records = []
            if isinstance(data, dict) and 'data' in data:
                for item in data['data']:
                    records.append({
                        'timestamp': item.get('timestamp'),
                        'open': float(item.get('open', 0)),
                        'high': float(item.get('high', 0)),
                        'low': float(item.get('low', 0)),
                        'close': float(item.get('close', 0)),
                        'volume': int(item.get('volume', 0))
                    })
            
            if not records:
                print("⚠️ 无法解析数据")
                return None
            
            df = pd.DataFrame(records)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.index = df.index.tz_localize('UTC').tz_convert(self.tz_taiwan)
            
            print(f"✅ 解析成功: {len(df)} 条数据")
            return df
            
        except Exception as e:
            print(f"❌ 解析失败: {e}")
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
            
            # 重置 MultiIndex
            if isinstance(spy_data.columns, pd.MultiIndex):
                spy_data.columns = spy_data.columns.get_level_values(0)
            
            spy_data.index = spy_data.index.tz_convert(self.tz_est)
            
            print(f"✅ 成功获取 {len(spy_data)} 条 SPY 数据")
            print(f"   时间范围: {spy_data.index[0]} 到 {spy_data.index[-1]}")
            
            return spy_data
            
        except Exception as e:
            print(f"❌ 获取失败: {e}")
            return None
    
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
        
        if not self.setup_driver():
            print("❌ 无法启动浏览器，退出")
            return
        
        try:
            # 计算日期范围
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            print(f"\n📅 获取日期范围:")
            print(f"   开始: {start_date.date()}")
            print(f"   结束: {end_date.date()}")
            
            # 方案1: 尝试从期交所 API 获取
            mxf_data = self.fetch_mxf_from_taifex_api()
            
            # 方案2: 如果失败，尝试 TradingView
            if mxf_data is None:
                print("\n💡 尝试从 TradingView 抓取...")
                mxf_data = self.fetch_mxf_from_tradingview()
            
            # 获取 SPY 数据
            spy_data = self.fetch_spy_data(start_date, end_date)
            
            # 保存数据
            print("\n" + "="*60)
            print("💾 保存数据...")
            print("="*60)
            
            saved_count = 0
            if self.save_to_csv(mxf_data, 'mxf_nightsession.csv', 'MXF 微台'):
                saved_count += 1
            
            if self.save_to_csv(spy_data, 'spy_daytime_1h.csv', 'SPY'):
                saved_count += 1
            
            print("\n" + "="*60)
            print(f"✅ 完成! 成功保存 {saved_count} 个文件")
            print("="*60)
            
        finally:
            if self.driver:
                self.driver.quit()
                print("\n🔌 浏览器已关闭")

if __name__ == '__main__':
    scraper = TradingViewMXFScraper()
    scraper.run()