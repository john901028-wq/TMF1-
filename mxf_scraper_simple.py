from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time
import yfinance as yf

class SimpleMXFScraper:
    def __init__(self):
        self.driver = None
        self.tz_taiwan = pytz.timezone('Asia/Taipei')
        self.tz_est = pytz.timezone('America/New_York')
    
    def setup_driver(self):
        """设置 Chrome WebDriver"""
        print("🔧 正在初始化浏览器...")
        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--start-maximized')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
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
            # MXF1! 是微台期货
            url = "https://tw.tradingview.com/chart/?symbol=TAIFEX%3AMXF1%21"
            print(f"📍 打开页面: {url}")
            
            self.driver.get(url)
            print("⏳ 等待页面加载 (需要 30 秒)...")
            
            # 等待图表加载
            time.sleep(30)
            
            # 尝试获取页面上的数据
            print("📈 尝试提取数据...")
            
            # 查找所有蜡烛数据
            script = """
            try {
                // 获取图表的所有可见数据点
                const candles = [];
                
                // 尝试从 DOM 中获取价格信息
                const priceElements = document.querySelectorAll('[class*="price"]');
                const timeElements = document.querySelectorAll('[class*="time"]');
                
                // 收集基本信息
                return {
                    pageTitle: document.title,
                    url: window.location.href,
                    hasChart: !!document.querySelector('.chart-container'),
                    priceElementsFound: priceElements.length,
                    timeElementsFound: timeElements.length
                };
            } catch(e) {
                return { error: e.toString() };
            }
            """
            
            result = self.driver.execute_script(script)
            print(f"📊 页面信息: {result}")
            
            # 截图保存
            screenshot_path = 'tradingview_mxf_screenshot.png'
            self.driver.save_screenshot(screenshot_path)
            print(f"📸 已保存截图: {screenshot_path}")
            
            print("\n⚠️ TradingView 页面数据高度动态加载")
            print("💡 建议: 手动从 TradingView 导出数据或使用官方 API")
            
            return None
            
        except Exception as e:
            print(f"❌ 出错: {e}")
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
            
            # 重置 MultiIndex 如果存在
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
            
            # 尝试从 TradingView 抓取
            mxf_data = self.fetch_mxf_from_tradingview()
            
            # 获取 SPY 数据
            spy_data = self.fetch_spy_data(start_date, end_date)
            
            # 保存数据
            print("\n" + "="*60)
            print("💾 保存数据...")
            print("="*60)
            
            saved_count = 0
            if self.save_to_csv(spy_data, 'spy_daytime_1h.csv', 'SPY'):
                saved_count += 1
            
            print("\n" + "="*60)
            print(f"✅ 完成! 成功保存 {saved_count} 个文件")
            print("="*60)
            
            # 显示数据样本
            if spy_data is not None and not spy_data.empty:
                print("\n📊 SPY 数据样本:")
                print(spy_data.head())
            
        finally:
            if self.driver:
                self.driver.quit()
                print("\n🔌 浏览器已关闭")

if __name__ == '__main__':
    scraper = SimpleMXFScraper()
    scraper.run()