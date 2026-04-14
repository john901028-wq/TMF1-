import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import time
import os

class TAIFEXAutoScraper:
    def __init__(self):
        self.output_file = "mxf_1h_data.csv"
        self.taifex_url = "https://www.taifex.com.tw/cht/3/dlFutDataDown"
        
    async def fetch_mxf_data(self, start_date, end_date):
        """使用 Playwright 自动化浏览器爬取 MXF 数据"""
        print("\n" + "="*60)
        print("📊 正在使用浏览器爬取 TAIFEX MXF 数据...")
        print("="*60)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                print(f"📍 打开页面: {self.taifex_url}")
                await page.goto(self.taifex_url, wait_until='networkidle', timeout=30000)
                
                print("⏳ 等待页面加载...")
                await page.wait_for_timeout(3000)
                
                # 日期格式
                start_str = start_date.strftime('%Y/%m/%d')
                end_str = end_date.strftime('%Y/%m/%d')
                
                print(f"📅 设置日期范围: {start_str} 到 {end_str}")
                
                # 尝试找到表单元素
                try:
                    # 设置开始日期
                    start_input = await page.query_selector('input[name*="start"]')
                    if start_input:
                        await start_input.fill(start_str)
                        print(f"✅ 设置开始日期: {start_str}")
                except:
                    print("⚠️ 无法设置开始日期")
                
                try:
                    # 设置结束日期
                    end_input = await page.query_selector('input[name*="end"]')
                    if end_input:
                        await end_input.fill(end_str)
                        print(f"✅ 设置结束日期: {end_str}")
                except:
                    print("⚠️ 无法设置结束日期")
                
                try:
                    # 选择 MXF
                    select_elem = await page.query_selector('select, [name*="commodity"]')
                    if select_elem:
                        await select_elem.select_option('MXF')
                        print("✅ 选择 MXF")
                except:
                    print("⚠️ 无法选择 MXF")
                
                # 点击查询/下载按钮
                try:
                    query_btn = await page.query_selector('button, input[type="submit"], [onclick*="query"]')
                    if query_btn:
                        await query_btn.click()
                        print("✅ 点击查询按钮")
                        await page.wait_for_timeout(5000)
                except Exception as e:
                    print(f"⚠️ 无法点击查询按钮: {e}")
                
                # 等待数据加载
                await page.wait_for_timeout(3000)
                
                # 尝试获取 CSV 下载链接
                csv_links = await page.query_selector_all('a[href*=".csv"], a[href*="download"]')
                
                if csv_links:
                    print(f"✅ 找到 {len(csv_links)} 个下载链接")
                    
                    # 获取第一个 CSV 链接
                    href = await csv_links[0].get_attribute('href')
                    print(f"📥 下载链接: {href}")
                    
                    # 下载文件
                    if href:
                        async with page.context.request as request:
                            response = await request.get(href)
                            if response.ok:
                                content = await response.text()
                                return self.parse_csv(content)
                else:
                    print("⚠️ 未找到下载链接")
                
                # 获取页面中的表格数据
                table_data = await page.query_selector('table')
                if table_data:
                    print("📋 找到表格数据")
                    html = await table_data.inner_html()
                    return self.parse_html_table(html)
                
            except Exception as e:
                print(f"❌ 爬取失败: {e}")
                import traceback
                traceback.print_exc()
                return None
            
            finally:
                await browser.close()
                print("🔌 浏览器已关闭")
    
    def parse_csv(self, csv_content):
        """解析 CSV 内容"""
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content), encoding='utf-8-sig')
            print(f"✅ 解析成功: {len(df)} 行")
            return df
        except Exception as e:
            print(f"❌ CSV 解析失败: {e}")
            return None
    
    def parse_html_table(self, html):
        """从 HTML 表格解析数据"""
        try:
            dfs = pd.read_html(html)
            if dfs:
                df = dfs[0]
                print(f"✅ 解析表格: {len(df)} 行")
                return df
        except Exception as e:
            print(f"❌ HTML 解析失败: {e}")
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
    
    async def run(self, days_back=365):
        """主运行流程"""
        print("\n" + "="*60)
        print("🚀 TAIFEX 自动爬虫 - 开始运行")
        print(f"⏰ 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        df = await self.fetch_mxf_data(start_date, end_date)
        
        if df is not None and not df.empty:
            success = self.save_to_csv(df)
            if success:
                print("\n📊 数据样本:")
                print(df.head(10))
            return success
        
        print("⚠️ 爬取失败，建议手动下载")
        return False

def main():
    scraper = TAIFEXAutoScraper()
    asyncio.run(scraper.run(days_back=365))

if __name__ == '__main__':
    main()