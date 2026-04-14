import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import time

async def scrape_tradingview_tmf():
    print("="*60)
    print("Scraping TMF data from TradingView")
    print("="*60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            # 打开 TradingView TMF 页面
            url = "https://tw.tradingview.com/chart/?symbol=TAIFEX%3ATMF1"
            print(f"\nOpening: {url}")
            await page.goto(url, wait_until='networkidle', timeout=60000)
            
            print("Waiting for page load...")
            await page.wait_for_timeout(5000)
            
            # 尝试获取数据
            print("Searching for data...")
            
            # 方法 1: 查找表格
            try:
                tables = await page.query_selector_all('table')
                if tables:
                    print(f"Found {len(tables)} tables")
                    
                    for i, table in enumerate(tables):
                        html = await table.inner_html()
                        print(f"Table {i} size: {len(html)} chars")
                        
                        # 尝试解析
                        import io
                        dfs = pd.read_html(io.StringIO(html))
                        if dfs:
                            print(f"Parsed {len(dfs)} dataframes from table {i}")
                            return dfs[0]
            except Exception as e:
                print(f"Table parsing failed: {e}")
            
            # 方法 2: 查找 iframe
            try:
                iframes = await page.query_selector_all('iframe')
                print(f"Found {len(iframes)} iframes")
            except:
                pass
            
            # 方法 3: 截图看看
            print("\nTaking screenshot...")
            await page.screenshot(path='tradingview.png')
            print("Screenshot saved to tradingview.png")
            
            # 方法 4: 获取页面源代码中的数据
            content = await page.content()
            print(f"Page content size: {len(content)} chars")
            
            # 查找 JSON 数据
            if '"data"' in content or 'OHLC' in content:
                print("Found data in page content")
                
                # 尝试提取 JSON
                import json
                import re
                
                # 查找 JSON 模式
                json_pattern = r'\{[^{}]*"o"[^{}]*"h"[^{}]*"l"[^{}]*"c"[^{}]*\}'
                matches = re.findall(json_pattern, content)
                
                if matches:
                    print(f"Found {len(matches)} potential data points")
                    return matches[:100]
            
            print("Could not extract data from page")
            return None
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None
        
        finally:
            await browser.close()

async def main():
    data = await scrape_tradingview_tmf()
    
    if data is not None:
        print("\n" + "="*60)
        print("Success!")
        print("="*60)
        print(data[:10])
    else:
        print("\nFailed to get data")
        print("Note: TradingView heavily restricts scraping")
        print("Alternative: Use their API or manual export")

if __name__ == '__main__':
    asyncio.run(main())
