import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import re
import os

class MXFCrawler:
    def __init__(self):
        self.output_file = "mxf_1h_data.csv"
        self.url = "https://tw.tradingview.com/chart/?symbol=TAIFEX%3AMXF1%21"
        
    async def fetch_mxf_data(self):
        """使用 Playwright 爬取 MXF 数据"""
        print("\n" + "="*60)
        print("📊 正在使用 Playwright 爬取 TradingView MXF 数据...")
        print("="*60)
        
        async with async_playwright() as p:
            try:
                # 启动浏览器
                print("🔧 启动浏览器...")
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()
                
                print(f"📍 打开页面: {self.url}")
                await page.goto(self.url, wait_until='networkidle', timeout=60000)
                
                # 等待页面加载
                print("⏳ 等待图表加载 (30 秒)...")
                await asyncio.sleep(30)
                
                # 注入 JavaScript 获取数据
                print("📈 提取图表数据...")
                
                script = """
                () => {
                    try {
                        // 获取所有 K 线 DOM 元素
                        const candles = [];
                        
                        // 方法 1: 从 chart 容器获取数据
                        const chartContainer = document.querySelector('[data-testid="chart-container"]') || 
                                             document.querySelector('.chart-container');
                        
                        if (chartContainer) {
                            console.log('找到图表容器');
                            
                            // 尝试获取所有可见的 K 线元素
                            const klineElements = chartContainer.querySelectorAll('rect[data-time]');
                            console.log('找到 K 线数量:', klineElements.length);
                            
                            klineElements.forEach(el => {
                                const time = el.getAttribute('data-time');
                                const open = el.getAttribute('data-open');
                                const high = el.getAttribute('data-high');
                                const low = el.getAttribute('data-low');
                                const close = el.getAttribute('data-close');
                                const volume = el.getAttribute('data-volume');
                                
                                if (time && close) {
                                    candles.push({
                                        time,
                                        open: parseFloat(open),
                                        high: parseFloat(high),
                                        low: parseFloat(low),
                                        close: parseFloat(close),
                                        volume: parseFloat(volume) || 0
                                    });
                                }
                            });
                        }
                        
                        // 方法 2: 从页面 text content 提取价格
                        if (candles.length === 0) {
                            const pageText = document.body.innerText;
                            console.log('页面文本长度:', pageText.length);
                            
                            // 返回页面信息用于调试
                            return {
                                status: 'extracted',
                                candles_found: candles.length,
                                page_text_sample: pageText.substring(0, 500)
                            };
                        }
                        
                        return {
                            status: 'success',
                            candles_count: candles.length,
                            data: candles.slice(-100)  // 返回最后 100 条
                        };
                    } catch(e) {
                        return {
                            status: 'error',
                            message: e.toString()
                        };
                    }
                }
                """
                
                result = await page.evaluate(script)
                print(f"📊 结果: {result}")
                
                # 尝试从页面表格中提取数据
                print("\n💡 尝试从页面表格中提取数据...")
                
                # 获取页面 HTML
                html = await page.content()
                
                # 查找包含数据的脚本标签或数据属性
                table_data = await page.evaluate("""
                    () => {
                        // 尝试获取所有表格数据
