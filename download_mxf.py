import requests
import pandas as pd
from datetime import datetime, timedelta
import json

def download_mxf_data():
    """从 TradingView 下载 MXF 1小时数据"""
    print("\n" + "="*60)
    print("📊 正在下载 MXF 数据...")
    print("="*60)
    
    try:
        # 方案 1: 尝试 TradingView 的官方 API
        symbol = 'TAIFEX:MXF1!'
        
        # 计算时间范围 (过去 365 天)
        to_time = int(datetime.now().timestamp())
        from_time = int((datetime.now() - timedelta(days=365)).timestamp())
        
        print(f"📅 时间范围: {datetime.fromtimestamp(from_time).date()} 到 {datetime.fromtimestamp(to_time).date()}")
        
        # 多个可能的 API 端点
        urls = [
            f"https://symbol-candle-data.tradingview-reuters.com/symbol?symbol={symbol}&resolution=60&from={from_time}&to={to_time}",
            f"https://www.tradingview.com/api/v1/symbols/{symbol}/candles?resolution=60&from={from_time}&to={to_time}",
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        data = None
        for url in urls:
            try:
                print(f"\n⏳ 尝试 URL: {url[:80]}...")
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data and (('candles' in data) or ('o' in data)):
                            print(f"✅ 成功获取数据!")
                            break
                    except:
                        pass
            except Exception as e:
                print(f"❌ 失败: {str(e)[:50]}")
                continue
        
        if data is None:
            print("\n⚠️ TradingView API 不可用")
            print("💡 建议: 手动从 TradingView 导出 CSV 文件")
            return False
        
        # 解析数据
        candles = data.get('candles', data.get('c', []))
        if not candles:
            print("❌ 无 K 线数据")
            return False
        
        # 提取时间、开高低收
        times = data.get('t', data.get('time', []))
        opens = data.get('o', data.get('open', []))
        highs = data.get('h', data.get('high', []))
        lows = data.get('l', data.get('low', []))
        closes = data.get('c', data.get('close', []))
        volumes = data.get('v', data.get('volume', [0]*len(candles)))
        
        if not all([times, opens, highs, lows, closes]):
            print("❌ 数据格式不完整")
            print(f"📋 数据键: {data.keys()}")
            return False
        
        # 转换为 DataFrame
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(times, unit='s'),
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        })
        
        # 保存 CSV
        filename = 'mxf_1h_data.csv'
        df.to_csv(filename, index=False)
        print(f"\n✅ 已保存到: {filename}")
        print(f"📊 数据行数: {len(df)}")
        print(f"📅 时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
        print(f"\n数据样本:")
        print(df.head(10))
        return True
        
    except Exception as e:
        print(f"❌ 出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    download_mxf_data()