"""
更新market_data_v2数据 - 补充3月27、28、31日数据
"""
import akshare as ak
import pandas as pd
import os
from datetime import datetime
import time

def get_last_date(filepath):
    """获取文件最后日期"""
    try:
        df = pd.read_csv(filepath)
        if len(df) > 0:
            return df['date'].iloc[-1]
    except:
        pass
    return None

def update_stock(symbol, data_dir):
    """更新单只股票"""
    filepath = os.path.join(data_dir, f"{symbol}.csv")
    
    last_date = get_last_date(filepath)
    if not last_date or last_date >= '2025-03-31':
        return True, 0
    
    try:
        # 下载3月27日后的数据
        df_new = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                    start_date='20250327',
                                    end_date='20250331')
        
        if len(df_new) == 0:
            return True, 0
        
        # 格式化数据
        df_formatted = pd.DataFrame({
            'date': df_new['日期'],
            'code': symbol,
            'open': df_new['开盘'],
            'high': df_new['最高'],
            'low': df_new['最低'],
            'close': df_new['收盘'],
            'volume': df_new['成交量'],
            'amount': df_new['成交额'],
            'pctChg': df_new['涨跌幅']
        })
        
        # 只保留文件中还没有的日期
        df_existing = pd.read_csv(filepath)
        new_dates = set(df_formatted['date']) - set(df_existing['date'])
        
        if len(new_dates) == 0:
            return True, 0
        
        df_to_append = df_formatted[df_formatted['date'].isin(new_dates)]
        df_to_append.to_csv(filepath, mode='a', header=False, index=False)
        
        return True, len(df_to_append)
        
    except Exception as e:
        return False, str(e)[:30]

def main():
    data_dir = "/Users/ydy/Downloads/market_data_v2"
    
    print("="*60)
    print("更新数据: 补充2025-03-27至03-31")
    print(f"开始时间: {datetime.now().strftime('%H:%M:%S')}")
    print("="*60)
    
    # 获取所有股票文件
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv') 
             and f[:6].isdigit() and len(f) == 10]
    files.sort()
    
    print(f"共 {len(files)} 只股票\n")
    
    success = 0
    skip = 0
    fail = 0
    total_added = 0
    
    for i, filename in enumerate(files):
        symbol = filename.replace('.csv', '')
        result, info = update_stock(symbol, data_dir)
        
        if result:
            if info > 0:
                success += 1
                total_added += info
                print(f"{symbol}: +{info}条")
            else:
                skip += 1
        else:
            fail += 1
            print(f"{symbol}: 失败 ({info})")
        
        # 每100只显示进度
        if (i + 1) % 100 == 0:
            print(f"\n--- 进度 {i+1}/{len(files)} | 成功:{success} 跳过:{skip} 失败:{fail} ---\n")
        
        time.sleep(0.05)
    
    print("="*60)
    print(f"完成! 成功:{success} 跳过:{skip} 失败:{fail}")
    print(f"共新增 {total_added} 条数据")
    print("="*60)

if __name__ == "__main__":
    main()
