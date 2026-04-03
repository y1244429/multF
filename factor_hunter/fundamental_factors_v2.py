#!/usr/bin/env python3
"""
基本面因子库构建 + IC分析
- 价值、成长、质量、盈利四大类（20个因子）
- 数据源：AKShare stock_financial_abstract_ths（东方财富/同花顺）
- IC截面：2023-01 ~ 2025-03（与V5价量因子一致）
"""

import os, glob, time, warnings
import numpy as np
import pandas as pd
import akshare as ak
from tqdm import tqdm

warnings.filterwarnings('ignore')

DATA_DIR   = '/Users/ydy/CodeBuddy/output/market_data'
OUTPUT_DIR = '/Users/ydy/WorkBuddy/20260326134350/factor_hunter/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 600只股票（与V5一致）───────────────────────────────────────────────────
files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))[:600]
STOCKS = [os.path.basename(f).replace('.csv', '') for f in files]
N = len(STOCKS)
print(f"股票数: {N}")

# ── 1. 拉取财务数据 ───────────────────────────────────────────────────────
CACHE = os.path.join(OUTPUT_DIR, 'fundamental_ths_cache.csv')

if os.path.exists(CACHE):
    print("📦 加载缓存...")
    all_fin = pd.read_csv(CACHE)
else:
    print(f"🌐 从AKShare拉取财务数据（预计 ~2分钟）...")
    rows = []
    fail = 0
    for code in tqdm(STOCKS, desc='拉取THS财务', ncols=80):
        try:
            df = ak.stock_financial_abstract_ths(symbol=code)
            if df is None or len(df) == 0:
                fail += 1
                continue
            df = df.copy()
            df['code'] = code
            rows.append(df)
        except Exception as e:
            fail += 1
        time.sleep(0.05)   # 50ms 节流

    print(f"  成功: {len(rows)} / {N}, 失败: {fail}")
    all_fin = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    all_fin.to_csv(CACHE, index=False, encoding='utf-8-sig')
    print(f"  已缓存: {CACHE}")

print(f"  财务数据: {all_fin.shape}")

# ── 2. 加载日线价格（用于估值因子）──────────────────────────────────────
print("📈 加载日线价格...")
price_dfs = []
for f in tqdm(files, desc='加载日线', ncols=80):
    code = os.path.basename(f).replace('.csv', '')
    df = pd.read_csv(f, usecols=['date', 'close'])
    df['code'] = code
    price_dfs.append(df)
price_df = pd.concat(price_dfs, ignore_index=True)
price_df['date'] = pd.to_datetime(price_df['date'])
print(f"  价格数据: {len(price_df)} 行, 日期: {price_df['date'].min().date()} ~ {price_df['date'].max().date()}")

# ── 3. 计算因子（每只股票取最新一期财务数据）─────────────────────────────
print("\n📊 计算基本面因子...")

def safe_float(x):
    try:
        return float(str(x).replace('%','').strip())
    except:
        return np.nan

def parse_pct(x):
    """解析百分比字符串，如 '12.5%' -> 12.5"""
    try:
        s = str(x).strip()
        if s == '-' or s == '' or s == 'nan':
            return np.nan
        if s.endswith('%'):
            return float(s[:-1])
        return float(s)
    except:
        return np.nan

def compute_factors(fin_df, price_df):
    """
    对每只股票取最新一期非空财务数据，计算因子
    """
    results = []

    for code in fin_df['code'].unique():
        sdf = fin_df[fin_df['code'] == code].copy()
        # 报告期排序，取最新一期
        sdf['报告期'] = pd.to_datetime(sdf['报告期'], errors='coerce')
        sdf = sdf.dropna(subset=['报告期']).sort_values('报告期', ascending=False)
        if len(sdf) == 0:
            continue
        row_data = sdf.iloc[0]

        r = {'code': code}

        # ── 辅助函数 ────────────────────────────────────────────────────
        def g(col):
            return safe_float(row_data.get(col, np.nan))

        def gp(col):
            """解析百分比"""
            return parse_pct(row_data.get(col, np.nan))

        # ── 质量因子（10个）────────────────────────────────────────────
        r['quality_roe']           = gp('净资产收益率')      # ROE
        r['quality_roe_diluted']  = gp('净资产收益率-摊薄') # ROE(摊薄)
        r['quality_net_margin']   = gp('销售净利率')         # 净利率
        r['quality_current_ratio']= g('流动比率')
        r['quality_quick_ratio']  = g('速动比率')
        r['quality_debt_ratio']   = gp('资产负债率')
        r['quality_equity_ratio'] = gp('产权比率')            # 杠杆指标
        r['quality_cps']          = g('每股经营现金流')       # 每股现金流
        r['quality_undist_profit']= g('每股未分配利润')      # 利润储备
        r['quality_accumulation'] = g('每股资本公积金')       # 资本公积

        # ── 盈利因子（5个）──────────────────────────────────────────────
        r['profit_eps']            = g('基本每股收益')
        r['profit_bps']            = g('每股净资产')
        r['profit_net_profit']     = g('净利润')
        r['profit_ex_net_profit']  = g('扣非净利润')
        r['profit_revenue']        = g('营业总收入')

        # ── 成长因子（5个）──────────────────────────────────────────────
        r['growth_net_profit']     = gp('净利润同比增长率')
        r['growth_ex_net_profit']  = gp('扣非净利润同比增长率')
        r['growth_revenue']        = gp('营业总收入同比增长率')
        # 用连续两年ROE变化代理ROE成长
        sdf2 = sdf.head(2)
        if len(sdf2) >= 2:
            roe1 = gp('净资产收益率')
            roe2 = safe_float(sdf2.iloc[1].get('净资产收益率', np.nan))
            r['growth_roe'] = (roe1 - roe2) if (not np.isnan(roe1) and not np.isnan(roe2)) else np.nan
        else:
            r['growth_roe'] = np.nan
        # 净利润绝对增长（亿元）
        r['growth_abs_profit'] = g('净利润')  # 最新一期绝对值

        # ── 价值因子（5个，需要价格）───────────────────────────────────
        # 用最新收盘价
        code_prices = price_df[price_df['code'] == code].sort_values('date')
        if len(code_prices) > 0:
            latest_price = code_prices['close'].iloc[-1]
            bps = g('每股净资产')
            eps = g('基本每股收益')
            cps = g('每股经营现金流')
            r['value_pb'] = latest_price / bps if bps > 0 else np.nan
            r['value_pe'] = latest_price / eps if eps > 0 else np.nan
            r['value_pc'] = latest_price / cps if cps > 0 else np.nan
            r['value_price'] = latest_price
            r['value_mkt_cap'] = latest_price * 1e8  # 近似总市值
        else:
            r['value_pb'] = r['value_pe'] = r['value_pc'] = np.nan
            r['value_price'] = r['value_mkt_cap'] = np.nan

        results.append(r)

    return pd.DataFrame(results)

fund_factors = compute_factors(all_fin, price_df)
fund_factors = fund_factors.dropna(subset=['code']).reset_index(drop=True)
print(f"  因子表: {fund_factors.shape}")
print(f"  因子列表: {[c for c in fund_factors.columns if c != 'code']}")

# ── 4. 月度收益率 ────────────────────────────────────────────────────────
print("\n📅 合成月度收益率...")
monthly = price_df.copy()
monthly['ym'] = monthly['date'].dt.to_period('M')
monthly_close = monthly.groupby(['code', 'ym'])['close'].last().reset_index()
monthly_close = monthly_close.sort_values(['code', 'ym'])
monthly_close['ret'] = monthly_close.groupby('code')['close'].pct_change()
monthly_close['ym_str'] = monthly_close['ym'].astype(str)

# IC截面：2023-01 ~ 2025-03
ic_months = sorted([m for m in monthly_close['ym_str'].unique() if '2023-01' <= m <= '2025-03'])
print(f"  IC截面月数: {len(ic_months)}")

# ── 5. 计算IC ───────────────────────────────────────────────────────────
FUND_FACTORS = [c for c in fund_factors.columns if c != 'code']
fund_factors_clean = fund_factors.set_index('code')

ic_data = {m: {} for m in ic_months}

for m in ic_months:
    # 当月收益
    ret_s = monthly_close[monthly_close['ym_str'] == m].set_index('code')['ret']
    for f in FUND_FACTORS:
        if f not in fund_factors_clean.columns:
            continue
        fac = fund_factors_clean[f]  # 因子值（最新一期财务，不做时间对齐）
        common = fac.index.intersection(ret_s.index).dropna()
        if len(common) < 30:
            continue
        ic = fac.loc[common].corr(ret_s.loc[common])
        if not pd.isna(ic):
            ic_data[m][f] = ic

ic_matrix = pd.DataFrame(ic_data).T
ic_matrix.index.name = '月份'
print(f"  IC矩阵: {ic_matrix.shape}")
print(f"  有IC的因子: {[c for c in ic_matrix.columns if ic_matrix[c].notna().any()]}")

# ── 6. 统计 ──────────────────────────────────────────────────────────────
if len(ic_matrix) > 0:
    stats = pd.DataFrame({
        '因子':        ic_matrix.columns,
        '平均IC':      ic_matrix.mean(),
        'IC_std':     ic_matrix.std(),
        'IR':          ic_matrix.mean() / ic_matrix.std(),
        'IC>0比例':   (ic_matrix > 0).mean(),
        '月份数':     ic_matrix.notna().sum(),
    })
    stats['|IC|'] = stats['平均IC'].abs()
    stats = stats.sort_values('IR', key=abs, ascending=False)
    stats.to_csv(os.path.join(OUTPUT_DIR, 'fundamental_ic_stats.csv'), index=False, encoding='utf-8-sig')
    print("\n基本面因子 IC/IR 全部排名:")
    print(stats[['因子','平均IC','IR','IC>0比例','月份数']].to_string())
    print(f"\n已保存: fundamental_ic_stats.csv")
