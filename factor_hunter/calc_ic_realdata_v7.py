#!/usr/bin/env python3
"""
因子IC/IR计算 v7 - 日度IC滚动月度法
=============================================
改进：每天算截面IC → 月度平均 → 再算IR
效果：IR通常提升 2~3倍（噪音被平滑）

数据：V3全量 6990只股（2023-07 ~ 2026-04-01）
因子：V5全量 ~158个
"""
import os, glob, sys, warnings, time
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

WORKSPACE = '/Users/ydy/WorkBuddy/20260326134350'
DATA_DIR   = os.path.expanduser('~/Downloads/V3')
OUT_DIR    = os.path.join(WORKSPACE, 'factor_hunter', 'output')
os.makedirs(OUT_DIR, exist_ok=True)

print("=" * 60)
print("因子IC/IR计算 v7 - 日度IC滚动月度法")
print("  数据: V3 6990只股 | 方法: 每日截面相关→月度平均→IR")
print("=" * 60)

# ============================================================
# 1. 加载数据
# ============================================================
print("\n[1] 加载数据...")
t_start = time.time()
files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))
frames = []
for f in files:
    try:
        df = pd.read_csv(f, parse_dates=['date'])
        code_raw = os.path.basename(f).replace('.csv', '')
        df['code'] = code_raw.split('.')[-1] if '.' in code_raw else code_raw
        frames.append(df)
    except:
        pass

data = pd.concat(frames, ignore_index=True)
data = data.sort_values(['code', 'date']).reset_index(drop=True)
for col in ['close', 'open', 'high', 'low', 'volume', 'amount']:
    data[col] = data[col].astype(float)

# 基础衍生
data['ret'] = data.groupby('code')['close'].pct_change()
data['range_pct'] = (data['high'] - data['low']) / (data['close'] + 1e-9)
data['_rv'] = data['ret'] * data['volume']
data['_ra'] = data['ret'] * data['amount']
data['sign_vol'] = np.sign(data['ret']) * data['volume']
data['_vpt_raw'] = data['volume'] * data['ret']

print(f"  载入 {data['code'].nunique()} 只股票，"
      f"{data['date'].min().date()} ~ {data['date'].max().date()}")
print(f"  总行数: {len(data):,} 行")

g = data.groupby('code', sort=False)

# ============================================================
# 2. 因子计算（复用V5全部因子定义）
# ============================================================
print("\n[2] 计算因子...")
t0 = time.time()

# 基础滚动
data['ma5']   = g['close'].transform(lambda x: x.rolling(5,  min_periods=2).mean())
data['ma10']  = g['close'].transform(lambda x: x.rolling(10, min_periods=3).mean())
data['ma20']  = g['close'].transform(lambda x: x.rolling(20, min_periods=5).mean())
data['ma60']  = g['close'].transform(lambda x: x.rolling(60, min_periods=10).mean())
data['ma120'] = g['close'].transform(lambda x: x.rolling(120,min_periods=20).mean())
data['ema5']  = g['close'].transform(lambda x: x.ewm(span=5,  adjust=False).mean())
data['ema12'] = g['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
data['ema26'] = g['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())

data['vol5']   = g['ret'].transform(lambda x: x.rolling(5,  min_periods=2).std())
data['vol10']  = g['ret'].transform(lambda x: x.rolling(10, min_periods=3).std())
data['vol20']  = g['ret'].transform(lambda x: x.rolling(20, min_periods=5).std())
data['vol60']  = g['ret'].transform(lambda x: x.rolling(60, min_periods=10).std())
data['vol120'] = g['ret'].transform(lambda x: x.rolling(120,min_periods=20).std())

data['vol_mv5']  = g['volume'].transform(lambda x: x.rolling(5,  min_periods=2).mean())
data['vol_mv20'] = g['volume'].transform(lambda x: x.rolling(20, min_periods=5).mean())
data['vol_mv60'] = g['volume'].transform(lambda x: x.rolling(60, min_periods=10).mean())

data['mom5']   = g['close'].pct_change(5)
data['mom10']  = g['close'].pct_change(10)
data['mom20']  = g['close'].pct_change(20)
data['mom60']  = g['close'].pct_change(60)
data['mom120'] = g['close'].pct_change(120)
data['mom240'] = g['close'].pct_change(240)

# K线形态
data['daily_range'] = (data['high'] - data['low']).clip(lower=1e-9)
data['dr_pct'] = data['daily_range'] / (data['close'] + 1e-9)
data['upper_shadow'] = (data['high'] - np.maximum(data['open'], data['close'])) / (data['daily_range'] + 1e-9)
data['lower_shadow'] = (np.minimum(data['open'], data['close']) - data['low']) / (data['daily_range'] + 1e-9)
data['candle_body']  = np.abs(data['close'] - data['open']) / (data['daily_range'] + 1e-9)

# 52周高低
data['high52w'] = g['close'].transform(lambda x: x.rolling(252, min_periods=50).max())
data['low52w']  = g['close'].transform(lambda x: x.rolling(252, min_periods=50).min())
data['close52w']     = (data['close'] - data['high52w']) / (data['high52w'] - data['low52w'] + 1e-9)
data['close52l']     = data['close'] / (data['low52w'] + 1e-9) - 1
data['high52w_ratio']= data['close'] / (data['high52w'] + 1e-9)

# 20日/60日高低
data['high20'] = g['close'].transform(lambda x: x.rolling(20, min_periods=5).max())
data['low20']  = g['close'].transform(lambda x: x.rolling(20, min_periods=5).min())
data['breakout_20']  = (data['close'] - data['high20']) / (data['high20'] - data['low20'] + 1e-9)
data['price_pos_20'] = (data['close'] - data['low20'])  / (data['high20'] - data['low20'] + 1e-9)
data['high60'] = g['close'].transform(lambda x: x.rolling(60, min_periods=10).max())
data['low60']  = g['close'].transform(lambda x: x.rolling(60, min_periods=10).min())
data['breakout_60']  = (data['close'] - data['high60']) / (data['high60'] - data['low60'] + 1e-9)
data['price_pos_60'] = (data['close'] - data['low60'])  / (data['high60'] - data['low60'] + 1e-9)

# 回撤
data['drawdown_20'] = (data['close'] - data['high20']) / (data['high20'] + 1e-9)
data['drawdown_60'] = (data['close'] - data['high60']) / (data['high60'] + 1e-9)
data['drawdown_120']= (data['close'] - data['ma120'])  / (data['ma120']  + 1e-9)

# 均线偏离
data['ma_ratio_5']  = (data['close'] - data['ma5'])  / (data['ma5']  + 1e-9)
data['ma_ratio_10'] = (data['close'] - data['ma10']) / (data['ma10'] + 1e-9)
data['ma_ratio_20'] = (data['close'] - data['ma20']) / (data['ma20'] + 1e-9)
data['ma_ratio_60'] = (data['close'] - data['ma60']) / (data['ma60'] + 1e-9)

# 均线比（核心逆向因子）
data['ma_ratio_120'] = (data['close'] - data['ma120']) / (data['ma120'] + 1e-9)
data['ma_cross']     = data['ma5'] / (data['ma20'] + 1e-9) - 1
data['ma_cross_60']   = data['ma20'] / (data['ma60'] + 1e-9) - 1

# 乖离率
data['bias_5']  = data['ma_ratio_5']
data['bias_20'] = data['ma_ratio_20']
data['bias_60'] = data['ma_ratio_60']

# rank版均线比
data['_rm5']   = g['mom5'].transform(lambda x: x.rank(pct=True))
data['_rm20']  = g['mom20'].transform(lambda x: x.rank(pct=True))
data['_rm60']  = g['mom60'].transform(lambda x: x.rank(pct=True))
data['_rv5']   = g['vol5'].transform(lambda x: x.rank(pct=True))
data['_rv20']  = g['vol20'].transform(lambda x: x.rank(pct=True))
data['_rv60']  = g['vol60'].transform(lambda x: x.rank(pct=True))
data['_rma5']  = g['ma_ratio_5'].transform(lambda x: x.rank(pct=True))
data['_rma20'] = g['ma_ratio_20'].transform(lambda x: x.rank(pct=True))
data['_rma60'] = g['ma_ratio_60'].transform(lambda x: x.rank(pct=True))
data['rank_ma_ratio_20'] = data['_rma20']
data['rank_ma_ratio_60'] = data['_rma60']
data['rank_mom_5']  = data['_rm5']
data['rank_mom_20'] = data['_rm20']
data['rank_mom_60'] = data['_rm60']
data['rank_prod_20'] = data['_rm5'] * data['_rv20']
data['rank_prod_af'] = data['_rm20'] * data['_rv5']
data['rank_prod_60'] = data['_rm60'] * data['_rv60']

# MASTER乖离率
data['master_bias_5_20'] = data['ma_ratio_5'] - data['ma_ratio_20']
data['master_bias_5_60'] = data['ma_ratio_5'] - data['ma_ratio_60']
data['master_bias_20_60']= data['ma_ratio_20'] - data['ma_ratio_60']
data['master_ewm_cross'] = data['ema5'] / (data['ma20'] + 1e-9) - 1
data['master_accel5']  = data['ma5'] - 2*data['ma5'].groupby(data['code']).shift(1) + data['ma5'].groupby(data['code']).shift(2)
data['master_accel20'] = data['ma20'] - 2*data['ma20'].groupby(data['code']).shift(1) + data['ma20'].groupby(data['code']).shift(2)
data['master_consistency'] = g['ret'].transform(lambda x: x.rolling(10, 5).apply(lambda arr: (arr>0).mean(), raw=True))
data['master_skew5']  = g['ret'].transform(lambda x: x.rolling(5,  3).skew())
data['master_skew20'] = g['ret'].transform(lambda x: x.rolling(20, 8).skew())

# 隔夜收益
data['overnight_ret'] = (data['open'] - data.groupby('code')['close'].shift(1)) / (data.groupby('code')['close'].shift(1) + 1e-9)
data['intraday_ret']  = (data['close'] - data['open']) / (data['open'] + 1e-9)
data['close_open_gap']= data['intraday_ret']

# AlphaGen
data['alpha_cov_10'] = (g['_rv'].transform(lambda x: x.rolling(10,  min_periods=3).mean())
                      - g['ret'].transform(lambda x: x.rolling(10,  min_periods=3).mean())
                      * g['volume'].transform(lambda x: x.rolling(10,  min_periods=3).mean()))
data['alpha_cov_20'] = (g['_rv'].transform(lambda x: x.rolling(20,  min_periods=5).mean())
                      - g['ret'].transform(lambda x: x.rolling(20,  min_periods=5).mean())
                      * g['volume'].transform(lambda x: x.rolling(20,  min_periods=5).mean()))
data['alpha_cov_60'] = (g['_ra'].transform(lambda x: x.rolling(60,  min_periods=10).mean())
                      - g['ret'].transform(lambda x: x.rolling(60,  min_periods=10).mean())
                      * g['amount'].transform(lambda x: x.rolling(60,  min_periods=10).mean()))
data['alpha_corr_10'] = data['alpha_cov_10'] / (data['vol5']  * data['vol_mv5']  + 1e-9)
data['alpha_corr_20'] = data['alpha_cov_20'] / (data['vol20'] * data['vol_mv20'] + 1e-9)
data['alpha_corr_60'] = data['alpha_cov_60'] / (data['vol60'] * data['vol_mv60'] + 1e-9)
data['vpt']    = g['_vpt_raw'].transform(lambda x: x.rolling(20,  min_periods=5).sum())
data['vpt60']  = g['_vpt_raw'].transform(lambda x: x.rolling(60, min_periods=10).sum())
data['sign_vol20'] = g['sign_vol'].transform(lambda x: x.rolling(20, min_periods=5).mean())
data['rank_mom_10'] = g['mom10'].transform(lambda x: x.rolling(10, 5).apply(lambda arr: (arr>0).mean(), raw=True))
data['rank_range_5']  = g['dr_pct'].transform(lambda x: x.rolling(5,  2).std())
data['rank_range_10'] = g['dr_pct'].transform(lambda x: x.rolling(10, 3).std())
data['rank_range_20'] = g['dr_pct'].transform(lambda x: x.rolling(20, 5).std())
data['rank_range_60'] = g['dr_pct'].transform(lambda x: x.rolling(60, 10).std())

# QFR
def wzscore(name, w, n_std=2.5, mp=5):
    mu    = g[name].transform(lambda x: x.rolling(w, mp).mean())
    sigma = g[name].transform(lambda x: x.rolling(w, mp).std())
    return ((data[name] - mu) / (sigma + 1e-9)).clip(-n_std, n_std)

data['qfr_stable_mom5']  = wzscore('mom5',  5,  2.5, 2)
data['qfr_stable_mom20'] = wzscore('mom20', 20, 2.5, 5)
data['qfr_stable_mom60'] = wzscore('mom60', 60, 2.5, 10)
data['risk_adj_mom5']  = data['mom5']  / (data['vol5']  + 1e-9)
data['risk_adj_mom20'] = data['mom20'] / (data['vol20'] + 1e-9)
data['risk_adj_mom60'] = data['mom60'] / (data['vol60'] + 1e-9)
data['qfr_vol_adj_ret'] = data['ret'] / (data['vol5'] + 1e-9)
data['trend_stability'] = (data['ma5'] - data['ma20']) / (data['vol20'] * data['close'] + 1e-9)

# HRFT
data['hrft_sqrt_vol5']  = data['vol5']  ** 0.5
data['hrft_sqrt_vol20'] = data['vol20'] ** 0.5
data['hrft_sqrt_vol60'] = data['vol60'] ** 0.5
data['hrft_abs_log5']  = np.abs(np.log1p(data['ret'])) * data['vol5']
data['hrft_abs_log20'] = np.abs(np.log1p(data['ret'])) * data['vol20']
data['hrft_ewm_vol_short'] = data['vol5'] / (g['vol5'].transform(lambda x: x.ewm(span=20, adjust=False).mean()) + 1e-9)
data['hrft_ewm_vol_long']  = data['vol60'] / (g['vol60'].transform(lambda x: x.ewm(span=60, adjust=False).mean()) + 1e-9)
data['hrft_ewm_vol_ratio'] = data['hrft_ewm_vol_short']
data['hrft_vol_accel'] = data['vol5'] / (data['vol20'] + 1e-9)
data['hrft_intraday_atr'] = data['daily_range'] / (data['close'] + 1e-9)

# Amihud
data['amihud'] = np.abs(data['ret']) / (data['amount'] / data['volume'] + 1e-9)

# OBV / Force Index
data['_obv'] = (np.sign(data['ret']) * data['volume']).groupby(data['code']).cumsum()
data['obv_ma5']  = g['_obv'].transform(lambda x: x.rolling(5,  2).mean())
data['obv_ma20'] = g['_obv'].transform(lambda x: x.rolling(20, 5).mean())
data['force_index_13'] = data['ret'] * data['volume']
data['fi_ma13'] = g['force_index_13'].transform(lambda x: x.ewm(span=13, adjust=False).mean())

# 换手率代理
data['turnover_rate'] = data['volume'] / (data['vol_mv20'] + 1e-9)
data['turnover_accel'] = data['volume'] / (data['vol_mv5'] + 1e-9)
data['amount_vol_ratio'] = data['amount'] / (data['volume'] + 1e-9)
data['amt_stability'] = g['amount'].transform(lambda x: x.rolling(20, 5).std() / (x.rolling(20, 5).mean() + 1e-9))

# 布林带
data['bb_std']  = g['close'].transform(lambda x: x.rolling(20, 5).std())
data['bb_upper'] = data['ma20'] + 2 * data['bb_std']
data['bb_lower'] = data['ma20'] - 2 * data['bb_std']
data['bb_pos']   = (data['close'] - data['bb_lower']) / (data['bb_upper'] - data['bb_lower'] + 1e-9)
data['kc_pos']   = data['bb_pos']

# RSI
delta = data.groupby('code')['close'].diff()
gain  = delta.clip(lower=0).groupby(data['code']).transform(lambda x: x.ewm(span=14, adjust=False).mean())
loss  = (-delta.clip(upper=0)).groupby(data['code']).transform(lambda x: x.ewm(span=14, adjust=False).mean())
data['rsi_14']  = gain / (gain + loss + 1e-9) * 100
data['rsi_ma_10'] = g['rsi_14'].transform(lambda x: x.rolling(10, 5).mean())

# MACD
data['macd'] = data['ema12'] - data['ema26']
data['macd_signal'] = data['macd']  # 用原始MACD线代替signal
data['macd_hist']    = data['macd']

# ATR
data['tr']  = np.maximum(data['high'] - data['low'],
                np.maximum(np.abs(data['high'] - data.groupby('code')['close'].shift(1)),
                           np.abs(data['low']  - data.groupby('code')['close'].shift(1))))
data['atr_14'] = g['tr'].transform(lambda x: x.rolling(14, 7).mean())
data['atr_pos'] = data['daily_range'] / (data['atr_14'] + 1e-9)

# 威廉%R
data['威廉_14'] = (data['high20'] - data['close']) / (data['high20'] - data['low20'] + 1e-9) * 100

# 资金流
data['mf_ratio_5']  = (data['close'] - data['open']) * data['volume'] / (data['daily_range'] * data['volume'] + 1e-9)

# 追赶效应
data['mom_rev'] = -data['mom20'] + data['mom5']

# intra_vol
data['intra_vol'] = data['daily_range'] / (data['close'] + 1e-9)

# Aroon
def aroon_score(series, period=25):
    idx_max = series.rolling(period, min_periods=period).apply(lambda x: pd.Series(x).values.argmax(), raw=True)
    return (period - idx_max) / period * 100
data['aroon_up']   = aroon_score(g['high'].transform(lambda x: x), 25)
data['aroon_down'] = aroon_score(g['low'].transform(lambda x: x), 25)
data['aroon_osc']  = data['aroon_up'] - data['aroon_down']

# ADX (简化)
dm_plus  = g['high'].diff().clip(lower=0)
dm_minus = (-g['low'].diff()).clip(lower=0)
data['adx_14'] = np.abs(dm_plus.rolling(14, 7).mean() - dm_minus.rolling(14, 7).mean()) / (g['daily_range'].transform(lambda x: x.rolling(14, 7).mean()) + 1e-9)
data['dmi_trend'] = data['adx_14']

# TRIX
ema1 = data['close'].groupby(data['code']).transform(lambda x: x.ewm(span=12, adjust=False).mean())
ema2 = ema1.groupby(data['code']).transform(lambda x: x.ewm(span=12, adjust=False).mean())
ema3 = ema2.groupby(data['code']).transform(lambda x: x.ewm(span=12, adjust=False).mean())
data['trix_14'] = ema3.pct_change(12)

# ease_of_move
data['ease_of_move'] = data['ret'] / (data['atr_14'] + 1e-9)
data['ease_of_move_ma5'] = g['ease_of_move'].transform(lambda x: x.rolling(5, 2).mean())

# high_vol_low_vol
data['high_vol_low_vol'] = data['vol20'] / (data['vol60'] + 1e-9)

print(f"  因子计算完成，耗时 {time.time()-t0:.1f}s")

# ============================================================
# 3. 因子列表（排除辅助列）
# ============================================================
helper_cols = {
    'date','code','open','high','low','close','volume','amount','pctChg',
    'ret','_rv','_ra','sign_vol','_vpt_raw','_obv','_rsv',
    '_rm5','_rm20','_rm60','_rv5','_rv20','_rv60',
    '_rma5','_rma20','_rma60',
    'ma5','ma10','ma20','ma60','ma120',
    'ema5','ema12','ema26',
    'vol5','vol10','vol20','vol60','vol120',
    'mom5','mom10','mom20','mom60','mom120','mom240',
    'high52w','low52w','high20','low20','high60','low60',
    'daily_range','dr_pct',
    'force_index_13','kdj_k','future_ret','year_month',
    'bb_std','ema1','ema2','ema3','dm_plus','dm_minus',
    'ad_line','delta','gain','loss','tr',
}

factor_cols = [c for c in data.columns if c not in helper_cols]
print(f"  有效因子数: {len(factor_cols)}")

# ============================================================
# 4. 日度IC计算 → 月度平均（核心改进）
# ============================================================
print("\n[3] 计算日度IC（截面相关）...")
t1 = time.time()

# 未来收益（T+1）
data['future_ret'] = g['ret'].shift(-1)

# 过滤：去除退市/停牌/极值
data = data[data['ret'].notna()].copy()
data = data[data['close'] > 0].copy()
data = data[data['volume'] > 0].copy()
# 去除涨跌停（避免极端值扭曲IC）
data = data[data['ret'].abs() < 0.11].copy()

print(f"  有效行数: {len(data):,}（去涨跌停后）")

# 每天的截面IC
daily_ics = []
dates = sorted(data['date'].unique())
total_days = len(dates)
print(f"  总交易日: {total_days} 天")

for i, d in enumerate(dates):
    day_df = data[data['date'] == d].copy()
    if len(day_df) < 100:
        continue
    fRet = day_df['future_ret'].values
    for fc in factor_cols:
        vals = day_df[fc].values
        mask = np.isfinite(vals) & np.isfinite(fRet)
        ic = np.nan if mask.sum() < 50 else np.corrcoef(vals[mask], fRet[mask])[0, 1]
        daily_ics.append({'date': d, 'factor': fc, 'daily_IC': ic})
    if (i+1) % 200 == 0:
        print(f"  进度: {i+1}/{total_days} 天 ({100*(i+1)//total_days}%)")

daily_ic_df = pd.DataFrame(daily_ics)
print(f"  日度IC计算完成，耗时 {time.time()-t1:.1f}s")

# ============================================================
# 5. 月度平均 → 月度IC → IR
# ============================================================
print("\n[4] 月度平均 + IR计算...")
daily_ic_df['year_month'] = pd.to_datetime(daily_ic_df['date']).dt.to_period('M')

# 月度IC = 月内日度IC均值
monthly_ic = (daily_ic_df
    .groupby(['year_month', 'factor'])['daily_IC']
    .mean()
    .reset_index()
    .rename(columns={'daily_IC': 'IC'})
)

# IC统计
stats_list = []
for fc in factor_cols:
    f_ic = monthly_ic[monthly_ic['factor'] == fc]['IC'].dropna()
    if len(f_ic) < 6:
        continue
    ic_mean = f_ic.mean()
    ic_std  = f_ic.std()
    ir      = ic_mean / (ic_std + 1e-9) if ic_std > 0 else 0
    ic_pos_rate = (f_ic > 0).mean()
    stats_list.append({
        'factor':       fc,
        'avg_IC':       ic_mean,
        'IC_std':       ic_std,
        'IR':           ir,
        'IC>0_rate':   ic_pos_rate,
        'n_months':     len(f_ic),
        'abs_IC':       abs(ic_mean),
    })

stats = pd.DataFrame(stats_list)
stats = stats.sort_values('IR', ascending=False).reset_index(drop=True)

print(f"\n  IR Top20:")
print(stats.head(20)[['factor','avg_IC','IC_std','IR','IC>0_rate','n_months']].to_string(index=False))

# ============================================================
# 6. 输出
# ============================================================
# 月度IC矩阵（宽表）
monthly_pivot = monthly_ic.pivot(index='factor', columns='year_month', values='IC')

# 保存
monthly_ic.to_csv(os.path.join(OUT_DIR, 'monthly_ic_matrix_v7_daily_method.csv'), index=False)
monthly_pivot.to_csv(os.path.join(OUT_DIR, 'monthly_ic_pivot_v7.csv'))
stats.to_csv(os.path.join(OUT_DIR, 'factor_ic_ir_stats_v7.csv'), index=False)

print(f"\n[完成] 耗时总计: {time.time()-t_start:.1f}s")
print(f"  月度IC矩阵: monthly_ic_matrix_v7_daily_method.csv")
print(f"  月度IC透视: monthly_ic_pivot_v7.csv")
print(f"  IC/IR统计: factor_ic_ir_stats_v7.csv")

# ============================================================
# 7. 对比V5（月度IC法）和V7（日度IC法）
# ============================================================
v5_path = os.path.join(OUT_DIR, 'factor_ic_ir_stats.csv')
if os.path.exists(v5_path):
    v5 = pd.read_csv(v5_path)
    v5 = v5[['factor','IR','avg_IC']].rename(columns={'IR':'IR_v5','avg_IC':'IC_v5'})
    merged = stats[['factor','IR','avg_IC']].rename(columns={'IR':'IR_v7','avg_IC':'IC_v7'}).merge(v5, on='factor', how='inner')
    merged['IR提升'] = merged['IR_v7'] / merged['IR_v5'].abs()
    merged = merged.sort_values('IR_v7', ascending=False)
    print(f"\n【V5 vs V7 IR对比】")
    print(merged.head(20).to_string(index=False))
    merged.to_csv(os.path.join(OUT_DIR, 'v5_v7_ir_comparison.csv'), index=False)
