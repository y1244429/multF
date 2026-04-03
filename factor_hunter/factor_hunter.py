#!/usr/bin/env python3
"""
================================================================================
  A股因子猎人 — Factor Hunter v2
  基于市场真实数据，自动搜索最优因子组合
  目标：90天内收益最高，最大回撤最小
  
  v2修复：
  - 因子改为逐日计算（截面因子），不再用标量
  - 修复 ret_1d 最后一天 NaN 导致全丢的问题
  - 优化采样和缓存策略
================================================================================
"""

import os
import sys
import json
import glob
import warnings
import logging
import traceback
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('factor_hunter')

# ============================================================================
# 配置
# ============================================================================
DATA_DIR = "/Users/ydy/Downloads/market_data_v2"
WORKSPACE = "/Users/ydy/WorkBuddy/20260326134350/factor_hunter"
OUTPUT_DIR = os.path.join(WORKSPACE, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# 第一部分：数据加载
# ============================================================================
def load_all_stocks(max_stocks=None, min_days=200):
    """加载所有股票数据"""
    logger.info(f"从 {DATA_DIR} 加载股票数据...")
    all_data = {}
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    
    for f in files:
        code = os.path.basename(f).replace(".csv", "")
        try:
            df = pd.read_csv(f, parse_dates=['date'])
            df = df.sort_values('date').reset_index(drop=True)
            if len(df) >= min_days and code.startswith(('0', '3', '6')):
                all_data[code] = df
        except Exception:
            pass
    
    if max_stocks and len(all_data) > max_stocks:
        np.random.seed(42)
        keys = list(all_data.keys())
        np.random.shuffle(keys)
        all_data = {k: all_data[k] for k in keys[:max_stocks]}
    
    logger.info(f"加载完成：{len(all_data)} 只股票")
    return all_data


# ============================================================================
# 第二部分：逐日截面因子计算
# ============================================================================
def compute_factor_panel(all_data, lookback_days=90):
    """
    计算横截面因子面板
    返回：dict {date: DataFrame(code, factor1, factor2, ..., ret_fwd)}
    每个日期一张截面表，所有股票的因子值 + 未来收益
    """
    logger.info("计算横截面因子面板...")
    
    # 收集所有日期
    all_dates = set()
    for code, df in all_data.items():
        for d in df['date'].values:
            all_dates.add(d)
    all_dates = sorted(all_dates)
    
    # 取最近 lookback_days 个交易日
    trading_dates = all_dates[-lookback_days:]
    logger.info(f"  交易日数: {len(trading_dates)}")
    
    # 预计算每只股票的技术指标
    stock_factors = {}
    for code, df in all_data.items():
        try:
            stock_factors[code] = _compute_stock_indicators(df)
        except Exception:
            pass
    
    logger.info(f"  预计算完成: {len(stock_factors)} 只股票")
    
    # 构建面板
    panel = {}
    for i, date in enumerate(trading_dates):
        rows = []
        for code, sf in stock_factors.items():
            if date not in sf.index:
                continue
            row = sf.loc[date]
            # 未来1日收益
            if i + 1 < len(trading_dates):
                next_date = trading_dates[i + 1]
                if next_date in sf.index:
                    price_today = sf.loc[date, 'close']
                    price_tomorrow = sf.loc[next_date, 'close']
                    row['ret_fwd'] = (price_tomorrow - price_today) / price_today
                    rows.append(row)
        
        if len(rows) >= 20:
            panel[date] = pd.DataFrame(rows)
    
    logger.info(f"  截面面板构建完成: {len(panel)} 个交易日")
    return panel, trading_dates


def _compute_stock_indicators(df):
    """为单只股票计算所有技术指标（逐日）"""
    df = df.sort_values('date').set_index('date')
    c = df['close'].astype(float)
    h = df['high'].astype(float)
    l = df['low'].astype(float)
    o = df['open'].astype(float)
    v = df['volume'].astype(float)
    amt = df['amount'].astype(float)
    
    result = pd.DataFrame(index=df.index)
    result['close'] = c
    
    # ===== 动量类 =====
    for p in [5, 10, 15, 20, 30, 60, 90, 120]:
        if len(c) > p:
            result[f'mom_{p}'] = c.pct_change(p) * 100
    
    # 动量加速度
    if len(c) > 20:
        result['mom_accel'] = c.pct_change(5) * 100 - c.pct_change(10) * 100
    
    # ===== 均线偏离 =====
    for p in [5, 10, 20, 40, 60]:
        if len(c) > p:
            ma = c.rolling(p).mean()
            result[f'bias_ma{p}'] = (c - ma) / ma * 100
    
    # 均线斜率
    for p in [10, 20, 40, 60]:
        if len(c) > p + 5:
            ma = c.rolling(p).mean()
            result[f'ma{p}_slope'] = (ma - ma.shift(5)) / ma * 100
    
    # 均线多头排列
    if len(c) > 60:
        ma5 = c.rolling(5).mean()
        ma10 = c.rolling(10).mean()
        ma20 = c.rolling(20).mean()
        ma60 = c.rolling(60).mean()
        result['ma_bull'] = ((ma5 > ma10).astype(float) + 
                             (ma10 > ma20).astype(float) + 
                             (ma20 > ma60).astype(float))
    
    # ===== 波动率 =====
    for p in [5, 10, 20, 60]:
        if len(c) > p:
            result[f'volatility_{p}'] = c.pct_change().rolling(p).std() * np.sqrt(252) * 100
    
    # ATR
    if len(c) > 20:
        tr = pd.concat([
            h - l,
            (h - c.shift(1)).abs(),
            (l - c.shift(1)).abs()
        ], axis=1).max(axis=1)
        result['atr_20'] = tr.rolling(20).mean() / c * 100
    
    # ===== 成交量 =====
    for p in [5, 10, 20, 60]:
        if len(v) > p:
            result[f'vol_ratio_{p}'] = v / v.rolling(p).mean()
            result[f'amt_ratio_{p}'] = amt / amt.rolling(p).mean()
    
    # ===== 价格位置 =====
    for p in [10, 20, 60]:
        if len(c) > p:
            hh = h.rolling(p).max()
            ll = l.rolling(p).min()
            result[f'price_pos_{p}'] = (c - ll) / (hh - ll) * 100
    
    # 距新高天数
    for p in [20, 60]:
        if len(c) > p:
            result[f'days_high_{p}'] = (h.rolling(p).apply(lambda x: p - 1 - np.argmax(x[::-1]), raw=True))
    
    # ===== 回撤 =====
    for p in [20, 60]:
        if len(c) > p:
            result[f'drawdown_{p}'] = (c - h.rolling(p).max()) / h.rolling(p).max() * 100
    
    # ===== 突破 =====
    if len(c) > 21:
        result['breakout_20'] = (c > h.shift(1).rolling(20).max()).astype(float)
    if len(c) > 61:
        result['breakout_60'] = (c > h.shift(1).rolling(60).max()).astype(float)
    
    # ===== K线形态 =====
    body = (c - o) / (h - l + 0.0001) * 100
    result['candle_body'] = body
    
    # 连涨天数
    up = (c > c.shift(1)).astype(float)
    result['consec_up'] = up.groupby((up == 0).cumsum()).cumsum()
    
    # 量价背离
    if len(c) > 20:
        price_chg = c.pct_change(20) * 100
        vol_chg = v.rolling(20).mean() / v.rolling(20).mean().shift(10) - 1
        result['vol_price_div'] = price_chg - vol_chg * 100
    
    return result


# ============================================================================
# 第三部分：单因子回测
# ============================================================================
def backtest_single_factor(panel, factor_name, top_pct=0.1):
    """
    在面板上测试单因子
    策略：每个交易日按因子排序，选前 top_pct 股票，等权持有1天
    """
    daily_rets = []
    ic_values = []
    
    dates = sorted(panel.keys())
    
    for i in range(len(dates) - 1):
        today = dates[i]
        tomorrow = dates[i + 1]
        
        cross = panel[today].copy()
        cross = cross.dropna(subset=[factor_name, 'ret_fwd'])
        
        if len(cross) < 30:
            continue
        
        # 选Top N
        n_select = max(5, int(len(cross) * top_pct))
        top = cross.nlargest(n_select, factor_name)
        
        # 等权收益
        port_ret = top['ret_fwd'].mean()
        daily_rets.append({'date': today, 'return': port_ret})
        
        # IC
        ic = cross[factor_name].corr(cross['ret_fwd'])
        if not np.isnan(ic):
            ic_values.append(ic)
    
    if len(daily_rets) < 20:
        return None
    
    rets = [r['return'] for r in daily_rets]
    returns = np.array(rets)
    
    total = (1 + returns).prod() - 1
    n = len(returns)
    ann_ret = (1 + total) ** (252 / n) - 1
    ann_vol = np.std(returns) * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
    
    cum = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak) / peak
    max_dd = np.min(dd)
    calmar = ann_ret / abs(max_dd) if max_dd != 0 else 0
    win_rate = np.sum(returns > 0) / len(returns)
    
    avg_ic = np.mean(ic_values) if ic_values else 0
    ic_ir = np.mean(ic_values) / np.std(ic_values) if len(ic_values) > 1 else 0
    
    return {
        'factor': factor_name,
        'ann_return': ann_ret,
        'ann_vol': ann_vol,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'calmar': calmar,
        'win_rate': win_rate,
        'avg_ic': avg_ic,
        'ic_ir': ic_ir,
        'total_return': total,
        'n_days': n,
        'n_stocks_tested': len(cross) if 'cross' in dir() else 0,
    }


def backtest_all_factors(panel):
    """批量回测所有因子"""
    # 获取因子列名（排除close和ret_fwd）
    sample_panel = list(panel.values())[0]
    factor_names = [c for c in sample_panel.columns if c not in ['close', 'ret_fwd']]
    
    logger.info(f"回测 {len(factor_names)} 个因子...")
    results = []
    
    for i, fname in enumerate(factor_names):
        try:
            r = backtest_single_factor(panel, fname)
            if r:
                results.append(r)
                status = f"✓ 夏普={r['sharpe']:.2f}"
            else:
                status = "✗ 无结果"
        except Exception as e:
            status = f"✗ {str(e)[:40]}"
        
        if (i + 1) % 10 == 0 or i == 0:
            logger.info(f"  [{i+1}/{len(factor_names)}] {fname} {status}")
    
    return pd.DataFrame(results), factor_names


# ============================================================================
# 第四部分：多因子组合搜索
# ============================================================================
def backtest_combination(panel, factor_weights, top_pct=0.1):
    """测试因子组合"""
    total_w = sum(factor_weights.values())
    if total_w == 0:
        return None
    
    daily_rets = []
    dates = sorted(panel.keys())
    
    for i in range(len(dates) - 1):
        today = dates[i]
        cross = panel[today].copy()
        
        # 检查因子是否存在
        missing = [f for f in factor_weights if f not in cross.columns]
        if missing:
            continue
        
        cross = cross.dropna(subset=list(factor_weights.keys()) + ['ret_fwd'])
        if len(cross) < 30:
            continue
        
        # 综合得分
        score = sum(cross[f] * w for f, w in factor_weights.items()) / total_w
        cross['score'] = score
        
        n_select = max(5, int(len(cross) * top_pct))
        top = cross.nlargest(n_select, 'score')
        port_ret = top['ret_fwd'].mean()
        daily_rets.append(port_ret)
    
    if len(daily_rets) < 20:
        return None
    
    returns = np.array(daily_rets)
    total = (1 + returns).prod() - 1
    n = len(returns)
    ann_ret = (1 + total) ** (252 / n) - 1
    ann_vol = np.std(returns) * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
    
    cum = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak) / peak
    max_dd = np.min(dd)
    calmar = ann_ret / abs(max_dd) if max_dd != 0 else 0
    win_rate = np.sum(returns > 0) / len(returns)
    
    return {
        'ann_return': ann_ret,
        'ann_vol': ann_vol,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'calmar': calmar,
        'win_rate': win_rate,
        'total_return': total,
        'n_days': n,
        'name': '',
        'weights': dict(factor_weights),
    }


def search_best_combo(panel, top_factors_df, n_random=300, n_fine_tune=50):
    """搜索最优因子组合"""
    logger.info(f"组合搜索：候选 {len(top_factors_df)} 个因子，随机 {n_random} + 微调 {n_fine_tune}")
    
    best = None
    best_score = -999
    all_results = []
    
    # 策略1：IC加权
    weights = {}
    for _, row in top_factors_df.iterrows():
        w = max(abs(row['avg_ic']), 0.01)
        weights[row['factor']] = w
    
    r = backtest_combination(panel, weights)
    if r:
        r['name'] = 'IC加权'
        all_results.append(r)
        score = r['sharpe'] * 0.3 + r['calmar'] * 0.4 + r['win_rate'] * 0.3
        if score > best_score:
            best_score = score
            best = r
        logger.info(f"  IC加权: 年化={r['ann_return']:.1%} 夏普={r['sharpe']:.2f} 回撤={r['max_drawdown']:.1%}")
    
    # 策略2：夏普加权
    weights2 = {}
    for _, row in top_factors_df.iterrows():
        w = max(row['sharpe'], 0.1)
        weights2[row['factor']] = w
    
    r2 = backtest_combination(panel, weights2)
    if r2:
        r2['name'] = '夏普加权'
        all_results.append(r2)
        score = r2['sharpe'] * 0.3 + r2['calmar'] * 0.4 + r2['win_rate'] * 0.3
        if score > best_score:
            best_score = score
            best = r2
        logger.info(f"  夏普加权: 年化={r2['ann_return']:.1%} 夏普={r2['sharpe']:.2f} 回撤={r2['max_drawdown']:.1%}")
    
    # 策略3：随机组合
    factor_list = top_factors_df['factor'].tolist()
    for i in range(n_random):
        n_f = np.random.randint(3, min(8, len(factor_list) + 1))
        selected = np.random.choice(factor_list, n_f, replace=False)
        w = {f: np.random.uniform(0.5, 3.0) for f in selected}
        
        r = backtest_combination(panel, w)
        if r:
            r['name'] = f'随机_{i+1}'
            all_results.append(r)
            score = r['sharpe'] * 0.3 + r['calmar'] * 0.4 + r['win_rate'] * 0.3
            if score > best_score:
                best_score = score
                best = r
        
        if (i + 1) % 100 == 0:
            logger.info(f"  随机搜索进度: {i+1}/{n_random}, 当前最优夏普={best['sharpe']:.2f}")
    
    # 策略4：从最优组合微调
    if best and best.get('weights'):
        base_w = best['weights']
        for _ in range(n_fine_tune):
            new_w = {}
            for f, w in base_w.items():
                new_w[f] = max(0.1, w * np.random.uniform(0.6, 1.4))
            # 随机加减一个因子
            if np.random.random() < 0.3 and len(factor_list) > len(base_w):
                extra = [f for f in factor_list if f not in base_w]
                if extra:
                    new_w[np.random.choice(extra)] = np.random.uniform(0.3, 2.0)
            
            r = backtest_combination(panel, new_w)
            if r:
                r['name'] = '微调'
                all_results.append(r)
                score = r['sharpe'] * 0.3 + r['calmar'] * 0.4 + r['win_rate'] * 0.3
                if score > best_score:
                    best_score = score
                    best = r
    
    logger.info(f"组合搜索完成，共 {len(all_results)} 种组合")
    return best, pd.DataFrame([
        {k: v for k, v in r.items() if k != 'weights'} for r in all_results
    ])


# ============================================================================
# 第五部分：可视化
# ============================================================================
def generate_report(single_df, best_combo, combo_df, output_path):
    """生成可视化报告"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'PingFang SC']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig = plt.figure(figsize=(28, 22))
    gs = gridspec.GridSpec(4, 3, hspace=0.35, wspace=0.3)
    fig.suptitle('🎯 A股因子猎人 — Factor Hunter 报告', fontsize=18, fontweight='bold', y=0.98)
    
    # 1. 单因子夏普 Top20
    ax1 = fig.add_subplot(gs[0, 0])
    top20 = single_df.nlargest(20, 'sharpe')
    colors = ['#e74c3c' if v > 0 else '#27ae60' for v in top20['sharpe']]
    ax1.barh(range(20), top20['sharpe'], color=colors)
    ax1.set_yticks(range(20))
    ax1.set_yticklabels(top20['factor'], fontsize=7)
    ax1.set_xlabel('Sharpe Ratio')
    ax1.set_title('单因子夏普比率 Top20', fontweight='bold')
    ax1.axvline(0, color='gray', ls='--', alpha=0.5)
    ax1.invert_yaxis()
    
    # 2. IC_IR Top20
    ax2 = fig.add_subplot(gs[0, 1])
    top_ic = single_df.reindex(single_df['ic_ir'].abs().nlargest(20).index).head(20)
    colors2 = ['#e74c3c' if v > 0 else '#27ae60' for v in top_ic['ic_ir']]
    ax2.barh(range(20), top_ic['ic_ir'], color=colors2)
    ax2.set_yticks(range(20))
    ax2.set_yticklabels(top_ic['factor'], fontsize=7)
    ax2.set_xlabel('IC_IR')
    ax2.set_title('单因子IC信息比率 Top20（按绝对值）', fontweight='bold')
    ax2.axvline(0, color='gray', ls='--', alpha=0.5)
    ax2.invert_yaxis()
    
    # 3. 最大回撤最优 Top20
    ax3 = fig.add_subplot(gs[0, 2])
    top_dd = single_df.nsmallest(20, 'max_drawdown')
    ax3.barh(range(20), top_dd['max_drawdown'] * 100, color='#3498db')
    ax3.set_yticks(range(20))
    ax3.set_yticklabels(top_dd['factor'], fontsize=7)
    ax3.set_xlabel('最大回撤 (%)')
    ax3.set_title('最大回撤最优 Top20', fontweight='bold')
    ax3.invert_yaxis()
    
    # 4. 收益-风险散点
    ax4 = fig.add_subplot(gs[1, 0])
    sc = ax4.scatter(single_df['ann_vol'] * 100, single_df['ann_return'] * 100,
                     c=single_df['sharpe'], cmap='RdYlGn', s=60, alpha=0.8, edgecolors='gray')
    plt.colorbar(sc, ax=ax4, label='Sharpe')
    ax4.set_xlabel('年化波动率 (%)')
    ax4.set_ylabel('年化收益率 (%)')
    ax4.set_title('因子风险-收益分布', fontweight='bold')
    ax4.axhline(0, color='gray', ls='--', alpha=0.5)
    
    # 5. 组合搜索散点
    ax5 = fig.add_subplot(gs[1, 1])
    if len(combo_df) > 0:
        valid = combo_df.dropna(subset=['sharpe', 'max_drawdown'])
        sc2 = ax5.scatter(valid['max_drawdown'] * 100, valid['ann_return'] * 100,
                          c=valid['sharpe'], cmap='RdYlGn', s=30, alpha=0.5, edgecolors='gray')
        plt.colorbar(sc2, ax=ax5, label='Sharpe')
        if best_combo:
            ax5.scatter([best_combo['max_drawdown'] * 100], [best_combo['ann_return'] * 100],
                        c='gold', s=300, marker='*', zorder=5, edgecolors='red', linewidths=2)
            ax5.annotate('BEST', (best_combo['max_drawdown'] * 100, best_combo['ann_return'] * 100),
                        fontsize=12, fontweight='bold', color='red',
                        xytext=(10, 10), textcoords='offset points')
    ax5.set_xlabel('最大回撤 (%)')
    ax5.set_ylabel('年化收益率 (%)')
    ax5.set_title('因子组合搜索（350+种组合）', fontweight='bold')
    
    # 6. 胜率分布
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.hist(single_df['win_rate'] * 100, bins=20, color='#9b59b6', alpha=0.7, edgecolor='white')
    ax6.axvline(50, color='red', ls='--', alpha=0.5, label='50%基准')
    ax6.set_xlabel('日胜率 (%)')
    ax6.set_ylabel('因子数')
    ax6.set_title('因子胜率分布', fontweight='bold')
    ax6.legend()
    
    # 7. 最优组合权重
    ax7 = fig.add_subplot(gs[2, 0])
    if best_combo and best_combo.get('weights'):
        w = best_combo['weights']
        sorted_w = sorted(w.items(), key=lambda x: x[1], reverse=True)
        names = [x[0] for x in sorted_w]
        vals = [x[1] for x in sorted_w]
        ax7.barh(range(len(names)), vals, color='#1abc9c')
        ax7.set_yticks(range(len(names)))
        ax7.set_yticklabels(names, fontsize=7)
        ax7.set_xlabel('权重')
        ax7.set_title('最优组合因子权重', fontweight='bold')
        ax7.invert_yaxis()
    
    # 8. 综合评分
    ax8 = fig.add_subplot(gs[2, 1])
    single_df['composite'] = (single_df['sharpe'].rank(pct=True) * 0.3 +
                              single_df['calmar'].rank(pct=True) * 0.3 +
                              single_df['win_rate'].rank(pct=True) * 0.2 +
                              single_df['ic_ir'].abs().rank(pct=True) * 0.2)
    top_comp = single_df.nlargest(15, 'composite')
    cmap = plt.cm.RdYlGn(np.linspace(0.3, 0.9, 15))
    ax8.barh(range(15), top_comp['composite'], color=cmap)
    ax8.set_yticks(range(15))
    ax8.set_yticklabels(top_comp['factor'], fontsize=7)
    ax8.set_title('因子综合评分 Top15', fontweight='bold')
    ax8.invert_yaxis()
    
    # 9. Calmar Top15
    ax9 = fig.add_subplot(gs[2, 2])
    top_cal = single_df.nlargest(15, 'calmar')
    ax9.barh(range(15), top_cal['calmar'], color='#e67e22')
    ax9.set_yticks(range(15))
    ax9.set_yticklabels(top_cal['factor'], fontsize=7)
    ax9.set_title('Calmar比率 (收益/回撤) Top15', fontweight='bold')
    ax9.invert_yaxis()
    
    # 10. 摘要信息
    ax10 = fig.add_subplot(gs[3, :])
    ax10.axis('off')
    
    txt = "=" * 80 + "\n"
    txt += "                     🏆 搜索结果摘要\n"
    txt += "=" * 80 + "\n\n"
    
    if best_combo:
        txt += f"  最优组合: {best_combo.get('name', 'N/A')}\n"
        txt += f"  年化收益率:  {best_combo['ann_return']:>10.2%}\n"
        txt += f"  夏普比率:    {best_combo['sharpe']:>10.2f}\n"
        txt += f"  最大回撤:    {best_combo['max_drawdown']:>10.2%}\n"
        txt += f"  Calmar比率:  {best_combo['calmar']:>10.2f}\n"
        txt += f"  日胜率:      {best_combo['win_rate']:>10.2%}\n"
        txt += f"  因子数量:    {len(best_combo.get('weights', {})):>10d}\n"
        txt += f"  因子权重:    {best_combo.get('weights', {})}\n\n"
    
    txt += f"  单因子统计: 测试 {len(single_df)} 个因子\n"
    txt += f"  正收益因子: {len(single_df[single_df['ann_return'] > 0])} 个\n"
    txt += f"  正夏普因子: {len(single_df[single_df['sharpe'] > 0])} 个\n"
    if len(combo_df) > 0:
        txt += f"  组合搜索数: {len(combo_df)} 种\n"
    
    txt += f"\n  回测参数: 90天窗口 | 前10%选股 | 日频调仓\n"
    txt += f"  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    txt += "\n" + "=" * 80
    
    ax10.text(0.05, 0.95, txt, transform=ax10.transAxes, fontsize=11,
             va='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='#f0f4f8', alpha=0.9))
    
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"报告已保存: {output_path}")


# ============================================================================
# 主流程
# ============================================================================
def main():
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("  🎯 A股因子猎人 v2 — 启动")
    logger.info("=" * 60)
    
    # Step 1: 加载数据
    logger.info("\n[1/5] 加载数据...")
    all_data = load_all_stocks(min_days=200)
    if len(all_data) < 100:
        logger.error("股票数据不足")
        return
    
    # Step 2: 计算因子面板
    logger.info("\n[2/5] 计算横截面因子面板...")
    panel, trading_dates = compute_factor_panel(all_data, lookback_days=90)
    if len(panel) < 30:
        logger.error("交易日数据不足")
        return
    
    # Step 3: 单因子回测
    logger.info("\n[3/5] 单因子回测...")
    single_df, factor_names = backtest_all_factors(panel)
    if len(single_df) == 0:
        logger.error("无因子回测结果")
        return
    
    single_df.to_csv(os.path.join(OUTPUT_DIR, "single_factor_results.csv"), 
                     index=False, encoding='utf-8-sig')
    
    logger.info("\n--- 单因子 Top10（夏普）---")
    for _, r in single_df.nlargest(10, 'sharpe').iterrows():
        logger.info(f"  {r['factor']:25s} 年化={r['ann_return']:8.1%} 夏普={r['sharpe']:6.2f} "
                    f"回撤={r['max_drawdown']:7.1%} IC={r['avg_ic']:.4f} 胜率={r['win_rate']:.1%}")
    
    # Step 4: 组合搜索
    logger.info("\n[4/5] 多因子组合搜索...")
    top_factors = single_df.nlargest(15, 'sharpe')
    best_combo, combo_df = search_best_combo(panel, top_factors, n_random=300, n_fine_tune=50)
    
    if best_combo:
        logger.info(f"\n🏆 最优组合: {best_combo['name']}")
        logger.info(f"   年化={best_combo['ann_return']:.2%} 夏普={best_combo['sharpe']:.2f} "
                    f"回撤={best_combo['max_drawdown']:.2%} Calmar={best_combo['calmar']:.2f}")
    
    # 保存结果
    if len(combo_df) > 0:
        combo_df.to_csv(os.path.join(OUTPUT_DIR, "combo_results.csv"), 
                       index=False, encoding='utf-8-sig')
    
    if best_combo:
        save = {k: v for k, v in best_combo.items() if k != 'weights'}
        save['weights'] = best_combo.get('weights', {})
        save['timestamp'] = datetime.now().isoformat()
        with open(os.path.join(OUTPUT_DIR, "best_combo.json"), 'w', encoding='utf-8') as f:
            json.dump(save, f, ensure_ascii=False, indent=2, default=str)
    
    # Step 5: 生成报告
    logger.info("\n[5/5] 生成报告...")
    report_path = os.path.join(OUTPUT_DIR, "factor_hunter_report.png")
    generate_report(single_df, best_combo, combo_df, report_path)
    
    # 更新heartbeat
    elapsed = (datetime.now() - start).total_seconds()
    heartbeat = {
        'status': 'completed',
        'last_run': datetime.now().isoformat(),
        'elapsed_seconds': elapsed,
        'n_factors': len(single_df),
        'n_combos': len(combo_df) if len(combo_df) > 0 else 0,
        'best_sharpe': float(best_combo['sharpe']) if best_combo else None,
        'best_ann_return': float(best_combo['ann_return']) if best_combo else None,
        'best_max_drawdown': float(best_combo['max_drawdown']) if best_combo else None,
        'best_weights': best_combo.get('weights', {}) if best_combo else {},
    }
    with open(os.path.join(WORKSPACE, "heartbeat.json"), 'w') as f:
        json.dump(heartbeat, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n✅ 完成！耗时 {elapsed:.0f}s")
    logger.info(f"报告: {report_path}")


if __name__ == '__main__':
    main()
