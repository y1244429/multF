#!/usr/bin/env python3
"""
v9公式选股器 · 实盘版
数据源：/Users/ydy/CodeBuddy/output/market_data/*.csv
输出：选股报告图 + CSV
"""

import os, glob
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = '/Users/ydy/CodeBuddy/output/market_data'
OUT_DIR  = '/Users/ydy/WorkBuddy/20260326134350/factor_hunter/output'
os.makedirs(OUT_DIR, exist_ok=True)

# ──────────────────────────────────────────
# Step 1: 读取所有股票最新N日数据
# ──────────────────────────────────────────
def load_stocks(n_days=65):
    files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))
    print(f'📂 发现 {len(files)} 个文件，开始读取...')
    rows = []
    for f in files:
        try:
            fname = os.path.basename(f).replace('.csv', '')   # e.g. 600519
            df = pd.read_csv(f, parse_dates=['date'])
            df = df.sort_values('date').tail(n_days).reset_index(drop=True)
            if len(df) < 30:
                continue
            code_full = df['code'].iloc[-1]   # e.g. sh.600519
            close = df['close'].values
            vol   = df['volume'].values
            amt   = df['amount'].values
            op    = df['open'].values
            hi    = df['high'].values
            lo    = df['low'].values
            n = len(close)

            # ── 指标计算 ──
            ma5   = np.mean(close[-5:])   if n >= 5  else close[-1]
            ma20  = np.mean(close[-20:])  if n >= 20 else close[-1]
            ma60  = np.mean(close[-60:])  if n >= 60 else close[-1]

            vol_ma5  = vol[-1]  / np.mean(vol[-5:])  if n >= 5  else 1.0
            vol_ma20 = vol[-1]  / np.mean(vol[-20:]) if n >= 20 else 1.0
            amt_ma20 = amt[-1]  / np.mean(amt[-20:]) if n >= 20 else 1.0

            # 隔夜收益
            overnight = (op[-1] / close[-2] - 1) * 100  if n >= 2  else 0.0

            # 收益
            ret1  = close[-1] / close[-2] - 1   if n >= 2  else 0.0
            ret5  = close[-1] / close[-6] - 1   if n >= 6  else 0.0
            ret20 = close[-1] / close[-21] - 1  if n >= 21 else 0.0

            # ATR14
            tr_list = []
            for k in range(1, min(15, n)):
                h_l = hi[-k] - lo[-k]
                h_c = abs(hi[-k] - close[-k-1])
                l_c = abs(lo[-k] - close[-k-1])
                tr_list.append(max(h_l, h_c, l_c))
            atr14 = np.mean(tr_list) if tr_list else 0.0

            # 波动调整收益
            vol_adj_ret = ret5 / (atr14 / close[-1] * 100 + 0.1)

            # 风险动量
            ret5_std = np.std(close[-6:] / close[-7:-1] - 1) if n >= 7 else 0.01
            risk_adj_mom = ret5 / (ret5_std + 0.0001)

            # BIAS5
            bias5 = (close[-1] / ma5 - 1) * 100

            # EOM
            eom = (close[-1] - op[-1]) / (atr14 + 0.01) * 10

            # 阳线
            bullish = 1 if close[-1] > op[-1] else 0

            # 20日最低
            llv20 = np.min(lo[-20:]) if n >= 20 else lo[-1]

            # 价偏离60日
            price_dev60 = (close[-1] / ma60 - 1) * 100

            # 均线多头
            ma_cross = 1 if (n >= 10 and
                             np.mean(close[-5:]) > np.mean(close[-10:]) * 1.002) else 0

            # 缩量 & 低换手代理
            shrivel = 1 if vol_ma20 < 0.85 else 0
            low_turn = 1 if vol_ma20 < 1.0 else 0
            amt_stable = 1 if 0.7 < amt_ma20 < 1.3 else 0
            amt_above = 1 if amt[-1] > np.mean(amt[-20:]) else 0

            rows.append({
                'code':      fname,           # 纯数字
                'code_full': code_full,       # sh./sz.前缀
                'close':     close[-1],
                'overnight': overnight,
                'ret1':      ret1,
                'ret5':      ret5,
                'ret20':     ret20,
                'vol_ma5':   vol_ma5,
                'vol_ma20':  vol_ma20,
                'amt_ma20':  amt_ma20,
                'ma5_ratio': close[-1] / ma5,
                'ma20_ratio': close[-1] / ma20,
                'price_dev60': price_dev60,
                'vol_adj_ret': vol_adj_ret,
                'risk_adj_mom': risk_adj_mom,
                'bias5':     bias5,
                'eom':       eom,
                'bullish':   bullish,
                'llv20':     llv20,
                'low_turn':  low_turn,
                'shrivel':   shrivel,
                'amt_stable': amt_stable,
                'ma_cross':   ma_cross,
                'amt_above':  amt_above,
            })
        except Exception as e:
            continue

    print(f'✅ 成功处理 {len(rows)} 只股票')
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# Step 2: 应用v9公式
# ──────────────────────────────────────────
def screen(df):
    out = {}

    # ── 公式A 严格：隔夜>0 + 低量 + ma5<1.05 + 阳线 ──
    m = (df['overnight'] > 0) & (df['vol_ma20'] < 1.0) & \
        (df['ma5_ratio'] < 1.05) & (df['bullish'] == 1)
    a = df[m].copy()
    a['score'] = a['overnight'] + (1 - a['vol_ma20']) * 10
    out['A_严格'] = a.sort_values('score', ascending=False).head(20)

    # 公式A 宽松
    m = (df['overnight'] > 0) & (df['vol_ma20'] < 1.2) & (df['bullish'] == 1)
    a = df[m].copy()
    a['score'] = a['overnight'] + (1 - a['vol_ma20']) * 10
    out['A_宽松'] = a.sort_values('score', ascending=False).head(20)

    # ── 公式B 严格：波动收益>0 + 缩量 + ma5>1 + 阳线 + 额>均额 ──
    m = (df['vol_adj_ret'] > 0) & (df['shrivel'] == 1) & \
        (df['ma5_ratio'] > 1.0) & (df['bullish'] == 1) & (df['amt_above'] == 1)
    b = df[m].copy()
    b['score'] = b['vol_adj_ret'] + (1 - b['vol_ma20']) * 5
    out['B_严格'] = b.sort_values('score', ascending=False).head(20)

    # 公式B 宽松
    m = (df['vol_adj_ret'] > 0) & (df['vol_ma20'] < 1.0) & (df['bullish'] == 1)
    b = df[m].copy()
    b['score'] = b['vol_adj_ret'] + (1 - b['vol_ma20']) * 5
    out['B_宽松'] = b.sort_values('score', ascending=False).head(20)

    # ── 公式C 严格：风险动量∈(-2,8) + bias5<-3 + 阳线 + 涨超llv20*1.02 + 额>均额 ──
    m = (df['risk_adj_mom'] > -2) & (df['risk_adj_mom'] < 8) & \
        (df['bias5'] < -3) & (df['bullish'] == 1) & \
        (df['close'] > df['llv20'] * 1.02) & (df['amt_above'] == 1)
    c = df[m].copy()
    c['score'] = -c['bias5'] + c['risk_adj_mom']
    out['C_严格'] = c.sort_values('score', ascending=False).head(20)

    # 公式C 宽松
    m = (df['bias5'] < -4) & (df['bullish'] == 1) & (df['close'] > df['llv20'] * 1.02)
    c = df[m].copy()
    c['score'] = -c['bias5']
    out['C_宽松'] = c.sort_values('score', ascending=False).head(20)

    # ── 公式D 严格：价偏离(-15%,5%) + vol_ma20<1.1 + 额稳 + 阳线 + ret1>0.5% ──
    m = (df['price_dev60'] < 5) & (df['price_dev60'] > -15) & \
        (df['vol_ma20'] < 1.1) & (df['amt_stable'] == 1) & \
        (df['bullish'] == 1) & (df['ret1'] > 0.005)
    d = df[m].copy()
    d['score'] = -d['price_dev60'] + (1 - d['vol_ma20']) * 10
    out['D_严格'] = d.sort_values('score', ascending=False).head(20)

    # 公式D 宽松
    m = (df['price_dev60'] < 5) & (df['price_dev60'] > -10) & (df['bullish'] == 1)
    d = df[m].copy()
    d['score'] = -d['price_dev60']
    out['D_宽松'] = d.sort_values('score', ascending=False).head(20)

    # ── 公式E 严格：EOM>0 + ma5>1 + 均线多头 + 阳线 + 额>均额 ──
    m = (df['eom'] > 0) & (df['ma5_ratio'] > 1.0) & \
        (df['ma_cross'] == 1) & (df['bullish'] == 1) & (df['amt_above'] == 1)
    e = df[m].copy()
    e['score'] = e['eom'] + (e['ma5_ratio'] - 1) * 20
    out['E_严格'] = e.sort_values('score', ascending=False).head(20)

    # 公式E 宽松
    m = (df['eom'] > 0) & (df['ma5_ratio'] > 1.0) & (df['bullish'] == 1)
    e = df[m].copy()
    e['score'] = e['eom']
    out['E_宽松'] = e.sort_values('score', ascending=False).head(20)

    return out

# ──────────────────────────────────────────
# Step 3: 生成图表报告
# ──────────────────────────────────────────
def plot_report(screened, df_all):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams['font.family'] = ['Arial Unicode MS', 'WenQuanYi Micro Hei', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    formulas = [
        ('A_严格', '公式A 隔夜强势·低换手\n【严格版】',  '#1565c0', '⭐⭐⭐⭐⭐'),
        ('A_宽松', '公式A 隔夜强势·低换手\n【宽松版】',  '#42a5f5', '⭐⭐⭐⭐⭐'),
        ('B_严格', '公式B 波动调整·缩量确认\n【严格版】', '#e65100', '⭐⭐⭐⭐⭐'),
        ('B_宽松', '公式B 波动调整·缩量确认\n【宽松版】', '#ff8a65', '⭐⭐⭐⭐⭐'),
        ('C_严格', '公式C 风险动量·超跌反弹\n【严格版】', '#2e7d32', '⭐⭐⭐⭐'),
        ('C_宽松', '公式C 风险动量·超跌反弹\n【宽松版】', '#66bb6a', '⭐⭐⭐⭐'),
        ('D_严格', '公式D 量价协同低位\n【严格版】',    '#6a1b9a', '⭐⭐⭐⭐'),
        ('D_宽松', '公式D 量价协同低位\n【宽松版】',    '#ab47bc', '⭐⭐⭐⭐'),
        ('E_严格', '公式E 动向指数追涨\n【严格版】',     '#c62828', '⭐⭐⭐'),
        ('E_宽松', '公式E 动向指数追涨\n【宽松版】',     '#ef5350', '⭐⭐⭐'),
    ]

    fig, axes = plt.subplots(2, 5, figsize=(26, 11))
    axes = axes.flatten()

    for idx, (key, title, color, stars) in enumerate(formulas):
        ax = axes[idx]
        sub = screened.get(key, pd.DataFrame())
        if sub.empty:
            ax.text(0.5, 0.5, '暂无候选', ha='center', va='center',
                    fontsize=11, color='gray')
            ax.set_title(f'{stars}\n{title}(0只)', fontsize=9, color='gray')
            ax.set_xticks([]); ax.set_yticks([])
            continue

        top = sub.head(10)
        labels = top['code'].tolist()
        scores = top['score'].tolist()

        bars = ax.barh(range(len(labels)), scores, color=color, alpha=0.85, edgecolor='white')
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('综合评分', fontsize=7)
        ax.set_title(f'{stars}\n{title}({len(sub)}只)', fontsize=8, color=color, fontweight='bold')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for bar, sc in zip(bars, scores):
            ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                    f'{sc:.2f}', va='center', fontsize=7, color=color)

    fig.text(0.5, 0.01,
             f'v9公式选股报告 · {pd.Timestamp.now().strftime("%Y-%m-%d")} · '
             f'扫描{df_all}只股票 · 数据来源：本地K线CSV（2023-01至今）',
             ha='center', fontsize=10, color='gray')
    plt.tight_layout(rect=[0, 0.03, 1, 1])
    out = os.path.join(OUT_DIR, 'v9_stock_screener_report.png')
    plt.savefig(out, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'\n📊 报告图: {out}')
    return out

# ──────────────────────────────────────────
# Step 4: 保存CSV
# ──────────────────────────────────────────
def save_csv(screened):
    rows = []
    for key, sub in screened.items():
        if sub.empty:
            continue
        tmp = sub.head(10).copy()
        tmp.insert(0, '公式', key)
        rows.append(tmp)
    if rows:
        df_out = pd.concat(rows, ignore_index=True)
        cols_keep = ['公式','code','close','overnight','vol_ma20','vol_adj_ret',
                     'risk_adj_mom','bias5','price_dev60','eom','bullish','score']
        cols_keep = [c for c in cols_keep if c in df_out.columns]
        df_out = df_out[cols_keep]
        out = os.path.join(OUT_DIR, 'v9_selected_stocks.csv')
        df_out.to_csv(out, index=False, encoding='utf-8-sig')
        print(f'📄 CSV: {out}')
        return out
    return None

# ──────────────────────────────────────────
# 主程序
# ──────────────────────────────────────────
def main():
    print('='*60)
    print('🔍 v9公式选股器 · 实盘版')
    print('='*60)

    df = load_stocks()
    print(f'\n🔢 指标计算完成，共 {len(df)} 只股票\n')

    print('📊 应用选股公式...')
    res = screen(df)

    # 汇总打印
    summary = {
        'A_严格': '【公式A 隔夜强势·低换手】严格',
        'A_宽松': '【公式A 隔夜强势·低换手】宽松',
        'B_严格': '【公式B 波动调整·缩量】严格',
        'B_宽松': '【公式B 波动调整·缩量】宽松',
        'C_严格': '【公式C 风险动量·超跌】严格',
        'C_宽松': '【公式C 风险动量·超跌】宽松',
        'D_严格': '【公式D 量价协同低位】严格',
        'D_宽松': '【公式D 量价协同低位】宽松',
        'E_严格': '【公式E 动向指数追涨】严格',
        'E_宽松': '【公式E 动向指数追涨】宽松',
    }

    print('\n' + '='*60)
    print('📋 选股结果')
    print('='*60)
    for key, name in summary.items():
        sub = res.get(key, pd.DataFrame())
        if sub.empty:
            print(f'\n  {name}: ⚠️ 暂无候选')
        else:
            tops = ', '.join(sub.head(5)['code'].tolist())
            print(f'\n  {name}: 共{len(sub)}只  Top5: {tops}')

    print('\n📊 生成可视化报告...')
    plot_report(res, len(df))
    save_csv(res)

    print('\n✅ 完成！')

if __name__ == '__main__':
    main()
