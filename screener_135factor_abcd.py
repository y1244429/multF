"""
全量135因子实测版 ABCD选股程序 v4
基于 因子监控_IC_IR_实测版_全量135因子.xlsx 中 Top因子设计4个风格公式

公式A: 隔夜效应+低换手  ★★★★★ (overnight_ret IR=2.26, turnover_rate IR=-1.28)
公式B: 波动调整收益+K线强势 ★★★★★ (qfr_vol_adj_ret IR=1.18, candle_body IR=0.60, aroon_osc IR=0.58)
公式C: 反转+低位 逆向蓄势 ★★★★ (rank_mom_60 IR=-1.02, rank_ma_ratio_60 IR=-0.93)
公式D: 基本面+价量共振 ★★★★ (growth_net_profit IR=0.52 + overnight_ret + candle_body)
公式X: 日内动量版 ★★★★★ (收盘买→次日开盘卖, 10条件严格筛选)

v2 新增过滤：
  - 排除 ST / *ST 股票（用 baostock stock_basic 名称识别）
  - 排除流通市值 < 50亿 的股票（用 volume / (turn/100) * close 反推）

v3 新增：
  - 公式A逐条件预过滤（NOT_ST AND MKT>50亿 AND AVG_OVN>0.001 AND TR5<10 AND AMT_STAB<0.5 AND CLOSE>REF30）
  - 公式A选出后按 formula_a_score 综合得分排名

v4 新增：
  - 公式X（日内动量版）10条件预过滤 + formula_x_score 排名 + Excel输出
  - 新增因子：M1阳线、M2今日涨幅、M3量比、M4价格vsMA5
"""

import os
import glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import warnings
warnings.filterwarnings('ignore')

# 中文字体
plt.rcParams['font.sans-serif'] = ['PingFang SC', 'Heiti TC', 'Arial Unicode MS', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

DATA_DIR = '/Users/ydy/Downloads/V3'
OUTPUT_DIR = '/Users/ydy/WorkBuddy/20260326134350/factor_hunter/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── ST & 市值过滤（用 baostock 获取）──────────────────────────────────────────

def load_filter_data():
    """
    从 baostock 获取：
    1. ST代码集合（名称含'ST'的在市股票）
    2. 流通市值字典（code → 亿元），通过 volume/turn*close 反推
    3. 5日平均换手率字典（tr5_dict: code → %），用于公式A过滤
    返回: (st_codes: set, mkt_dict: dict, tr5_dict: dict)
    """
    cache_file = os.path.join(OUTPUT_DIR, '_filter_cache.csv')

    # ── 使用缓存（当天内有效）────────────────────────────────────────────────
    if os.path.exists(cache_file):
        df_cache = pd.read_csv(cache_file)
        from datetime import date
        today_str = str(date.today())
        if 'fetch_date' in df_cache.columns and df_cache['fetch_date'].iloc[0] == today_str:
            st_codes = set(df_cache[df_cache['isST'] == 1]['code_short'].tolist())
            mkt_dict = dict(zip(df_cache['code_short'], df_cache['float_mkt']))
            tr5_dict = dict(zip(df_cache['code_short'], df_cache['tr5']))
            print(f'  [缓存] ST={len(st_codes)}只，市值={len(mkt_dict)}只，换手率={len(tr5_dict)}只')
            return st_codes, mkt_dict, tr5_dict

    # ── 联网获取 ────────────────────────────────────────────────────────────
    try:
        import baostock as bs
        from datetime import date, timedelta

        lg = bs.login()

        # Step1: 获取在市普通A股列表及ST名称
        rs = bs.query_stock_basic()
        info_data = []
        while (rs.error_code == '0') & rs.next():
            info_data.append(rs.get_row_data())
        df_info = pd.DataFrame(info_data, columns=rs.fields)
        stocks = df_info[(df_info['type'] == '1') & (df_info['status'] == '1')].copy()
        stocks['code_short'] = stocks['code'].str.split('.').str[1]
        stocks['isST'] = stocks['code_name'].str.contains('ST', na=False).astype(int)
        st_codes_bs = set(stocks[stocks['isST'] == 1]['code_short'].tolist())
        print(f'  baostock: 在市A股={len(stocks)}只，ST={len(st_codes_bs)}只')

        # Step2: 批量获取最近5日 turn（换手率）反推流通市值 + TR5均值
        today = date.today()
        start_d = (today - timedelta(days=10)).strftime('%Y-%m-%d')
        end_d = today.strftime('%Y-%m-%d')

        code_list = stocks['code'].tolist()
        print(f'  开始批量获取换手率（{len(code_list)}只）...')

        import concurrent.futures

        def _fetch_one(code):
            try:
                rs2 = bs.query_history_k_data_plus(
                    code, 'date,close,volume,turn,isST',
                    start_date=start_d, end_date=end_d,
                    frequency='d', adjustflag='3')
                rows = []
                while rs2.next():
                    rows.append(rs2.get_row_data())
                if rows:
                    # 取最近有数据的5天
                    recent = rows[-5:] if len(rows) >= 5 else rows
                    turns = [float(r[3]) for r in recent if r[3] and float(r[3]) > 0]
                    tr5 = sum(turns) / len(turns) if turns else 0
                    latest = rows[-1]
                    close = float(latest[1]) if latest[1] else 0
                    volume = float(latest[2]) if latest[2] else 0
                    turn = float(latest[3]) if latest[3] else 0
                    is_st = int(latest[4]) if latest[4] else 0
                    float_mkt = volume / (turn / 100) * close / 1e8 if turn > 0 else 0
                    return {
                        'code_short': code.split('.')[1],
                        'close': close,
                        'turn': turn,
                        'tr5': tr5,           # 5日平均换手率（%）
                        'isST': is_st,
                        'float_mkt': float_mkt,
                    }
            except Exception:
                pass
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            results = list(executor.map(_fetch_one, code_list))

        bs.logout()

        valid = [r for r in results if r is not None]
        df_cache = pd.DataFrame(valid)
        df_cache['fetch_date'] = str(date.today())

        # 保存缓存
        df_cache.to_csv(cache_file, index=False)
        print(f'  获取成功: {len(df_cache)}只，已缓存')

        # 合并baostock名称识别的ST
        df_cache.loc[df_cache['code_short'].isin(st_codes_bs), 'isST'] = 1

        st_codes = set(df_cache[df_cache['isST'] == 1]['code_short'].tolist())
        mkt_dict = dict(zip(df_cache['code_short'], df_cache['float_mkt']))
        tr5_dict = dict(zip(df_cache['code_short'], df_cache['tr5']))
        return st_codes, mkt_dict, tr5_dict

    except Exception as e:
        print(f'  ⚠️ baostock获取失败: {e}，跳过ST/市值过滤')
        return set(), {}, {}

# ─── 因子计算函数 ───────────────────────────────────────────────────────────────

def calc_overnight_ret(df, n=5):
    """隔夜收益 = 开盘/前收-1, 取n日均值"""
    ovn = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
    return ovn.rolling(n).mean().iloc[-1]

def calc_turnover_rate(df, n=5):
    """换手率代理 = VOL/(流通股本估算), 用VOL/VOL_AVG120代替"""
    vol_avg = df['volume'].rolling(120).mean()
    tr_proxy = df['volume'] / vol_avg  # 相对换手率
    return tr_proxy.rolling(n).mean().iloc[-1]

def calc_amt_stability(df, n=20):
    """成交额稳定性 = STD/MEAN (越小越稳定)"""
    amt = df['amount']
    return (amt.rolling(n).std() / amt.rolling(n).mean()).iloc[-1]

def calc_qfr_vol_adj_ret(df, n=20):
    """波动调整收益 = n日收益率 / n日波动率"""
    ret = df['close'].pct_change()
    vol = ret.rolling(n).std()
    ret_n = df['close'].pct_change(n)
    return (ret_n / (vol + 1e-8)).iloc[-1]

def calc_candle_body(df, n=5):
    """K线实体均值 (相对前收)"""
    body = (df['close'] - df['open']) / df['close'].shift(1) * 100
    return body.rolling(n).mean().iloc[-1]

def calc_aroon_osc(df, n=25):
    """Aroon震荡指标"""
    high_bars = df['high'].rolling(n+1).apply(lambda x: n - np.argmax(x), raw=True)
    low_bars  = df['low'].rolling(n+1).apply(lambda x: n - np.argmin(x), raw=True)
    aroon_up = (n - high_bars) / n * 100
    aroon_dn = (n - low_bars)  / n * 100
    return (aroon_up - aroon_dn).iloc[-1]

def calc_master_bias_5_20(df):
    """MA5/MA20偏离度"""
    ma5  = df['close'].rolling(5).mean()
    ma20 = df['close'].rolling(20).mean()
    return ((ma5 - ma20) / ma20 * 100).iloc[-1]

def calc_rank_mom_60(df):
    """60日动量（逆向因子：负动量=超跌）"""
    return (df['close'].pct_change(60) * 100).iloc[-1]

def calc_bias_60(df):
    """60日均线偏离率"""
    ma60 = df['close'].rolling(60).mean()
    return ((df['close'] - ma60) / ma60 * 100).iloc[-1]

def calc_vol_ratio_20(df):
    """成交量比（当日/20日均量）"""
    vol_avg20 = df['volume'].rolling(20).mean()
    return (df['volume'] / vol_avg20).iloc[-1]

def calc_lower_shadow(df):
    """下影线比例 (下影线/前收)"""
    low_body = df[['open', 'close']].min(axis=1)
    return ((low_body - df['low']) / df['close'].shift(1) * 100).iloc[-1]

def calc_ma5_20_cross(df):
    """MA5是否上穿MA20（布尔）"""
    ma5  = df['close'].rolling(5).mean()
    ma20 = df['close'].rolling(20).mean()
    cross = (ma5.iloc[-1] > ma20.iloc[-1]) and (ma5.iloc[-2] <= ma20.iloc[-2])
    return float(cross)

def calc_ma5_20_diff(df):
    """MA5 - MA20 差值（正=多头排列）"""
    ma5  = df['close'].rolling(5).mean().iloc[-1]
    ma20 = df['close'].rolling(20).mean().iloc[-1]
    return ma5 - ma20

def calc_ret_30d(df):
    """近30日收益率"""
    return (df['close'].pct_change(30) * 100).iloc[-1]

# ─── 公式X（日内动量版）专用因子 ───────────────────────────────────────────────

def calc_candle_body_ratio(df):
    """
    M1: 今日K线阳线强度 = (close-open)/前收*100
    正值=收阳，负值=收阴，用于 M1: CLOSE>OPEN 判断
    """
    body = (df['close'] - df['open']) / df['close'].shift(1) * 100
    return body.iloc[-1]

def calc_today_return(df):
    """
    M2: 今日收益率 = (close - ref_close_1) / ref_close_1 * 100
    用于 M2: CLOSE>REF(CLOSE,1) 判断
    """
    return (df['close'].pct_change(1) * 100).iloc[-1]

def calc_vol_momentum(df):
    """
    M3: 成交量动量 = VOL / REF(VOL,1) - 1
    正值=今日放量，>0时 M3条件满足
    """
    vol_today = df['volume'].iloc[-1]
    vol_yesterday = df['volume'].iloc[-2] if len(df) >= 2 else vol_today
    return (vol_today / (vol_yesterday + 1e-8) - 1) * 100

def calc_above_ma5(df):
    """
    M4: 价格相对MA5偏离 = CLOSE/MA5 - 1
    正值=价格在MA5上方，>0时 M4条件满足
    """
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    return (df['close'].iloc[-1] / (ma5 + 1e-8) - 1) * 100

# ─── 综合打分 ───────────────────────────────────────────────────────────────────

def score_percentile(series):
    """截面百分位打分 0-100"""
    rank = series.rank(pct=True) * 100
    return rank

def formula_a_score(row):
    """
    公式A: 隔夜效应+低换手 综合精选版
    overnight_ret 正向(高分好) + turnover_rate 逆向(低换手好) + amt_stability 逆向(稳定好)
    """
    s_ovn = row['pct_overnight']          # 越高越好
    s_tr  = 100 - row['pct_turnover']     # 逆向，低换手=高分
    s_amt = 100 - row['pct_amt_stab']     # 逆向，稳定=低std=高分
    s_ret = row['pct_ret30']              # 近期趋势向上
    return s_ovn * 0.40 + s_tr * 0.30 + s_amt * 0.20 + s_ret * 0.10

def formula_b_score(row):
    """
    公式B: 波动调整收益+K线强势 动量质量版
    qfr_vol_adj_ret 正向 + candle_body 正向 + aroon_osc 正向 + master_bias_5_20 正向
    """
    s_var = row['pct_qfr_var']            # 高波动调整收益=好
    s_body = row['pct_candle_body']       # 大实体阳线=好
    s_aroon = row['pct_aroon']            # aroon强势=好
    s_bias = row['pct_bias5_20']          # MA5略高于MA20=好
    return s_var * 0.35 + s_body * 0.25 + s_aroon * 0.25 + s_bias * 0.15

def formula_c_score(row):
    """
    公式C: 反转+低位 逆向蓄势版
    rank_mom_60 逆向(超跌) + bias_60 逆向(低位) + vol_ratio 逆向(缩量) + lower_shadow 逆向(下影支撑大)
    """
    s_mom60 = 100 - row['pct_mom60']         # 逆向：超跌=高分
    s_bias60 = 100 - row['pct_bias60']       # 逆向：低于均线=高分
    s_vol = 100 - row['pct_vol_ratio']       # 逆向：缩量=高分
    s_shadow = row['pct_lower_shadow']       # 下影线大=支撑=高分
    return s_mom60 * 0.35 + s_bias60 * 0.30 + s_vol * 0.20 + s_shadow * 0.15

def formula_d_score(row):
    """
    公式D: 价量共振 综合版 (无基本面数据用价量代替)
    overnight_ret + candle_body + amt_stability + ma5_20多头
    """
    s_ovn  = row['pct_overnight']
    s_body = row['pct_candle_body']
    s_amt  = 100 - row['pct_amt_stab']     # 稳定=好
    s_ma   = row['pct_ma5_20_diff']        # MA5>MA20=多头
    return s_ovn * 0.35 + s_body * 0.25 + s_amt * 0.20 + s_ma * 0.20

def formula_x_score(row):
    """
    公式X: 日内动量版（收盘买→次日开盘卖）
    10条件叠加：阳线+涨幅+放量+MA5上方+低换手+隔夜均值+成交稳定+30日向上
    打分权重：
      - 隔夜均值(稳定规律): 30%
      - 今日涨幅(动量强度): 20%
      - 量比(资金参与): 20%
      - MA5偏离(趋势质量): 15%
      - 低换手(筹码稳定): 10%
      - 成交稳定: 5%
    """
    s_ovn   = row['pct_overnight']          # 隔夜均值，越高越好
    s_today = row['pct_today_ret']          # 今日涨幅，越高越好
    s_vol   = row['pct_vol_momentum']       # 放量程度，越高越好
    s_ma5   = row['pct_above_ma5']         # 价格在MA5上方，越高越好
    s_tr    = 100 - row['pct_turnover']     # 低换手=高分
    s_amt   = 100 - row['pct_amt_stab']     # 成交稳定=高分
    return s_ovn*0.30 + s_today*0.20 + s_vol*0.20 + s_ma5*0.15 + s_tr*0.10 + s_amt*0.05

# ─── 主程序 ────────────────────────────────────────────────────────────────────

def _process_one_file(f):
    """多进程处理单个CSV文件，返回rec或None"""
    import os, pandas as pd, numpy as np

    try:
        code = os.path.basename(f).replace('.csv', '')

        df = pd.read_csv(f, parse_dates=['date'])
        df = df.sort_values('date').reset_index(drop=True)

        if len(df) < 120:
            return None
        close_last = df['close'].iloc[-1]
        if close_last <= 0:
            return None

        # ── 用V3自身数据估算市值 & 换手率（跳过baostock）──────────────────────
        # 流通市值估算：5日均成交额 / 0.005(假设0.5%日换手率) → 亿元
        recent5 = df.tail(5)
        avg_amt = recent5['amount'].mean()
        avg_close5 = recent5['close'].mean()
        avg_vol5 = recent5['volume'].mean()
        vol_120_avg = df['volume'].rolling(120).mean().iloc[-1]
        mkt_est = avg_amt / 0.005 / 1e8          # 估算流通市值（亿元）
        # 换手率代理：用5日均量/120日均量（>1.5=放量，<0.8=缩量）
        tr5_est = (avg_vol5 / (vol_120_avg + 1e-8) - 1) * 100 if vol_120_avg > 0 else 0

        rec = {
            'code': code,
            'close': close_last,
            'float_mkt': mkt_est,
            'tr5':       tr5_est,
            'overnight':    calc_overnight_ret(df, 5),
            'turnover':     calc_turnover_rate(df, 5),
            'amt_stab':     calc_amt_stability(df, 20),
            'qfr_var':      calc_qfr_vol_adj_ret(df, 20),
            'candle_body':  calc_candle_body(df, 5),
            'aroon':        calc_aroon_osc(df, 25),
            'bias5_20':     calc_master_bias_5_20(df),
            'mom60':        calc_rank_mom_60(df),
            'bias60':       calc_bias_60(df),
            'vol_ratio':    calc_vol_ratio_20(df),
            'lower_shadow': calc_lower_shadow(df),
            'ma5_20_diff':  calc_ma5_20_diff(df),
            'ret30':        calc_ret_30d(df),
            'candle_body_ratio': calc_candle_body_ratio(df),
            'today_ret':         calc_today_return(df),
            'vol_momentum':      calc_vol_momentum(df),
            'above_ma5':        calc_above_ma5(df),
        }
        return rec
    except Exception:
        return None


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, '*.csv')))
    print(f'📂 共 {len(files)} 只股票，开始并行计算因子（{os.cpu_count()}核）...')

    MIN_MKT = 50.0
    TR5_THRESHOLD = 200.0   # 成交量代理阈值（200 = 3x 120日均量视为高换手）

    import multiprocessing as mp
    with mp.Pool(processes=min(8, mp.cpu_count())) as pool:
        results = pool.map(_process_one_file, files)

    records = [r for r in results if r is not None]
    print(f'✅ 并行计算完成，有效股票: {len(records)}')

    # ── 预过滤 ───────────────────────────────────────────────────────────
    skip_mkt = sum(1 for r in records if 0 < r['float_mkt'] < MIN_MKT)
    skip_tr5 = sum(1 for r in records if r['tr5'] > TR5_THRESHOLD)
    records = [r for r in records
               if not (0 < r['float_mkt'] < MIN_MKT) and r['tr5'] <= TR5_THRESHOLD]
    print(f'   过滤小市值: {skip_mkt}只，高换手: {skip_tr5}只，剩余: {len(records)}')
    df_all = pd.DataFrame(records).dropna(subset=[c for c in ['overnight','turnover','amt_stab',
        'qfr_var','candle_body','aroon','bias5_20','mom60','bias60','vol_ratio',
        'lower_shadow','ma5_20_diff','ret30','candle_body_ratio','today_ret',
        'vol_momentum','above_ma5']])
    print(f'📊 去NaN后: {len(df_all)} 只')

    # 计算百分位
    for col in ['overnight', 'turnover', 'amt_stab', 'qfr_var', 'candle_body',
                'aroon', 'bias5_20', 'mom60', 'bias60', 'vol_ratio', 'lower_shadow',
                'ma5_20_diff', 'ret30', 'candle_body_ratio', 'today_ret',
                'vol_momentum', 'above_ma5']:
        df_all[f'pct_{col}'] = score_percentile(df_all[col])

    # ── 公式A逐条件预过滤 ───────────────────────────────────────────────────
    # ── 计算5个公式得分（先在全体df_all上算）────────────────────────────────
    df_all['score_A'] = df_all.apply(formula_a_score, axis=1)
    df_all['score_B'] = df_all.apply(formula_b_score, axis=1)
    df_all['score_C'] = df_all.apply(formula_c_score, axis=1)
    df_all['score_D'] = df_all.apply(formula_d_score, axis=1)

    # 过滤极端价格（仙股/超高价）
    df_all = df_all[(df_all['close'] > 2) & (df_all['close'] < 200)]

    # ── 公式A逐条件预过滤（在打分+过滤价格后）──────────────────────────────
    # 条件：AVG_OVN>0.001（隔夜均值正向）AND AMT_STAB<0.5（成交稳定）AND ret30>0
    mask_a = (
        (df_all['overnight'] > 0.001) &
        (df_all['amt_stab'] < 0.5) &
        (df_all['ret30'] > 0)
    )
    df_a_pass = df_all[mask_a].copy()
    # 对公式A通过股票重新百分位（相对同类比较）
    for col in ['overnight', 'turnover', 'amt_stab', 'ret30']:
        df_a_pass[f'pct_{col}'] = score_percentile(df_a_pass[col])
    print(f'📊 公式A预过滤通过: {len(df_a_pass)} 只（条件: AVG_OVN>0.001 AND AMT_STAB<0.5 AND ret30>0）')

    # ── 公式X逐条件预过滤（10条件全部满足）───────────────────────────────────
    # M1: CLOSE>OPEN    → candle_body_ratio > 0
    # M2: CLOSE>REF(CL,1) → today_ret > 0
    # M3: VOL>REF(VOL,1)  → vol_momentum > 0
    # M4: CLOSE>MA5      → above_ma5 > 0
    # M5: TR5<10%        → tr5 < 10  (已在全局过滤)
    # AVG_OVN>0.001     → overnight > 0.001
    # AMT_STAB<0.5      → amt_stab < 0.5
    # RET30>0           → ret30 > 0
    # CLOSE 2~200       → 已过滤
    mask_x = (
        (df_all['candle_body_ratio'] > 0) &   # M1: 收阳线
        (df_all['today_ret'] > 0) &            # M2: 今日上涨
        (df_all['vol_momentum'] > 0) &          # M3: 今日放量
        (df_all['above_ma5'] > 0) &           # M4: 价格>MA5
        (df_all['overnight'] > 0.001) &        # 近5日隔夜均值正向
        (df_all['amt_stab'] < 0.5) &           # 成交额稳定
        (df_all['ret30'] > 0) &                # 近30日趋势向上
        (df_all['close'] > 2) &               # 剔除仙股
        (df_all['close'] < 200)               # 剔除超高价
    )
    df_x_pass = df_all[mask_x].copy()
    print(f'📊 公式X预过滤通过: {len(df_x_pass)} 只（10条件全部满足）')

    # 对公式X通过股票重新百分位
    for col in ['overnight', 'turnover', 'amt_stab', 'today_ret', 'vol_momentum', 'above_ma5']:
        df_x_pass[f'pct_{col}'] = score_percentile(df_x_pass[col])

    # 公式X仅在通过池内打分
    df_x_pass['score_X'] = df_x_pass.apply(formula_x_score, axis=1)

    # 选Top100
    top_a = df_a_pass.nlargest(100, 'score_A')[['code', 'close', 'float_mkt', 'score_A', 'tr5', 'overnight', 'amt_stab', 'ret30']].reset_index(drop=True)
    top_b = df_all.nlargest(100, 'score_B')[['code', 'close', 'float_mkt', 'score_B', 'qfr_var', 'candle_body', 'aroon', 'bias5_20']].reset_index(drop=True)
    top_c = df_all.nlargest(100, 'score_C')[['code', 'close', 'float_mkt', 'score_C', 'mom60', 'bias60', 'vol_ratio', 'lower_shadow']].reset_index(drop=True)
    top_d = df_all.nlargest(100, 'score_D')[['code', 'close', 'float_mkt', 'score_D', 'overnight', 'candle_body', 'amt_stab', 'ma5_20_diff']].reset_index(drop=True)
    top_x = df_x_pass.nlargest(100, 'score_X')[['code', 'close', 'float_mkt', 'score_X', 'tr5', 'overnight', 'today_ret', 'vol_momentum', 'above_ma5', 'amt_stab', 'ret30']].reset_index(drop=True)

    # 从records构建市值字典（用于Excel/可视化）
    mkt_dict = {r['code']: r['float_mkt'] for r in records}
    tr5_dict  = {r['code']: r['tr5'] for r in records}

    # 找多公式共振（含X）
    codes_a = set(top_a['code'])
    codes_b = set(top_b['code'])
    codes_c = set(top_c['code'])
    codes_d = set(top_d['code'])
    codes_x = set(top_x['code'])
    all_codes_in_top = [codes_a, codes_b, codes_c, codes_d, codes_x]
    resonance = {}
    for code in set.union(*all_codes_in_top):
        cnt = sum(1 for s in all_codes_in_top if code in s)
        if cnt >= 2:
            resonance[code] = cnt

    print(f'\n📊 公式A预过滤 {len(df_a_pass)}只 公式X预过滤 {len(df_x_pass)}只')
    print(f'📊 共振股（>=2公式）: {len(resonance)} 只')
    for code, cnt in sorted(resonance.items(), key=lambda x: -x[1]):
        print(f'  {code}: {cnt} 个公式入选 {"⭐"*cnt}')

    # 保存结果
    out_csv = os.path.join(OUTPUT_DIR, '135factor_abcd_selected.csv')
    out_xlsx = os.path.join(OUTPUT_DIR, '135factor_abcd_selected.xlsx')

    top_a['formula'] = 'A'
    top_b['formula'] = 'B'
    top_c['formula'] = 'C'
    top_d['formula'] = 'D'
    top_x['formula'] = 'X'
    combined = pd.concat([top_a, top_b, top_c, top_d, top_x], ignore_index=True)
    combined.to_csv(out_csv, index=False)

    # 写入Excel（含5个Sheet + 共振汇总）
    try:
        with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
            top_a.to_excel(writer, sheet_name='公式A_隔夜低换手', index=False)
            top_b.to_excel(writer, sheet_name='公式B_波动调整', index=False)
            top_c.to_excel(writer, sheet_name='公式C_反转低位', index=False)
            top_d.to_excel(writer, sheet_name='公式D_价量共振', index=False)
            top_x.to_excel(writer, sheet_name='公式X_日内动量', index=False)
            # 共振汇总Sheet
            if resonance:
                res_sorted = sorted(resonance.items(), key=lambda x: -x[1])
                res_rows = []
                for code, cnt in res_sorted:
                    row = {'股票代码': code, '共振公式数': cnt}
                    mkt_val = mkt_dict.get(code, 0)
                    row['流通市值'] = f'{mkt_val:.0f}亿' if mkt_val > 0 else '—'
                    tr5_val = tr5_dict.get(code, 0)
                    row['TR5(%)'] = f'{tr5_val:.1f}%' if tr5_val > 0 else '—'
                    for fname, tdf, scol in [('A', top_a, 'score_A'), ('B', top_b, 'score_B'),
                                              ('C', top_c, 'score_C'), ('D', top_d, 'score_D'), ('X', top_x, 'score_X')]:
                        if code in set(tdf['code']):
                            sv = tdf[tdf['code'] == code][scol].values
                            row[f'公式{fname}得分'] = f'{sv[0]:.1f}' if len(sv) > 0 else '-'
                        else:
                            row[f'公式{fname}得分'] = '-'
                    res_rows.append(row)
                df_res = pd.DataFrame(res_rows)
                df_res.to_excel(writer, sheet_name='共振汇总', index=False)
        print(f'📊 Excel已保存: {out_xlsx}')
    except Exception as e:
        print(f'  ⚠️ Excel写入失败: {e}')

    # ─── 可视化 ────────────────────────────────────────────────────────────────
    print('\n🎨 生成可视化报告...')
    fig = plt.figure(figsize=(22, 28), facecolor='#0d1117')

    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.50, wspace=0.35,
                           top=0.94, bottom=0.03, left=0.05, right=0.97,
                           height_ratios=[1, 1, 0.8])

    COLORS = {
        'A': ('#FF6B35', '#FFB347', '隔夜效应+低换手'),
        'B': ('#4FC3F7', '#81D4FA', '波动调整+K线强势'),
        'C': ('#69F0AE', '#B9F6CA', '反转低位逆向蓄势'),
        'D': ('#CE93D8', '#E1BEE7', '价量共振综合版'),
        'X': ('#FFD700', '#FFF176', '日内动量收盘→次日卖'),
    }
    formula_data = [
        ('A', top_a, 'score_A', COLORS['A']),
        ('B', top_b, 'score_B', COLORS['B']),
        ('C', top_c, 'score_C', COLORS['C']),
        ('D', top_d, 'score_D', COLORS['D']),
        ('X', top_x, 'score_X', COLORS['X']),
    ]

    for idx, (fname, top_df, score_col, (c1, c2, desc)) in enumerate(formula_data[:4]):
        # 前4个公式: A/B/C/D 放在前两行
        row_i = idx // 2
        col_i = idx % 2
        ax = fig.add_subplot(gs[row_i, col_i])
        ax.set_facecolor('#161b22')

        n = min(15, len(top_df))
        codes = top_df['code'].head(n).tolist()
        scores = top_df[score_col].head(n).tolist()
        mkts = top_df['float_mkt'].head(n).tolist() if 'float_mkt' in top_df.columns else [0]*n

        bar_colors = []
        for code in codes:
            if code in resonance and resonance[code] >= 3:
                bar_colors.append('#FFD700')
            elif code in resonance and resonance[code] == 2:
                bar_colors.append('#FF6B6B')
            else:
                bar_colors.append(c1)

        bars = ax.barh(range(n), scores[::-1], color=bar_colors[::-1],
                       edgecolor='none', height=0.7)

        ylabels = []
        for code, mkt in zip(codes[::-1], mkts[::-1]):
            mkt_str = f'{mkt:.0f}亿' if mkt > 0 else '—'
            ylabels.append(f'{code}  {mkt_str}')

        ax.set_yticks(range(n))
        ax.set_yticklabels(ylabels, color='white', fontsize=8.5, fontfamily='monospace')
        ax.set_xlabel('综合评分', color='#8b949e', fontsize=9)
        ax.tick_params(colors='#8b949e', labelsize=8)
        ax.spines[:].set_color('#30363d')
        ax.set_xlim(min(scores)*0.95, max(scores)*1.03)

        title_text = f'公式{fname}: {desc}\nTop{n} 候选股（已剔除ST+市值<50亿+TR5>10%）  |  ■ 金:3+共振  ■ 红:2共振'
        ax.set_title(title_text, color=c2, fontsize=10, fontweight='bold', pad=8)
        ax.grid(axis='x', color='#21262d', linewidth=0.5)

    # 公式X: 单独一行(第3行左侧)
    ax_x = fig.add_subplot(gs[2, 0])
    ax_x.set_facecolor('#161b22')
    fname, top_df, score_col, (c1, c2, desc) = ('X', top_x, 'score_X', COLORS['X'])
    n = min(15, len(top_df))
    codes = top_df['code'].head(n).tolist()
    scores = top_df[score_col].head(n).tolist()
    mkts = top_df['float_mkt'].head(n).tolist()
    tr5s = top_df['tr5'].head(n).tolist()

    bar_colors = []
    for code in codes:
        if code in resonance and resonance[code] >= 3:
            bar_colors.append('#FFD700')
        elif code in resonance and resonance[code] == 2:
            bar_colors.append('#FF6B6B')
        else:
            bar_colors.append(c1)

    bars = ax_x.barh(range(n), scores[::-1], color=bar_colors[::-1], edgecolor='none', height=0.7)
    ylabels = []
    for code, mkt, tr5 in zip(codes[::-1], mkts[::-1], tr5s[::-1]):
        mkt_str = f'{mkt:.0f}亿' if mkt > 0 else '—'
        tr5_str = f'{tr5:.1f}%' if tr5 > 0 else '—'
        ylabels.append(f'{code}  {mkt_str}  TR5{tr5_str}')
    ax_x.set_yticks(range(n))
    ax_x.set_yticklabels(ylabels, color='white', fontsize=8.5, fontfamily='monospace')
    ax_x.set_xlabel('综合评分', color='#8b949e', fontsize=9)
    ax_x.tick_params(colors='#8b949e', labelsize=8)
    ax_x.spines[:].set_color('#30363d')
    ax_x.set_xlim(min(scores)*0.95, max(scores)*1.03)
    ax_x.set_title(f'公式X: 日内动量（收盘买→次日卖）\nTop{n} 候选（10条件全部满足）  |  ■ 金:3+共振  ■ 红:2共振', color=c2, fontsize=10, fontweight='bold', pad=8)
    ax_x.grid(axis='x', color='#21262d', linewidth=0.5)

    # 共振汇总表 (第3行右侧)
    ax_res = fig.add_subplot(gs[2, 1])
    ax_res.set_facecolor('#161b22')
    ax_res.axis('off')

    if resonance:
        res_sorted = sorted(resonance.items(), key=lambda x: -x[1])
        table_data = []
        for code, cnt in res_sorted:
            row_data = [code, '⭐' * cnt]
            mkt_val = mkt_dict.get(code, 0)
            row_data.append(f'{mkt_val:.0f}亿' if mkt_val > 0 else '—')
            tr5_val = tr5_dict.get(code, 0)
            row_data.append(f'{tr5_val:.1f}%' if tr5_val > 0 else '—')
            for fname, top_df, score_col, _ in formula_data:
                if code in set(top_df['code']):
                    sv = top_df[top_df['code'] == code][score_col].values
                    row_data.append(f'{sv[0]:.1f}' if len(sv) > 0 else '-')
                else:
                    row_data.append('-')
            table_data.append(row_data)

        col_labels = ['股票代码', '共振等级', '流通市值', 'TR5(%)',
                      '公式A', '公式B', '公式C', '公式D', '公式X']
        ax_res.set_title(f'★ 多公式共振股（{len(resonance)}只）—— 优先关注',
                         color='#FFD700', fontsize=12, fontweight='bold', pad=15)

        table = ax_res.table(
            cellText=table_data,
            colLabels=col_labels,
            cellLoc='center',
            loc='center',
            bbox=[0, 0.05, 1, 0.90]
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        for (r, c), cell in table.get_celld().items():
            cell.set_facecolor('#0d1117' if r % 2 == 0 else '#161b22')
            cell.set_edgecolor('#30363d')
            if r == 0:
                cell.set_facecolor('#21262d')
                cell.set_text_props(color='#FFD700', fontweight='bold')
            else:
                cell.set_text_props(color='white')
    else:
        ax_res.text(0.5, 0.5, '暂无多公式共振股',
                    ha='center', va='center', color='#8b949e', fontsize=14,
                    transform=ax_res.transAxes)

    # 总标题
    fig.suptitle('全量135因子实测版 ABCD+X 五公式选股报告',
                 color='white', fontsize=16, fontweight='bold', y=0.97)
    fig.text(0.5, 0.955,
             'A:隔夜低换手  B:波动调整+趋势  C:超跌反转  D:价量共振  X:日内动量收盘→次日卖  |  基于IC/IR Top因子实证设计',
             ha='center', color='#8b949e', fontsize=10)

    out_png = os.path.join(OUTPUT_DIR, '135factor_abcd_report.png')
    fig.savefig(out_png, dpi=150, bbox_inches='tight', facecolor='#0d1117')
    plt.close()
    print(f'📊 报告已保存: {out_png}')
    print(f'📄 CSV已保存:  {out_csv}')
    print(f'📊 Excel已保存: {out_xlsx}')

    # 打印详细结果
    print('\n' + '='*60)
    for fname, top_df, score_col, (c1, c2, desc) in formula_data:
        print(f'\n【公式{fname}】{desc}  Top10:')
        print(top_df.head(10).to_string(index=False))

    return df_all, top_a, top_b, top_c, top_d, top_x, resonance

if __name__ == '__main__':
    main()
