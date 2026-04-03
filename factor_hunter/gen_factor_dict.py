#!/usr/bin/env python3
"""
生成因子说明 Excel 文件
"""

import pandas as pd

# 因子定义
factors = [
    # 动量类
    ("mom_5", "动量5日", "最近5日涨跌幅百分比，衡量短期动量"),
    ("mom_10", "动量10日", "最近10日涨跌幅百分比，衡量短期动量"),
    ("mom_15", "动量15日", "最近15日涨跌幅百分比，衡量中短期动量"),
    ("mom_20", "动量20日", "最近20日涨跌幅百分比，衡量中期动量"),
    ("mom_30", "动量30日", "最近30日涨跌幅百分比，衡量中期动量"),
    ("mom_60", "动量60日", "最近60日涨跌幅百分比，衡量中长线动量"),
    ("mom_90", "动量90日", "最近90日涨跌幅百分比，衡量长线动量"),
    ("mom_120", "动量120日", "最近120日涨跌幅百分比，衡量长线动量"),
    ("mom_accel", "动量加速度", "5日动量 - 10日动量，衡量动量的变化趋势（加速/减速）"),

    # 均线偏离
    ("bias_ma5", "5日均线偏离", "收盘价相对于5日均线的偏离度（百分比），衡量短期超买超卖"),
    ("bias_ma10", "10日均线偏离", "收盘价相对于10日均线的偏离度（百分比）"),
    ("bias_ma20", "20日均线偏离", "收盘价相对于20日均线的偏离度（百分比）"),
    ("bias_ma40", "40日均线偏离", "收盘价相对于40日均线的偏离度（百分比）"),
    ("bias_ma60", "60日均线偏离", "收盘价相对于60日均线的偏离度（百分比），衡量中长期超买超卖"),

    # 均线斜率
    ("ma10_slope", "10日均线斜率", "5日内10日均线的变化率，衡量短期趋势强弱"),
    ("ma20_slope", "20日均线斜率", "5日内20日均线的变化率，衡量中期趋势强弱"),
    ("ma40_slope", "40日均线斜率", "5日内40日均线的变化率"),
    ("ma60_slope", "60日均线斜率", "5日内60日均线的变化率，衡量中长期趋势"),

    # 均线多头排列
    ("ma_bull", "均线多头排列", "MA5>MA10 + MA10>MA20 + MA20>MA60，值越大排列越完美，衡量趋势强度"),

    # 波动率
    ("volatility_5", "5日波动率", "最近5日收益率年化标准差，衡量短期波动"),
    ("volatility_10", "10日波动率", "最近10日收益率年化标准差，衡量短期波动"),
    ("volatility_20", "20日波动率", "最近20日收益率年化标准差，衡量中期波动"),
    ("volatility_60", "60日波动率", "最近60日收益率年化标准差，衡量长期波动"),

    # ATR
    ("atr_20", "20日ATR", "20日平均真实波幅/收盘价*100，衡量价格波动幅度"),

    # 成交量
    ("vol_ratio_5", "5日成交量比率", "当日成交量 / 5日平均成交量，衡量量能放大"),
    ("vol_ratio_10", "10日成交量比率", "当日成交量 / 10日平均成交量"),
    ("vol_ratio_20", "20日成交量比率", "当日成交量 / 20日平均成交量"),
    ("vol_ratio_60", "60日成交量比率", "当日成交量 / 60日平均成交量，衡量长期量能变化"),

    # 成交额
    ("amt_ratio_5", "5日成交额比率", "当日成交额 / 5日平均成交额，衡量资金活跃度"),
    ("amt_ratio_10", "10日成交额比率", "当日成交额 / 10日平均成交额"),
    ("amt_ratio_20", "20日成交额比率", "当日成交额 / 20日平均成交额"),
    ("amt_ratio_60", "60日成交额比率", "当日成交额 / 60日平均成交额"),

    # 价格位置
    ("price_pos_10", "价格位置10日", "收盘价在10日区间内的百分位（0-100），衡量相对强弱"),
    ("price_pos_20", "价格位置20日", "收盘价在20日区间内的百分位（0-100）"),
    ("price_pos_60", "价格位置60日", "收盘价在60日区间内的百分位（0-100），衡量中长期相对位置"),

    # 距新高天数
    ("days_high_20", "距20日新高天数", "距离20日内最高点的天数，越小越强势"),
    ("days_high_60", "距60日新高天数", "距离60日内最高点的天数"),

    # 回撤
    ("drawdown_20", "20日回撤", "收盘价相对于20日最高点的回撤幅度（负数），越小越好"),
    ("drawdown_60", "60日回撤", "收盘价相对于60日最高点的回撤幅度（负数），抗跌性强"),

    # 突破
    ("breakout_20", "突破20日新高", "收盘价突破昨日20日最高点为1，否则0，衡量突破信号"),
    ("breakout_60", "突破60日新高", "收盘价突破昨日60日最高点为1，否则0，衡量长线突破"),

    # K线形态
    ("candle_body", "K线实体", "（收盘-开盘）/（最高-最低）*100，衡量K线实体饱满度，反映多空力量"),

    # 连涨天数
    ("consec_up", "连续上涨天数", "连续上涨的天数，动量延续性指标"),

    # 量价背离
    ("vol_price_div", "量价背离", "20日涨幅 - 20日成交量变化率*100，衡量量价配合度，负值表示量价背离"),
]

# 创建 DataFrame
df = pd.DataFrame(factors, columns=["因子代码", "因子名称", "计算原理说明"])

# 读取单因子回测结果获取夏普排名
try:
    results = pd.read_csv("/Users/ydy/WorkBuddy/20260326134350/factor_hunter/output/single_factor_results.csv")
    results = results[['factor', 'sharpe', 'ann_return', 'max_drawdown']]
    results = results.sort_values('sharpe', ascending=False).reset_index(drop=True)
    results['排名'] = range(1, len(results) + 1)
    df = df.merge(results[['factor', 'sharpe', 'ann_return', 'max_drawdown', '排名']],
                  left_on='因子代码', right_on='factor', how='left')
    df = df.drop(columns=['factor'])
    df = df.sort_values('排名')
except:
    df['夏普'] = None
    df['年化'] = None
    df['回撤'] = None
    df['排名'] = None

# 重新排列列
df = df.sort_values('排名', na_position='last')
df = df[['排名', '因子代码', '因子名称', '计算原理说明', 'sharpe', 'ann_return', 'max_drawdown']]
df = df.rename(columns={'sharpe': '夏普', 'ann_return': '年化(%)', 'max_drawdown': '最大回撤(%)'})

# 保存为 Excel
output_path = "/Users/ydy/WorkBuddy/20260326134350/factor_hunter/output/因子字典_带说明.xlsx"
df.to_excel(output_path, index=False, engine='openpyxl')

print(f"✅ 因子字典已保存: {output_path}")
print(f"\n共 {len(df)} 个因子")
print(f"\nTop 5 因子（按夏普）:")
print(df.head(5).to_string(index=False))
