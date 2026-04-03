# multF - A股多因子量化选股系统

基于A股真实行情数据的量化选股与因子分析系统。核心方法论参考：
- 北大光华刘晓蕾团队（Management Science 2023）：469个A股异象检验
- AlphaForge / AlphaGPS / AlphaGen（JMLR/KDD 2023-2025）
- HRFT-WWW 2025 / QFR等最新学术成果

## 项目结构

```
multF/
├── screener_135factor_abcd.py     # 🏆 主选股程序（五公式ABCD+X）
├── factor_hunter/                # 因子猎人自动化系统
│   ├── factor_hunter.py          # 每日15:30自动运行，搜索最优因子组合
│   ├── calc_ic_realdata_v7.py    # IC/IR回测计算
│   ├── gen_excel_full_135.py     # 全量135因子Excel监控
│   ├── generate_ic_monitor.py    # IC_IR可视化报告
│   ├── alternative_factors_v2.py  # 量价另类因子库
│   ├── fundamental_factors_v2.py   # 基本面因子库
│   └── merge_*.py                # 因子合并
├── q_factor/                     # Q因子模型研究
│   ├── q_factor_model_v2.py      # Q因子构建
│   └── q_factor_backtest_v2.py   # Q因子回测
├── formulas/                     # 通达信公式（可直接导入）
│   ├── 通达信选股公式_135因子版.txt  # 最新五公式
│   ├── 通达信选股公式_v9.txt      # v9版
│   └── 公式X_日内动量版.txt       # 日内动量T+1
├── output/                       # 样例输出
│   ├── 135factor_abcd_selected.xlsx
│   └── 135factor_abcd_report.png
└── data/                         # K线数据目录（见下方）
```

## 快速开始

### 1. 数据准备

下载V3 K线数据到 `data/V3/` 目录：
- 文件格式：每只股票一个CSV，列：`date,open,high,low,close,volume,amount,pctChg`
- code格式：`sz.000001` / `sh.600519`
- 推荐数据源：`/Users/ydy/Downloads/V3`（6995只A股）

或使用其他格式的数据目录，修改脚本中的 `DATA_DIR` 常量。

### 2. 运行选股

```bash
# 五公式ABCD+X选股（推荐）
python screener_135factor_abcd.py

# 输出
# - factor_hunter/output/135factor_abcd_selected.xlsx  # Excel结果
# - factor_hunter/output/135factor_abcd_report.png    # 可视化报告
```

### 3. 因子IC回测

```bash
python factor_hunter/calc_ic_realdata_v7.py
```

### 4. 自动化（可选）

```bash
# 配置定时任务，每日15:30自动运行因子猎人
# 参见 factor_hunter/factor_hunter.py 头部说明
```

## 五公式体系（135因子版）

| 公式 | 策略 | 核心因子 | 风格 |
|------|------|---------|------|
| **A** | 隔夜效应+低换手 | overnight_ret + turnover_proxy | 稳健中线 |
| **B** | 波动调整+K线强势 | qfr_vol_adj_ret + candle_body + aroon | 短线趋势 |
| **C** | 反转低位逆向蓄势 | rank_mom_60(bear) + bias_60 + lower_shadow | 左侧抄底 |
| **D** | 价量共振综合 | overnight_ret + candle_body + amt_stab + MA多头 | 中线综合 |
| **X** | 日内动量（T+1） | overnight_ret + candle_body_ratio + today_ret | 隔夜动量 |

### 全量135因子 Top5（IC_IR，2023-01 至 2026-04）

| 排名 | 因子 | IR | 方向 | 来源 |
|------|------|-----|------|------|
| #1 | overnight_ret | +2.26 | 正向 | 北大光华 |
| #2 | turnover_rate | -1.28 | 逆向 | 低换手=机构稳定 |
| #3 | rank_prod_60 | -1.28 | 逆向 | AlphaGen |
| #4 | amt_stability | -1.21 | 逆向 | 成交稳定 |
| #5 | qfr_vol_adj_ret | +1.18 | 正向 | QFR |

**关键规律**：A股反转效应强于趋势，逆向因子（低换手、低波动）普遍优于趋势因子。

## 主要模块说明

### factor_hunter.py
因子猎人自动化系统。每日扫描全市场，352种因子组合搜索，寻找最优权重。
- 最优历史组合：年化61.1%，夏普3.83，最大回撤-6.3%
- 最优因子：drawdown_20 > drawdown_60 > price_pos_10 > breakout_60

### calc_ic_realdata_v7.py
因子IC（Information Coefficient）回测。计算每个因子与下月收益的相关性，评估预测能力。

### gen_excel_full_135.py
生成全量135因子监控Excel，包含：
- Sheet1：因子监控总表（IC均值/IR/胜率/月度收益）
- Sheet2：月度IC热力图
- Sheet3：使用说明
- Sheet4：通达信选股公式

## 通达信公式使用

1. 打开通达信软件
2. 功能 → 公式系统 → 公式管理器
3. 新建 → 技术指标/条件选股
4. 粘贴 `formulas/通达信选股公式_135因子版.txt` 中的公式代码

公式A示例（须配合市值/换手率数据源）：
```
NOT_ST := NOT(NAMELIKE('ST')) AND NOT(NAMELIKE('*ST'));
MKT50  := FINANCE(40)>5000;
M5 := FINANCE(37)*100<5;
AVG_OVN := ...;
AMT_STAB := ...;
RET30 := ...;
NOT_ST AND MKT50 AND M1 AND M2 AND M3 AND M4 AND M5 AND AVG_OVN>0.001 AND AMT_STAB<0.5 AND RET30>0;
```

## 数据说明

| 数据集 | 路径 | 股票数 | 更新时间 |
|--------|------|--------|---------|
| V3（推荐）| `data/V3/` | 6995只 | 每日更新 |
| 旧版 | `data/market_data/` | 4066只 | 静态 |

## 技术栈

- Python 3.9+（pandas、numpy、matplotlib、seaborn）
- 数据：本地CSV（通达信导出 / AKShare / baostock）
- 可视化：matplotlib + seaborn（暗色主题）
- 公式：通达信/东方财富

## 免责声明

本项目仅供研究学习，不构成投资建议。量化策略存在失效风险，历史收益不代表未来表现。
