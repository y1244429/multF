# 数据目录说明

本目录用于存放K线数据。由于数据文件较大（V3约6995只股票CSV），请手动下载并放置于此。

## 推荐数据：V3（6995只A股）

### 下载方式

**方式一：通达信导出（推荐）**
1. 打开通达信 → 安装选择器 → 勾选所有A股
2. 功能 → 数据导出 → 高级导出
3. 选择：日期、开盘、最高、最低、收盘、成交量、成交额、涨跌幅
4. 导出格式：CSV，分隔符：逗号

**方式二：AKShare脚本下载**
```python
import akshare as ak
import pandas as pd

# 获取全市场股票列表
stock_info = ak.stock_info_a_code_name()

# 逐只下载日线数据（示例）
for code in stock_info['code'][:10]:
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        df.to_csv(f"V3/{code}.csv", index=False)
    except:
        pass
```

### 文件格式要求

| 列名 | 类型 | 说明 |
|------|------|------|
| date | str/datetime | 交易日期，格式 YYYY-MM-DD |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | float | 成交量（股） |
| amount | float | 成交额（元） |
| pctChg | float | 涨跌幅（%） |

### 代码格式
- 上证：`sh.600519`
- 深证：`sz.000001`
- 科创板：`sh.688xxx`
- 创业板：`sz.300xxx`

### 路径配置

在脚本中修改 `DATA_DIR` 常量：
```python
DATA_DIR = '/path/to/your/multF/data/V3'
```

## 旧版数据（4066只）

如使用旧版数据目录：
```
/Users/ydy/CodeBuddy/output/market_data
```
每只股票CSV包含 `code` 列，格式不同，脚本已做兼容处理。
