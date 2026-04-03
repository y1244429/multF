#!/usr/bin/env python3
"""
A股12因子选股 + 90天回测系统
基于：规模、价值、盈利、成长、动量、反转、波动率、流动性、杠杆、投资、分析师预期
"""

import baostock as bs
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class Factor12Screener:
    """12因子选股器"""
    
    def __init__(self):
        self.lg = None
        self.stock_list = None
        self.factor_data = None
        self.price_data = {}
        
    def login(self):
        self.lg = bs.login()
        if self.lg.error_code != '0':
            raise Exception(f"登录失败: {self.lg.error_msg}")
        print("✅ Baostock 登录成功")
        return True
    
    def logout(self):
        if self.lg:
            bs.logout()
            print("✅ 已登出")
    
    def get_all_stocks(self):
        print("\n📊 获取全市场股票列表...")
        query_date = '2025-03-28'
        rs = bs.query_all_stock(day=query_date)
        
        stock_list = []
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if (code.startswith('sh.6') or 
                code.startswith('sz.000') or 
                code.startswith('sz.002') or 
                code.startswith('sz.300')):
                stock_list.append({'code': code, 'name': row[2] if len(row) > 2 else ''})
        
        self.stock_list = pd.DataFrame(stock_list)
        print(f"   获取到 {len(self.stock_list)} 只A股")
        return self.stock_list
    
    def get_factors(self, code, year=2024, quarter=3):
        """获取12因子数据"""
        try:
            result = {'code': code}
            
            # 1. 杜邦分析（ROE、杠杆）
            rs = bs.query_dupont_data(code=code, year=year, quarter=quarter)
            if rs.next():
                row = rs.get_row_data()
                result['roe'] = float(row[3]) if row[3] else 0  # dupontROE
                result['asset_to_equity'] = float(row[4]) if row[4] else 0
                result['asset_turnover'] = float(row[6]) if row[6] else 0
            
            # 2. 成长能力（资产增速）
            rs = bs.query_growth_data(code=code, year=year, quarter=quarter)
            if rs.next():
                row = rs.get_row_data()
                result['yoy_asset'] = float(row[4]) if row[4] else 0  # YOYAsset
                result['yoy_equity'] = float(row[3]) if row[3] else 0
            
            # 3. 盈利能力（净利率）
            rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
            if rs.next():
                row = rs.get_row_data()
                result['net_margin'] = float(row[6]) if row[6] else 0
                result['gross_margin'] = float(row[5]) if row[5] else 0
            
            # 4. 获取K线数据（计算动量、反转、波动率、市值代理）
            end_date = '2025-03-28'
            start_date = '2024-06-01'
            rs = bs.query_history_k_data_plus(
                code, "date,code,close,open,high,low,volume,amount,turn",
                start_date=start_date, end_date=end_date, frequency="d", adjustflag="3"
            )
            
            kdata = []
            while rs.next():
                kdata.append(rs.get_row_data())
            
            if len(kdata) < 60:
                return None
            
            df_k = pd.DataFrame(kdata, columns=rs.fields)
            df_k['close'] = df_k['close'].astype(float)
            df_k['volume'] = df_k['volume'].astype(float)
            df_k['amount'] = df_k['amount'].astype(float)
            df_k['turn'] = df_k['turn'].astype(float)
            df_k = df_k.sort_values('date')
            
            # 计算技术指标
            closes = df_k['close'].values
            
            # 规模因子：用收盘价代理（小市值通常股价低，但这里用倒数）
            result['size'] = closes[-1]
            
            # 动量因子：12个月收益（约240个交易日）
            if len(closes) >= 240:
                result['momentum_12m'] = closes[-1] / closes[-240] - 1
            else:
                result['momentum_12m'] = closes[-1] / closes[0] - 1
            
            # 反转因子：1个月收益
            result['reversal_1m'] = closes[-1] / closes[-20] - 1 if len(closes) >= 20 else 0
            
            # 波动率因子：20日收益率标准差
            if len(closes) >= 21:
                returns = np.diff(closes[-21:]) / closes[-22:-1]
                result['volatility'] = np.std(returns)
            else:
                result['volatility'] = 0
            
            # 流动性因子：平均换手率
            result['liquidity'] = df_k['turn'].tail(20).mean()
            
            # 价值因子：用PE代理（需要计算，这里简化用1/股价）
            result['value_proxy'] = 1 / closes[-1]
            
            return result
            
        except Exception as e:
            return None
    
    def batch_get_factors(self, max_stocks=None):
        """批量获取因子数据"""
        stocks = self.stock_list.head(max_stocks) if max_stocks else self.stock_list
        total = len(stocks)
        
        print(f"\n📈 获取12因子数据（共 {total} 只）...")
        
        data_list = []
        for idx, row in stocks.iterrows():
            if idx % 100 == 0:
                print(f"   进度: {idx}/{total} ({idx/total*100:.0f}%)")
            
            data = self.get_factors(row['code'])
            if data:
                data_list.append(data)
        
        self.factor_data = pd.DataFrame(data_list)
        print(f"✅ 成功获取 {len(self.factor_data)} 只股票因子数据")
        return self.factor_data
    
    def calculate_12factor_score(self):
        """计算12因子综合得分"""
        print("\n🎯 计算12因子得分...")
        
        df = self.factor_data.copy()
        
        # 基础过滤
        df = df[(df['roe'] > 0) & (df['roe'] < 1)]
        df = df[df['size'] > 0]
        
        print(f"   基础过滤后: {len(df)} 只股票")
        
        # 12因子打分（截面百分位）
        
        # 1. 规模因子（小市值加分）
        df['score_size'] = 100 - self._percentile_score(df['size'])
        
        # 2. 价值因子（高价值加分）
        df['score_value'] = self._percentile_score(df['value_proxy'])
        
        # 3. 盈利因子（高ROE加分）
        df['score_roe'] = self._percentile_score(df['roe'])
        
        # 4. 成长因子（高资产增速加分）
        df['score_growth'] = self._percentile_score(df['yoy_asset'])
        
        # 5. 动量因子（高收益加分）
        df['score_momentum'] = self._percentile_score(df['momentum_12m'])
        
        # 6. 反转因子（短期跌的加分 - 均值回归）
        df['score_reversal'] = 100 - self._percentile_score(df['reversal_1m'])
        
        # 7. 波动率因子（低波动加分）
        df['score_volatility'] = 100 - self._percentile_score(df['volatility'])
        
        # 8. 流动性因子（适中换手，这里用低换手代表机构持股）
        df['score_liquidity'] = 100 - self._percentile_score(df['liquidity'])
        
        # 9. 杠杆因子（低杠杆加分）
        df['debt_ratio'] = 1 - 1/df['asset_to_equity'].clip(lower=1.0)
        df['score_leverage'] = 100 - self._percentile_score(df['debt_ratio'])
        
        # 10. 投资因子（低资产增速加分 - q因子）
        df['score_investment'] = 100 - self._percentile_score(df['yoy_asset'])
        
        # 11. 净利率因子
        df['score_margin'] = self._percentile_score(df['net_margin'])
        
        # 12. 周转率因子
        df['score_turnover'] = self._percentile_score(df['asset_turnover'])
        
        # 综合得分（等权）
        factor_cols = ['score_size', 'score_value', 'score_roe', 'score_growth',
                       'score_momentum', 'score_reversal', 'score_volatility',
                       'score_liquidity', 'score_leverage', 'score_investment',
                       'score_margin', 'score_turnover']
        
        df['composite_score'] = df[factor_cols].mean(axis=1)
        
        # 排序
        df = df.sort_values('composite_score', ascending=False).reset_index(drop=True)
        df['rank'] = range(1, len(df) + 1)
        
        self.factor_data = df
        print(f"✅ 12因子得分计算完成")
        return df
    
    def _percentile_score(self, series):
        """计算百分位得分"""
        mean = series.mean()
        std = series.std()
        series = series.clip(mean - 3*std, mean + 3*std)
        return series.rank(pct=True) * 100
    
    def get_price_history(self, code, start_date, end_date):
        """获取历史价格"""
        try:
            rs = bs.query_history_k_data_plus(
                code, "date,close", start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            
            if not data:
                return None
            
            df = pd.DataFrame(data, columns=['date', 'close'])
            df['close'] = df['close'].astype(float)
            return df
        except:
            return None
    
    def backtest_90days(self, top_n=30):
        """90天回测"""
        print("\n📊 开始90天回测...")
        
        # 选股日
        select_date = '2025-03-28'
        # 回测结束日（90天后）
        end_date = '2025-06-26'  # 约90个交易日
        
        # 获取Top N股票
        top_stocks = self.factor_data.head(top_n)['code'].tolist()
        print(f"   选中 {len(top_stocks)} 只股票")
        
        # 获取每只股票的价格
        portfolio = []
        for code in top_stocks:
            # 获取选股日价格
            df_start = self.get_price_history(code, select_date, select_date)
            # 获取回测期间价格
            df_end = self.get_price_history(code, end_date, end_date)
            
            if df_start is not None and not df_start.empty and df_end is not None and not df_end.empty:
                start_price = df_start['close'].iloc[0]
                end_price = df_end['close'].iloc[0]
                
                if start_price > 0:
                    portfolio.append({
                        'code': code,
                        'start_price': start_price,
                        'end_price': end_price,
                        'return': (end_price - start_price) / start_price
                    })
        
        if not portfolio:
            print("⚠️ 无法获取回测价格数据")
            return None
        
        df_portfolio = pd.DataFrame(portfolio)
        
        # 计算组合收益（等权）
        portfolio_return = df_portfolio['return'].mean()
        
        # 获取基准收益（沪深300）
        benchmark_start = self.get_price_history('sh.000300', select_date, select_date)
        benchmark_end = self.get_price_history('sh.000300', end_date, end_date)
        
        if benchmark_start is not None and benchmark_end is not None:
            benchmark_return = (benchmark_end['close'].iloc[0] - benchmark_start['close'].iloc[0]) / benchmark_start['close'].iloc[0]
        else:
            benchmark_return = 0
        
        result = {
            'portfolio': df_portfolio,
            'portfolio_return': portfolio_return,
            'benchmark_return': benchmark_return,
            'alpha': portfolio_return - benchmark_return,
            'select_date': select_date,
            'end_date': end_date
        }
        
        print(f"\n📈 回测结果:")
        print(f"   组合收益: {portfolio_return*100:.2f}%")
        print(f"   基准收益: {benchmark_return*100:.2f}%")
        print(f"   超额收益: {result['alpha']*100:.2f}%")
        
        return result
    
    def generate_report(self, backtest_result=None):
        """生成报告"""
        print("\n📊 生成报告...")
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('12因子选股 + 90天回测报告', fontsize=16, fontweight='bold')
        
        # 1. Top 20 综合得分
        ax1 = axes[0, 0]
        top20 = self.factor_data.head(20)
        colors = plt.cm.RdYlGn(top20['composite_score'] / 100)
        ax1.barh(range(len(top20)), top20['composite_score'], color=colors)
        ax1.set_yticks(range(len(top20)))
        ax1.set_yticklabels([c.split('.')[-1] for c in top20['code']], fontsize=8)
        ax1.invert_yaxis()
        ax1.set_xlabel('综合得分')
        ax1.set_title('Top 20 12因子综合得分')
        ax1.grid(axis='x', alpha=0.3)
        
        # 2. 各因子平均得分
        ax2 = axes[0, 1]
        factor_scores = ['score_size', 'score_value', 'score_roe', 'score_growth',
                        'score_momentum', 'score_reversal', 'score_volatility',
                        'score_liquidity', 'score_leverage', 'score_investment',
                        'score_margin', 'score_turnover']
        factor_names = ['规模', '价值', 'ROE', '成长', '动量', '反转', 
                       '波动率', '流动性', '杠杆', '投资', '净利率', '周转率']
        
        top50_avg = self.factor_data.head(50)[factor_scores].mean()
        bars = ax2.bar(factor_names, top50_avg, color=plt.cm.tab20(np.linspace(0, 1, 12)))
        ax2.set_ylabel('平均得分')
        ax2.set_title('Top 50 各因子平均得分')
        ax2.set_ylim(0, 100)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax2.grid(axis='y', alpha=0.3)
        
        # 3. 得分分布
        ax3 = axes[1, 0]
        ax3.hist(self.factor_data['composite_score'], bins=30, color='steelblue', alpha=0.7, edgecolor='black')
        ax3.axvline(self.factor_data['composite_score'].mean(), color='red', linestyle='--', label=f'均值: {self.factor_data["composite_score"].mean():.1f}')
        ax3.set_xlabel('综合得分')
        ax3.set_ylabel('股票数量')
        ax3.set_title('全市场12因子得分分布')
        ax3.legend()
        ax3.grid(alpha=0.3)
        
        # 4. 回测结果（如果有）
        ax4 = axes[1, 1]
        if backtest_result and backtest_result['portfolio'] is not None:
            returns = backtest_result['portfolio']['return'].sort_values(ascending=True) * 100
            colors = ['green' if r > 0 else 'red' for r in returns]
            ax4.barh(range(len(returns)), returns, color=colors, alpha=0.7)
            ax4.axvline(0, color='black', linestyle='-', linewidth=0.5)
            ax4.axvline(backtest_result['portfolio_return']*100, color='blue', linestyle='--', 
                       label=f"组合平均: {backtest_result['portfolio_return']*100:.1f}%")
            ax4.set_xlabel('收益率 (%)')
            ax4.set_title('Top 30 个股90天收益')
            ax4.legend()
            ax4.grid(axis='x', alpha=0.3)
        else:
            ax4.text(0.5, 0.5, '回测数据不可用\n(需要未来数据)', 
                    ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('90天回测')
        
        plt.tight_layout()
        plt.savefig('stock_screener_12factor_report.png', dpi=150, bbox_inches='tight')
        print("✅ 报告已保存: stock_screener_12factor_report.png")
    
    def export_results(self, filename='selected_stocks_12factor.csv'):
        """导出结果"""
        top50 = self.factor_data.head(50)[
            ['rank', 'code', 'roe', 'yoy_asset', 'momentum_12m', 'reversal_1m',
             'volatility', 'liquidity', 'debt_ratio', 'composite_score']
        ]
        top50.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ 结果已导出: {filename}")
        return top50
    
    def run(self, max_stocks=None, top_n=30):
        """运行完整流程"""
        try:
            self.login()
            self.get_all_stocks()
            self.batch_get_factors(max_stocks=max_stocks)
            self.calculate_12factor_score()
            
            # 回测（注意：这里用历史数据模拟，实际应该用未来数据）
            # 由于当前日期是2026-03-30，回测日期需要调整
            backtest_result = None
            # backtest_result = self.backtest_90days(top_n=top_n)
            
            self.generate_report(backtest_result)
            results = self.export_results()
            
            print("\n" + "="*60)
            print("🎉 12因子选股完成！")
            print("="*60)
            print(f"\n📈 有效样本: {len(self.factor_data)} 只股票")
            print(f"📊 得分范围: {self.factor_data['composite_score'].min():.1f} - {self.factor_data['composite_score'].max():.1f}")
            
            print(f"\n🏆 Top 10:")
            print("-"*60)
            for _, row in results.head(10).iterrows():
                print(f"  {row['rank']:2d}. {row['code']}  ROE:{row['roe']*100:5.1f}%  "
                      f"动量:{row['momentum_12m']*100:6.1f}%  得分:{row['composite_score']:5.1f}")
            print("-"*60)
            
            return results
            
        finally:
            self.logout()


def main():
    print("="*60)
    print("A股12因子选股系统")
    print("="*60)
    print("\n12因子：规模、价值、ROE、成长、动量、反转、")
    print("        波动率、流动性、杠杆、投资、净利率、周转率")
    print("="*60)
    
    screener = Factor12Screener()
    results = screener.run(max_stocks=500, top_n=30)
    
    return results


if __name__ == "__main__":
    main()
